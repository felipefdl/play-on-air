//! Timed Cast PLAY and the once-per-second drift loop for a calibrated speaker.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rust_cast::channels::media::PlayerState;

use crate::audio::sync::{
  DriftAction, ResolvedSync, SyncTimeline, TickLog, drift_action, instant_to_unix_ns, predicted_error, signed_secs,
  smooth_ema,
};
use crate::cast::{CastControl, MediaPlayback};

use super::Bridge;
use super::state::CastRoute;

const SYNC_POLL: Duration = Duration::from_millis(50);
const DRIFT_TICK: Duration = Duration::from_secs(1);
const SPIN_TAIL: Duration = Duration::from_millis(1);
/// When stream frame 0 is already due, how long to wait for PAUSED before sending PLAY.
const PAUSED_GRACE: Duration = Duration::from_millis(500);

/// Timing constants and timeline for one synced Cast session.
pub(super) struct SyncAttach {
  pub(super) timeline: Arc<SyncTimeline>,
  pub(super) params: ResolvedSync,
}

/// What the drift task needs after the session is installed.
pub(super) struct SyncSession {
  pub(super) device_id: String,
  pub(super) timeline: Arc<SyncTimeline>,
  pub(super) play_latency: Duration,
  pub(super) bias_ns: i64,
  pub(super) sample_rate: u32,
  /// Bumped after each accepted re-LOAD so the timed start runs again.
  pub(super) load_epoch: AtomicU64,
  /// Set once the timed PLAY for the current load has been acknowledged.
  ///
  /// Until then a resume must not send its own PLAY: the timed start still owns it.
  play_sent: AtomicBool,
  pool: Arc<dyn CastControl>,
  route: Arc<CastRoute>,
  /// Media key of the calibrated speaker itself. When multizone moves the session to a Cast
  /// group host, `currentTime` comes from that host, whose offset is not calibrated, so drift
  /// correction pauses until the session is back on this key.
  home_media: String,
  session_alive: Arc<AtomicBool>,
  playback_paused: Arc<AtomicBool>,
  transport_flight: Arc<tokio::sync::Mutex<()>>,
}

impl std::fmt::Debug for SyncSession {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("SyncSession")
      .field("device_id", &self.device_id)
      .field("play_latency", &self.play_latency)
      .field("bias_ns", &self.bias_ns)
      .field("sample_rate", &self.sample_rate)
      .finish_non_exhaustive()
  }
}

impl SyncSession {
  #[expect(
    clippy::too_many_arguments,
    reason = "sync task owns the session handles it has to outlive"
  )]
  pub(super) fn from_attach(
    attach: SyncAttach,
    device_id: &str,
    sample_rate: u32,
    pool: Arc<dyn CastControl>,
    route: Arc<CastRoute>,
    session_alive: Arc<AtomicBool>,
    playback_paused: Arc<AtomicBool>,
    transport_flight: Arc<tokio::sync::Mutex<()>>,
  ) -> Self {
    let home_media = route.media();
    Self {
      device_id: device_id.to_owned(),
      timeline: attach.timeline,
      play_latency: attach.params.play_latency,
      bias_ns: attach.params.bias_ns,
      sample_rate: sample_rate.max(1),
      load_epoch: AtomicU64::new(1),
      play_sent: AtomicBool::new(false),
      pool,
      route,
      home_media,
      session_alive,
      playback_paused,
      transport_flight,
    }
  }

  fn alive(&self) -> bool {
    self.session_alive.load(Ordering::Acquire)
  }

  fn epoch_is(&self, epoch: u64) -> bool {
    self.load_epoch.load(Ordering::Acquire) == epoch
  }

  /// Whether this load's timed PLAY has been acknowledged.
  pub(super) fn play_sent(&self) -> bool {
    self.play_sent.load(Ordering::Acquire)
  }
}

impl Bridge {
  pub(super) fn note_sync_reload(&self, device_id: &str) {
    let devices = self.devices.lock();
    if let Some(super::state::DeviceState::Playing { session, .. }) = devices.get(device_id).map(|slot| &slot.state)
      && let Some(sync) = &session.sync
    {
      let _prev = sync.load_epoch.fetch_add(1, Ordering::AcqRel);
    }
    drop(devices);
  }
}

/// Wait for PAUSED + stream-zero due time, PLAY at `D0 - L`, then correct drift until reload.
pub(super) async fn run_sync(sync: Arc<SyncSession>) {
  loop {
    if !sync.alive() {
      return;
    }
    let epoch = sync.load_epoch.load(Ordering::Acquire);
    if !timed_start(&sync, epoch).await {
      return;
    }
    if !drift_until_reload(&sync, epoch).await {
      return;
    }
  }
}

/// `false` when the session has ended. `true` when a newer LOAD should start over, or PLAY was sent.
async fn timed_start(sync: &SyncSession, epoch: u64) -> bool {
  sync.play_sent.store(false, Ordering::Release);
  loop {
    if !sync.alive() {
      return false;
    }
    if !sync.epoch_is(epoch) {
      return true;
    }
    let Some(d0) = wait_for_arm(sync, epoch).await else {
      return sync.alive() && !sync.epoch_is(epoch);
    };
    let play_at = d0.checked_sub(sync.play_latency).unwrap_or(d0);
    if !sleep_until_play(sync, epoch, play_at).await {
      return sync.alive() && !sync.epoch_is(epoch);
    }
    if sync.playback_paused.load(Ordering::Acquire) {
      continue;
    }
    match send_timed_play(sync, epoch, d0, play_at).await {
      PlaySend::Sent => {
        sync.play_sent.store(true, Ordering::Release);
        return sync.alive();
      },
      PlaySend::Stop => return false,
      PlaySend::Retry => {
        if sync.playback_paused.load(Ordering::Acquire) || !sync.epoch_is(epoch) {
          continue;
        }
        tokio::time::sleep(PAUSED_GRACE).await;
      },
    }
  }
}

/// Receiver reported PAUSED, or the wait for that report has run out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArmWait {
  /// PAUSED is in. Sleep until the play instant, then PLAY.
  Ready,
  /// PAUSED did not arrive before the deadline. PLAY anyway.
  Fallback,
  Wait,
}

/// `play_at` when frame 0 is still ahead. Otherwise a short grace after `seen_at`, so a late
/// timeline does not wait forever for a status that never comes.
fn arm_deadline(play_at: Instant, seen_at: Instant) -> Instant {
  if play_at > seen_at {
    play_at
  } else {
    seen_at.checked_add(PAUSED_GRACE).unwrap_or(seen_at)
  }
}

fn arm_wait(receiver_paused: bool, now: Instant, play_at: Instant, seen_at: Instant) -> ArmWait {
  if receiver_paused {
    ArmWait::Ready
  } else if now >= arm_deadline(play_at, seen_at) {
    ArmWait::Fallback
  } else {
    ArmWait::Wait
  }
}

async fn wait_for_arm(sync: &SyncSession, epoch: u64) -> Option<Instant> {
  let mut seen: Option<(Instant, Instant)> = None;
  loop {
    if !sync.alive() || !sync.epoch_is(epoch) {
      return None;
    }
    if sync.playback_paused.load(Ordering::Acquire) {
      seen = None;
      tokio::time::sleep(SYNC_POLL).await;
      continue;
    }
    let Some(d0) = sync.timeline.d0_ready() else {
      seen = None;
      tokio::time::sleep(SYNC_POLL).await;
      continue;
    };
    let seen_at = match seen {
      Some((marked, at)) if marked == d0 => at,
      _ => {
        let at = Instant::now();
        seen = Some((d0, at));
        at
      },
    };
    let play_at = d0.checked_sub(sync.play_latency).unwrap_or(d0);
    let status = playback(sync).await;
    let receiver_paused = matches!(status.as_ref(), Some(media) if media.player_state == PlayerState::Paused);
    match arm_wait(receiver_paused, Instant::now(), play_at, seen_at) {
      ArmWait::Wait => tokio::time::sleep(SYNC_POLL).await,
      ArmWait::Ready => return Some(d0),
      ArmWait::Fallback => {
        tracing::info!(device = %sync.device_id, "sync PAUSED did not arrive; sending PLAY");
        return Some(d0);
      },
    }
  }
}

async fn sleep_until_play(sync: &SyncSession, epoch: u64, play_at: Instant) -> bool {
  let wake = play_at.checked_sub(SPIN_TAIL).unwrap_or(play_at);
  loop {
    if !sync.alive() || !sync.epoch_is(epoch) {
      return false;
    }
    let now = Instant::now();
    if now >= wake {
      break;
    }
    let slice = Duration::from_millis(200).min(wake.saturating_duration_since(now));
    tokio::time::sleep(slice).await;
  }
  while Instant::now() < play_at {
    if !sync.alive() || !sync.epoch_is(epoch) {
      return false;
    }
    std::hint::spin_loop();
  }
  true
}

enum PlaySend {
  Sent,
  /// User paused, the load epoch moved, or PLAY failed. The caller decides whether to retry.
  Retry,
  Stop,
}

async fn send_timed_play(sync: &SyncSession, epoch: u64, d0: Instant, play_at: Instant) -> PlaySend {
  let _flight = sync.transport_flight.lock().await;
  if !sync.alive() {
    return PlaySend::Stop;
  }
  if !sync.epoch_is(epoch) || sync.playback_paused.load(Ordering::Acquire) {
    return PlaySend::Retry;
  }
  let late = Instant::now().saturating_duration_since(play_at);
  let late_ms = late.as_secs_f64() * 1_000.0;
  if late_ms > 0.5 {
    tracing::info!(device = %sync.device_id, late_ms, "sync PLAY late; drift loop will catch up");
  }
  let sent = Instant::now();
  let pool = Arc::clone(&sync.pool);
  let key = sync.route.media();
  let played = tokio::task::spawn_blocking(move || pool.play(&key)).await;
  match played {
    Ok(Ok(())) => {
      let ack_ms = sent.elapsed().as_secs_f64() * 1_000.0;
      sync.timeline.log_start(d0, sent, late_ms, ack_ms);
      PlaySend::Sent
    },
    Ok(Err(err)) => {
      tracing::warn!(device = %sync.device_id, error = %err, "sync PLAY failed");
      PlaySend::Retry
    },
    Err(err) => {
      tracing::warn!(device = %sync.device_id, error = %err, "sync PLAY task failed");
      PlaySend::Retry
    },
  }
}

/// `false` when the session ended. `true` when `load_epoch` moved on.
async fn drift_until_reload(sync: &SyncSession, epoch: u64) -> bool {
  if !sync.alive() {
    return false;
  }
  if !sync.epoch_is(epoch) {
    return true;
  }
  let mut tick = tokio::time::interval(DRIFT_TICK);
  tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
  let _first = tick.tick().await;
  let mut ema = None;
  let mut hosted = false;
  loop {
    let _tick = tick.tick().await;
    if !sync.alive() {
      return false;
    }
    if !sync.epoch_is(epoch) {
      return true;
    }
    if sync.route.media() != sync.home_media {
      if !hosted {
        tracing::info!(
          device = %sync.device_id,
          host = %sync.route.media(),
          "sync drift correction paused: a Cast group hosts the session and its timing is not calibrated"
        );
        hosted = true;
        ema = None;
      }
      continue;
    }
    hosted = false;
    if let Some(next) = drift_tick(sync, ema).await {
      ema = Some(next);
    }
  }
}

async fn drift_tick(sync: &SyncSession, ema: Option<f64>) -> Option<f64> {
  let status = playback(sync).await?;
  let t = Instant::now();
  if status.player_state != PlayerState::Playing {
    return ema;
  }
  let current = status.current_time?;
  let sample = sample_error(sync, t, f64::from(current))?;
  let smoothed = smooth_ema(ema, sample.predicted);
  apply_drift(sync, smoothed);
  let request = match drift_action(smoothed, sync.sample_rate) {
    DriftAction::Adjust(samples) => samples,
    DriftAction::None => 0,
  };
  sync.timeline.log_tick(&TickLog {
    t_unix_ns: instant_to_unix_ns(t),
    current_time: f64::from(current),
    s_aud: sample.s_aud,
    due_unix_ns: instant_to_unix_ns(sample.due),
    e_ms: sample.error * 1_000.0,
    e_pred_ms: sample.predicted * 1_000.0,
    ema_ms: smoothed * 1_000.0,
    request,
    applied_total: sync.timeline.applied_net(),
  });
  Some(smoothed)
}

struct SampleError {
  s_aud: f64,
  due: Instant,
  error: f64,
  predicted: f64,
}

fn sample_error(sync: &SyncSession, t: Instant, current_time: f64) -> Option<SampleError> {
  let bias_secs = (sync.bias_ns as f64) / 1_000_000_000.0;
  let s_aud = (current_time - bias_secs) * f64::from(sync.sample_rate);
  let due = sync.timeline.due_at(s_aud)?;
  let error = signed_secs(t, due);
  let pending = sync.timeline.pending_net(s_aud);
  let predicted = predicted_error(error, pending, sync.sample_rate);
  Some(SampleError { s_aud, due, error, predicted })
}

fn apply_drift(sync: &SyncSession, ema: f64) {
  if ema.abs() > crate::audio::sync::DRIFT_STEP_SECS {
    sync.timeline.log_step(ema * 1_000.0);
  }
  match drift_action(ema, sync.sample_rate) {
    DriftAction::Adjust(samples) => sync.timeline.add_request(samples),
    DriftAction::None => {},
  }
}

async fn playback(sync: &SyncSession) -> Option<MediaPlayback> {
  let pool = Arc::clone(&sync.pool);
  let key = sync.route.media();
  match tokio::task::spawn_blocking(move || pool.media_playback(&key)).await {
    Ok(Ok(status)) => Some(status),
    Ok(Err(err)) => {
      tracing::debug!(device = %sync.device_id, error = %err, "sync media status failed");
      None
    },
    Err(err) => {
      tracing::debug!(device = %sync.device_id, error = %err, "sync media status task failed");
      None
    },
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn arm_wait_plays_at_the_mark_or_after_a_bounded_grace() {
    let seen = Instant::now();
    let play_at = seen + Duration::from_millis(700);
    assert_eq!(arm_wait(true, seen, play_at, seen), ArmWait::Ready);
    assert_eq!(arm_wait(false, seen + Duration::from_millis(100), play_at, seen), ArmWait::Wait);
    assert_eq!(arm_wait(false, play_at, play_at, seen), ArmWait::Fallback);

    let play_at_past = seen.checked_sub(Duration::from_secs(2)).expect("past");
    assert_eq!(
      arm_wait(false, seen, play_at_past, seen),
      ArmWait::Wait,
      "grace has not elapsed"
    );
    assert_eq!(
      arm_wait(false, seen + PAUSED_GRACE, play_at_past, seen),
      ArmWait::Fallback,
      "a late timeline must not wait forever for PAUSED"
    );
  }
}
