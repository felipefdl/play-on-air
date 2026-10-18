//! AirPlay pause / resume / flush handling.
//!
//! A pause sends Cast PAUSE at once: with the Cast read-ahead buffered on the speaker, every
//! moment PAUSE waits is audible. A flush owes a re-LOAD to the next resume, when the sender's
//! new anchor has queued audio (Cast still buffers the pre-flush audio, so PLAY would replay
//! it). Cast PAUSE / PLAY and the post-PAUSE diagnostics probe run on the session's task set, so
//! replace and end teardown abort them with the generation.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio::time::sleep;

use crate::cast::{CastControl, MediaPlayback};

use super::Bridge;
use super::state::{CastRoute, DeviceState, PlayingSession};

/// After a long pause the Cast media session may be gone or holding a stale buffer tail.
/// Re-LOAD instead of PLAY when the pause exceeded this budget.
const LONG_PAUSE_RELOAD: Duration = Duration::from_secs(30);

/// After a Cast PAUSE is acknowledged, read the media status once more this much later, to see
/// whether the receiver really stopped (position frozen) or keeps playing buffered audio.
const PAUSE_PROBE_DELAY: Duration = Duration::from_secs(1);

/// Whether a detached PAUSE/PLAY from `expected_gen` may still touch Cast for `device_id`.
///
/// Requires both session liveness and a matching live generation so a command from a replaced
/// generation cannot land on the new session while the old `session_alive` is still true.
fn detached_cast_control_allowed(alive: bool, live_gen: Option<u64>, expected_gen: u64) -> bool {
  alive && live_gen == Some(expected_gen)
}

/// What a detached Cast PAUSE / PLAY needs from its session.
#[derive(Debug)]
struct TransportControl {
  pool: Arc<dyn CastControl>,
  route: Arc<CastRoute>,
  session_alive: Arc<AtomicBool>,
  playback_paused: Arc<AtomicBool>,
  transport_flight: Arc<tokio::sync::Mutex<()>>,
}

impl TransportControl {
  fn of(session: &PlayingSession) -> Self {
    Self {
      pool: Arc::clone(&session.pool),
      route: Arc::clone(&session.route),
      session_alive: Arc::clone(&session.session_alive),
      playback_paused: Arc::clone(&session.playback_paused),
      transport_flight: Arc::clone(&session.transport_flight),
    }
  }
}

/// Where a pause started, for the latency report.
#[derive(Debug, Clone, Copy)]
struct PauseOrigin {
  /// When the AirPlay receiver got the rate 0 (or flush).
  rate0_at: Instant,
  /// Media body bytes served at that moment.
  bytes_at_rate0: u64,
}

/// Cast follow-up chosen by [`Bridge::plan_resume`] (bookkeeping already applied).
#[derive(Debug)]
enum ResumeAction {
  /// Not paused and nothing owed; no Cast command.
  None,
  Play {
    generation: u64,
    control: TransportControl,
  },
  Reload {
    generation: u64,
    reason: &'static str,
    /// Owed flush request this re-LOAD satisfies (skipped if already covered).
    flush_request: Option<u64>,
  },
}

/// Pure decision for a `Resumed` event (unit-tested).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResumeDecision {
  /// Cast was paused; issue PLAY.
  Play,
  /// Cast was paused longer than [`LONG_PAUSE_RELOAD`]; re-LOAD.
  Reload,
  /// A flush is owed: Cast still buffers up to `CAST_READ_AHEAD` of audio from before it (the
  /// old track on a skip), so re-LOAD now that the new anchor's audio is queued.
  ReloadFlush,
  /// A stall re-LOAD was abandoned by a pause; Cast's media is still stalled, so re-LOAD.
  ReloadStall,
  /// Not paused and nothing owed.
  None,
}

/// A re-LOAD the next resume owes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwedReload {
  None,
  /// A flush (takes precedence: its LOAD also replaces stalled media).
  Flush,
  /// A stall re-LOAD a pause abandoned.
  Stall,
}

const fn resume_decision(paused: bool, long_pause: bool, owed: OwedReload) -> ResumeDecision {
  // An owed re-LOAD outranks PLAY. Flush: the ring was cleared, so only a fresh LOAD drops the
  // old track Cast still buffers (PLAY would resume that tail). Stall: Cast's media stopped
  // pulling, so PLAY would not bring the audio back.
  match owed {
    OwedReload::Flush => ResumeDecision::ReloadFlush,
    OwedReload::Stall => ResumeDecision::ReloadStall,
    OwedReload::None if paused && long_pause => ResumeDecision::Reload,
    OwedReload::None if paused => ResumeDecision::Play,
    OwedReload::None => ResumeDecision::None,
  }
}

/// Cast PAUSE for a rate 0, in order with PLAY, then the post-PAUSE probe.
///
/// Under [`TransportControl::transport_flight`] the command re-checks that the session still
/// wants to be paused, so a resume that overtook it leaves Cast playing.
async fn cast_pause_command(
  bridge: Arc<Bridge>,
  device_id: String,
  generation: u64,
  origin: PauseOrigin,
  control: TransportControl,
) {
  let transport = control.transport_flight.lock().await;
  let started = Instant::now();
  let check_bridge = Arc::clone(&bridge);
  let id = device_id.clone();
  let pool = Arc::clone(&control.pool);
  let route = Arc::clone(&control.route);
  let alive = Arc::clone(&control.session_alive);
  let paused = Arc::clone(&control.playback_paused);
  let pause_result = tokio::task::spawn_blocking(move || {
    let live_gen = check_bridge.session_generation(&id);
    if !detached_cast_control_allowed(alive.load(Ordering::Acquire), live_gen, generation)
      || !paused.load(Ordering::Acquire)
    {
      return Ok(None);
    }
    pool.pause(&route.media()).map(Some)
  })
  .await;
  drop(transport);
  let acked_at = Instant::now();
  let ack = match pause_result {
    Ok(Ok(Some(ack))) => ack,
    Ok(Ok(None)) => {
      tracing::debug!(%device_id, generation, "Cast PAUSE skipped; resumed or replaced first");
      return;
    },
    Ok(Err(err)) => {
      tracing::warn!(%device_id, generation, error = %err, "Cast PAUSE failed");
      return;
    },
    Err(err) => {
      tracing::warn!(%device_id, generation, error = %err, "Cast PAUSE task join failed");
      return;
    },
  };
  let bytes_at_ack = bridge.served_bytes(&device_id, generation);
  tracing::info!(
    %device_id,
    generation,
    cast_target = %control.route.media(),
    since_rate0_ms = acked_at.duration_since(origin.rate0_at).as_millis(),
    bridge_ms = started.duration_since(origin.rate0_at).as_millis(),
    command_ms = acked_at.duration_since(started).as_millis(),
    player_state = ?ack.player_state,
    current_time = ?ack.current_time,
    bytes_since_rate0 = ?bytes_at_ack.map(|bytes| bytes.saturating_sub(origin.bytes_at_rate0)),
    "Cast PAUSE acknowledged"
  );
  probe_after_pause(&bridge, &device_id, generation, &control, ack, bytes_at_ack).await;
}

/// Diagnostics: [`PAUSE_PROBE_DELAY`] after the PAUSE ack, report whether the receiver still
/// advances its position and whether it kept pulling the media body.
async fn probe_after_pause(
  bridge: &Arc<Bridge>,
  device_id: &str,
  generation: u64,
  control: &TransportControl,
  ack: MediaPlayback,
  bytes_at_ack: Option<u64>,
) {
  sleep(PAUSE_PROBE_DELAY).await;
  if !control.playback_paused.load(Ordering::Acquire) || bridge.session_generation(device_id) != Some(generation) {
    return;
  }
  let bytes_now = bridge.served_bytes(device_id, generation);
  let pool = Arc::clone(&control.pool);
  let key = control.route.media();
  let status = tokio::task::spawn_blocking(move || pool.media_playback(&key)).await;
  let probed = match status {
    Ok(Ok(probed)) => probed,
    Ok(Err(err)) => {
      tracing::info!(%device_id, generation, error = %err, "Cast media status after PAUSE unavailable");
      return;
    },
    Err(err) => {
      tracing::debug!(%device_id, generation, error = %err, "Cast media status task join failed");
      return;
    },
  };
  let advanced_s = probed.current_time.zip(ack.current_time).map(|(later, then)| later - then);
  let bytes_since_ack = bytes_now.zip(bytes_at_ack).map(|(later, then)| later.saturating_sub(then));
  tracing::info!(
    %device_id,
    generation,
    after_ms = PAUSE_PROBE_DELAY.as_millis(),
    player_state = ?probed.player_state,
    current_time = ?probed.current_time,
    current_time_at_ack = ?ack.current_time,
    advanced_s = ?advanced_s,
    bytes_since_ack = ?bytes_since_ack,
    "Cast media status after PAUSE"
  );
}

/// Cast PLAY on resume, in order with PAUSE; a failed PLAY falls back to a re-LOAD.
async fn cast_play_command(bridge: Arc<Bridge>, device_id: String, generation: u64, control: TransportControl) {
  let transport = control.transport_flight.lock().await;
  let check_bridge = Arc::clone(&bridge);
  let id = device_id.clone();
  let TransportControl {
    pool,
    route,
    session_alive,
    playback_paused,
    ..
  } = control;
  let play_result = tokio::task::spawn_blocking(move || {
    let live_gen = check_bridge.session_generation(&id);
    if !detached_cast_control_allowed(session_alive.load(Ordering::Acquire), live_gen, generation)
      || playback_paused.load(Ordering::Acquire)
    {
      return Ok(());
    }
    pool.play(&route.media())
  })
  .await;
  drop(transport);
  match &play_result {
    Ok(Ok(())) => return,
    Ok(Err(err)) => {
      tracing::debug!(%device_id, generation, error = %err, "Cast play failed; re-LOADing");
    },
    Err(err) => {
      tracing::debug!(%device_id, generation, error = %err, "Cast play task join failed; re-LOADing");
    },
  }
  // Separate session task so Paused/Ended can proceed while re-LOAD runs;
  // reload_playing_media re-checks generation + session_alive.
  bridge.spawn_reload_playing_media(&device_id, generation, "resume_play_failed", None);
}

impl Bridge {
  /// Pause Cast media (AirPlay rate 0, received at `rate0_at`). Keeps HTTP + session in
  /// `Playing { paused }`.
  ///
  /// Local paused bookkeeping is synchronous and Cast PAUSE goes out at once: an iOS track
  /// skip (flush, rate 0, then a new anchor) pauses too, and its resume re-LOADs. The PAUSE
  /// is a session task spawned under the slot lock, so teardown aborts it with the generation.
  pub(super) fn handle_pause(self: &Arc<Self>, device_id: &str, rate0_at: Instant) {
    let mut devices = self.devices.lock();
    let Some(DeviceState::Playing { generation: live_gen, session }) =
      devices.get_mut(device_id).map(|slot| &mut slot.state)
    else {
      drop(devices);
      tracing::debug!(%device_id, "dropping Paused; no Playing generation");
      return;
    };
    let generation = *live_gen;
    if session.paused {
      drop(devices);
      tracing::debug!(%device_id, generation, "AirPlay paused; already paused");
      return;
    }
    session.paused = true;
    session.playback_paused.store(true, Ordering::Release);
    session.paused_at = Some(Instant::now());
    let origin = PauseOrigin {
      rate0_at,
      bytes_at_rate0: session.media.bytes_served(),
    };
    let command = cast_pause_command(
      Arc::clone(self),
      device_id.to_owned(),
      generation,
      origin,
      TransportControl::of(session),
    );
    let _pause = session.tasks.spawn(device_id, command);
    drop(devices);
    tracing::info!(
      %device_id,
      generation,
      queued_ms = rate0_at.elapsed().as_millis(),
      "AirPlay paused; pausing Cast media"
    );
  }

  /// Resume Cast media after AirPlay playout restarts.
  ///
  /// Cast PLAY if paused; re-LOAD after a long pause or when a flush or a pause-abandoned stall
  /// re-LOAD is owed. PLAY runs after any PAUSE still in flight and re-checks the generation and
  /// `session_alive`, so a replaced generation cannot PLAY the new session.
  pub(super) fn handle_resume(self: &Arc<Self>, device_id: &str) {
    let Some(action) = self.plan_resume(device_id) else {
      return;
    };
    match action {
      ResumeAction::None => {},
      ResumeAction::Play { generation, control } => {
        tracing::info!(%device_id, generation, "AirPlay resumed; resuming Cast media");
        let command = cast_play_command(Arc::clone(self), device_id.to_owned(), generation, control);
        self.spawn_session_task(device_id, generation, command);
      },
      ResumeAction::Reload { generation, reason, flush_request } => {
        tracing::info!(%device_id, generation, reason, "AirPlay resumed; re-LOADing Cast media");
        // Detached: long Cast re-LOAD must not HOL-block the device worker FIFO.
        self.spawn_reload_playing_media(device_id, generation, reason, flush_request);
      },
    }
  }

  /// Apply `Resumed` bookkeeping under the slot lock and choose the Cast follow-up.
  ///
  /// Every `Resumed` clears local pause state (whatever the decision), so the stall watchdog
  /// and re-LOAD gates never see a stale `paused`.
  fn plan_resume(&self, device_id: &str) -> Option<ResumeAction> {
    let mut devices = self.devices.lock();
    let Some(DeviceState::Playing { generation: live_gen, session }) =
      devices.get_mut(device_id).map(|slot| &mut slot.state)
    else {
      drop(devices);
      tracing::debug!(%device_id, "dropping Resumed; no Playing generation");
      return None;
    };
    let generation = *live_gen;
    let long_pause = session.paused_at.is_some_and(|at| at.elapsed() >= LONG_PAUSE_RELOAD);
    let owed = if session.flush_reload_owed() {
      OwedReload::Flush
    } else if session.stall_reload_owed {
      OwedReload::Stall
    } else {
      OwedReload::None
    };
    let decision = resume_decision(session.paused, long_pause, owed);
    session.paused = false;
    session.playback_paused.store(false, Ordering::Release);
    session.paused_at = None;

    let action = match decision {
      ResumeDecision::Play => ResumeAction::Play {
        generation,
        control: TransportControl::of(session),
      },
      ResumeDecision::Reload => ResumeAction::Reload {
        generation,
        reason: "long_pause_resume",
        flush_request: None,
      },
      ResumeDecision::ReloadFlush => ResumeAction::Reload {
        generation,
        reason: "flush_resume",
        flush_request: Some(session.flush_reload_requested),
      },
      ResumeDecision::ReloadStall => ResumeAction::Reload {
        generation,
        reason: "stall_owed_resume",
        flush_request: None,
      },
      ResumeDecision::None => ResumeAction::None,
    };
    drop(devices);
    Some(action)
  }

  /// Flush-all: ring already cleared by ingest; the next resume owes a re-LOAD.
  ///
  /// Every flush comes with a pause (Cast PAUSE already stopped the pre-flush audio) and is
  /// followed by a resume once the sender's new anchor has audio queued, so the re-LOAD waits
  /// for that resume instead of a timer. A re-LOAD in flight that started before this flush
  /// does not cover it.
  pub(super) fn handle_flush(&self, device_id: &str) {
    let mut devices = self.devices.lock();
    let Some(DeviceState::Playing { generation: live_gen, session }) =
      devices.get_mut(device_id).map(|slot| &mut slot.state)
    else {
      drop(devices);
      tracing::debug!(%device_id, "dropping Flushed; no Playing generation");
      return;
    };
    let generation = *live_gen;
    session.flush_reload_requested = session.flush_reload_requested.saturating_add(1);
    let request = session.flush_reload_requested;
    drop(devices);
    tracing::info!(%device_id, generation, request, "AirPlay flushed; re-LOAD owed to the next resume");
  }

  /// Media body bytes served for `device_id`'s live `generation` (pause diagnostics).
  fn served_bytes(&self, device_id: &str, generation: u64) -> Option<u64> {
    match self.devices.lock().get(device_id).map(|slot| &slot.state) {
      Some(DeviceState::Playing { generation: live_gen, session }) if *live_gen == generation => {
        Some(session.media.bytes_served())
      },
      Some(
        DeviceState::Idle | DeviceState::Starting { .. } | DeviceState::Playing { .. } | DeviceState::Following { .. },
      )
      | None => None,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::cast::CastControl;
  use crate::cast::CastPool;

  use crate::audio::PcmRing;
  use crate::bridge::load::ReloadOutcome;
  use crate::bridge::teardown::{TeardownCastPolicy, teardown_playing};
  use crate::bridge::test_support::{CastCall, FakeCast, insert_playing, test_playing_session};
  use crate::media::MediaServer;
  use crate::registry::DeviceRegistry;

  /// Bridge on a recording [`FakeCast`] with one Playing session for `device_id`.
  async fn fake_bridge(device_id: &str) -> (Arc<Bridge>, Arc<FakeCast>) {
    let fake = Arc::new(FakeCast::default());
    let pool: Arc<dyn CastControl> = Arc::<FakeCast>::clone(&fake);
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    insert_playing(
      &bridge,
      device_id,
      1,
      test_playing_session(media, device_id, pool, Arc::new(PcmRing::new(2, 64))),
    );
    (bridge, fake)
  }

  /// Cast transport commands (PAUSE / PLAY / LOAD) in the order the fake received them.
  fn transport(fake: &FakeCast) -> Vec<CastCall> {
    fake
      .calls()
      .into_iter()
      .filter(|call| matches!(call, CastCall::Pause(_) | CastCall::Play(_) | CastCall::Load(_)))
      .collect()
  }

  async fn wait_until(what: &str, fake: &FakeCast, check: impl Fn(&[CastCall]) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !check(&fake.calls()) {
      assert!(Instant::now() < deadline, "timed out waiting for {what}: {:?}", fake.calls());
      sleep(Duration::from_millis(5)).await;
    }
  }

  fn with_session<T>(bridge: &Bridge, device_id: &str, read: impl FnOnce(&PlayingSession) -> T) -> T {
    let devices = bridge.devices.lock();
    let Some(DeviceState::Playing { session, .. }) = devices.get(device_id).map(|slot| &slot.state) else {
      panic!("expected Playing");
    };
    let value = read(session);
    drop(devices);
    value
  }

  /// A rate 0 reaches Cast as PAUSE right away (no debounce): the speaker's read-ahead keeps
  /// playing for as long as PAUSE waits.
  #[tokio::test]
  async fn pause_sends_cast_pause_immediately() {
    let (bridge, fake) = fake_bridge("dev-pause").await;

    let rate0_at = Instant::now();
    bridge.handle_pause("dev-pause", rate0_at);
    assert!(with_session(&bridge, "dev-pause", |session| session.paused && session.paused_at.is_some()));
    wait_until("Cast PAUSE", &fake, |calls| {
      calls.contains(&CastCall::Pause("dev-pause".to_owned()))
    })
    .await;
    assert!(
      rate0_at.elapsed() < Duration::from_millis(300),
      "Cast PAUSE took {:?}",
      rate0_at.elapsed()
    );
    assert!(bridge.has_session("dev-pause"), "pause keeps the session");

    // A duplicate rate 0 sends nothing more.
    bridge.handle_pause("dev-pause", Instant::now());
    sleep(Duration::from_millis(50)).await;
    assert_eq!(transport(&fake), vec![CastCall::Pause("dev-pause".to_owned())]);
    bridge.handle_session_end("dev-pause").await;
  }

  /// Resume after a pause sends PLAY, after the PAUSE.
  #[tokio::test]
  async fn resume_after_pause_plays() {
    let (bridge, fake) = fake_bridge("dev-resume").await;

    bridge.handle_pause("dev-resume", Instant::now());
    wait_until("Cast PAUSE", &fake, |calls| {
      calls.contains(&CastCall::Pause("dev-resume".to_owned()))
    })
    .await;
    bridge.handle_resume("dev-resume");
    wait_until("Cast PLAY", &fake, |calls| {
      calls.contains(&CastCall::Play("dev-resume".to_owned()))
    })
    .await;
    assert_eq!(
      transport(&fake),
      vec![
        CastCall::Pause("dev-resume".to_owned()),
        CastCall::Play("dev-resume".to_owned())
      ]
    );
    assert!(!with_session(&bridge, "dev-resume", |session| session.paused));
    bridge.handle_session_end("dev-resume").await;
  }

  /// Pause and resume back to back (an iOS re-anchor): Cast never ends paused, whichever order
  /// the detached commands reach the transport lock in.
  #[tokio::test]
  async fn quick_pause_resume_never_ends_paused() {
    let (bridge, fake) = fake_bridge("dev-reanchor").await;

    for _ in 0..20 {
      bridge.handle_pause("dev-reanchor", Instant::now());
      bridge.handle_resume("dev-reanchor");
    }
    sleep(Duration::from_millis(200)).await;
    let calls = transport(&fake);
    assert!(
      !matches!(calls.last(), Some(CastCall::Pause(_))),
      "Cast must not end paused after a resume: {calls:?}"
    );
    assert!(!with_session(&bridge, "dev-reanchor", |session| session.paused));
    bridge.handle_session_end("dev-reanchor").await;
  }

  /// iOS skip: flush + rate 0, then the new anchor's rate 1. Cast pauses at once (the old track
  /// stops) and the resume re-LOADs exactly once; nothing is left owed or paused.
  #[tokio::test]
  async fn skip_pauses_then_reloads_once_at_resume() {
    let (bridge, fake) = fake_bridge("dev-skip").await;

    bridge.handle_flush("dev-skip");
    bridge.handle_pause("dev-skip", Instant::now());
    wait_until("Cast PAUSE", &fake, |calls| {
      calls.contains(&CastCall::Pause("dev-skip".to_owned()))
    })
    .await;
    sleep(Duration::from_millis(50)).await;
    bridge.handle_resume("dev-skip");
    wait_until("re-LOAD", &fake, |calls| calls.contains(&CastCall::Load("dev-skip".to_owned()))).await;
    sleep(Duration::from_millis(200)).await;

    assert_eq!(
      transport(&fake),
      vec![CastCall::Pause("dev-skip".to_owned()), CastCall::Load("dev-skip".to_owned())],
      "one PAUSE, then one re-LOAD instead of PLAY"
    );
    with_session(&bridge, "dev-skip", |session| {
      assert!(!session.paused, "resume cleared local pause");
      assert_eq!(session.flush_reload_requested, 1);
      assert!(!session.flush_reload_owed(), "the re-LOAD covered the flush");
    });
    bridge.handle_session_end("dev-skip").await;
  }

  /// Skip while paused (and resumed much later): the flush stays owed, so the resume re-LOADs
  /// instead of resuming the pre-flush buffer tail with Cast PLAY.
  #[tokio::test]
  async fn flush_while_paused_reloads_on_resume() {
    let (bridge, _fake) = fake_bridge("dev-flush-paused").await;

    bridge.handle_pause("dev-flush-paused", Instant::now());
    bridge.handle_flush("dev-flush-paused");
    assert!(with_session(&bridge, "dev-flush-paused", PlayingSession::flush_reload_owed));
    let action = bridge.plan_resume("dev-flush-paused").expect("Playing");
    assert!(
      matches!(
        action,
        ResumeAction::Reload {
          generation: 1,
          reason: "flush_resume",
          flush_request: Some(1),
        }
      ),
      "resume after a paused flush must re-LOAD, got {action:?}"
    );
    bridge.handle_session_end("dev-flush-paused").await;
  }

  /// One probe after the PAUSE ack reads the media status (pause latency diagnostics).
  #[tokio::test]
  async fn pause_ack_is_followed_by_one_media_status_probe() {
    let probes = |fake: &FakeCast| {
      fake
        .calls()
        .iter()
        .filter(|call| matches!(call, CastCall::MediaStatus(_)))
        .count()
    };
    let (bridge, fake) = fake_bridge("dev-probe").await;

    bridge.handle_pause("dev-probe", Instant::now());
    sleep(PAUSE_PROBE_DELAY + Duration::from_millis(300)).await;
    assert_eq!(probes(&fake), 1, "{:?}", fake.calls());

    // A resume before the probe cancels it.
    bridge.handle_resume("dev-probe");
    bridge.handle_pause("dev-probe", Instant::now());
    sleep(Duration::from_millis(50)).await;
    bridge.handle_resume("dev-probe");
    sleep(PAUSE_PROBE_DELAY + Duration::from_millis(300)).await;
    assert_eq!(probes(&fake), 1, "no probe once resumed: {:?}", fake.calls());
    bridge.handle_session_end("dev-probe").await;
  }

  #[test]
  fn resume_decision_helper() {
    assert_eq!(resume_decision(true, false, OwedReload::None), ResumeDecision::Play);
    assert_eq!(resume_decision(true, true, OwedReload::None), ResumeDecision::Reload);
    assert_eq!(resume_decision(false, false, OwedReload::None), ResumeDecision::None);
    // An owed flush re-LOAD wins over PLAY, paused or not.
    assert_eq!(resume_decision(true, false, OwedReload::Flush), ResumeDecision::ReloadFlush);
    assert_eq!(resume_decision(false, false, OwedReload::Flush), ResumeDecision::ReloadFlush);
    assert_eq!(resume_decision(true, true, OwedReload::Flush), ResumeDecision::ReloadFlush);
    // An owed stall re-LOAD outranks PLAY too.
    assert_eq!(resume_decision(true, false, OwedReload::Stall), ResumeDecision::ReloadStall);
    assert_eq!(resume_decision(false, false, OwedReload::Stall), ResumeDecision::ReloadStall);
  }

  /// A resume clears every piece of local pause state, so the stall watchdog and re-LOAD gates
  /// are not left muted by a stale `paused`.
  #[tokio::test]
  async fn resume_clears_pause_state() {
    let (bridge, _fake) = fake_bridge("dev-resume-state").await;

    bridge.handle_pause("dev-resume-state", Instant::now());
    let action = bridge.plan_resume("dev-resume-state").expect("Playing");
    assert!(matches!(action, ResumeAction::Play { generation: 1, .. }), "{action:?}");
    with_session(&bridge, "dev-resume-state", |session| {
      assert!(!session.paused);
      assert!(!session.playback_paused.load(Ordering::Acquire));
      assert!(session.paused_at.is_none());
    });
    bridge.handle_session_end("dev-resume-state").await;
  }

  /// A flush re-LOAD that queued behind an in-flight re-LOAD and was then abandoned by a pause
  /// keeps its intent: the next resume re-LOADs instead of sending PLAY.
  #[tokio::test]
  async fn flush_reload_abandoned_by_pause_is_owed_to_resume() {
    let pool: Arc<dyn CastControl> = Arc::new(CastPool::new(None));
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let mut playing = test_playing_session(media, "dev-flush-race", pool, Arc::new(PcmRing::new(2, 64)));
    playing.flush_reload_requested = 1;
    let reload_flight = Arc::clone(&playing.reload_flight);
    insert_playing(&bridge, "dev-flush-race", 1, playing);

    let in_flight = reload_flight.lock().await;
    let queued = tokio::spawn({
      let bridge_for_task = Arc::clone(&bridge);
      async move {
        bridge_for_task
          .reload_playing_media("dev-flush-race", 1, "flush", Some(1))
          .await
      }
    });
    sleep(Duration::from_millis(20)).await;
    bridge.handle_pause("dev-flush-race", Instant::now());
    drop(in_flight);
    let reloaded = tokio::time::timeout(Duration::from_secs(2), queued)
      .await
      .expect("queued re-LOAD finishes")
      .expect("join");
    assert_eq!(reloaded, ReloadOutcome::Paused, "pause must abort the queued flush re-LOAD");

    let action = bridge.plan_resume("dev-flush-race").expect("Playing");
    assert!(
      matches!(action, ResumeAction::Reload { flush_request: Some(1), .. }),
      "abandoned flush must be re-LOADed on resume, got {action:?}"
    );
    bridge.handle_session_end("dev-flush-race").await;
  }

  #[test]
  fn detached_cast_control_requires_alive_and_matching_generation() {
    assert!(detached_cast_control_allowed(true, Some(3), 3));
    assert!(!detached_cast_control_allowed(false, Some(3), 3));
    assert!(
      !detached_cast_control_allowed(true, Some(4), 3),
      "replaced gen must not control Cast"
    );
    assert!(!detached_cast_control_allowed(true, None, 3));
    assert!(!detached_cast_control_allowed(true, Some(2), 3));
  }

  #[tokio::test]
  async fn replaced_generation_loses_detached_cast_control() {
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None))));
    let pool = Arc::clone(&bridge.cast_pool);
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let session = test_playing_session(media, "dev-gen-ctl", pool, Arc::new(PcmRing::new(2, 64)));
    let old_alive = Arc::clone(&session.session_alive);
    insert_playing(&bridge, "dev-gen-ctl", 1, session);

    // Accept a replacement start: live gen becomes 2 (Starting) while old session_alive
    // is still true until detached teardown runs — the residual B2 race.
    let (new_gen, prior) = bridge.accept_start("dev-gen-ctl");
    assert_eq!(new_gen, 2);
    assert!(prior.is_some());
    assert!(
      old_alive.load(Ordering::Acquire),
      "old session_alive still true before teardown"
    );
    assert_eq!(bridge.session_generation("dev-gen-ctl"), Some(2));
    assert!(
      !detached_cast_control_allowed(old_alive.load(Ordering::Acquire), bridge.session_generation("dev-gen-ctl"), 1),
      "old PAUSE/PLAY must not run against the new generation"
    );

    if let Some(old) = prior {
      teardown_playing(*old, TeardownCastPolicy::SkipStopForReplace).await;
    }
  }
}
