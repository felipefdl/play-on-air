//! Stall watchdog: detect Cast no longer pulling the HTTP body and recover or end the session.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use tokio::time::sleep;

use super::Bridge;
use super::fallback::{FLAC_FALLBACK_WINDOW, FlacStrikeDecision, FlacStrikeReason, apply_flac_strike};
use super::state::{DeviceState, EgressKind};
use super::teardown::{TeardownCastPolicy, teardown_playing};

/// Stall watchdog poll interval (cheap `MediaServer::progress` read).
const STALL_CHECK_INTERVAL: Duration = Duration::from_secs(2);

/// `last_body_write` older than this while PCM is available ⇒ Cast stopped pulling.
const STALL_BODY_STALE: Duration = Duration::from_secs(5);

/// Second stall within this window after a stall re-LOAD kicks the session.
const STALL_REPEAT_WINDOW: Duration = Duration::from_secs(60);

/// Body HTTP write is stale when last write is older than [`STALL_BODY_STALE`], or when Cast
/// never pulled and the session has been running at least that long.
fn body_is_stale(last_write_age: Option<Duration>, started_age: Duration) -> bool {
  last_write_age.map_or(started_age >= STALL_BODY_STALE, |age| age >= STALL_BODY_STALE)
}

/// Pure stall-watchdog decision (unit-tested; [`Bridge::stall_watchdog_tick`] applies it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StallAction {
  /// No stall action (paused, body fresh, or empty ring / sender silence).
  None,
  /// Early FLAC session stall → remember WAV and switch egress for this generation.
  EarlyFlacFallback,
  /// Second stall within [`STALL_REPEAT_WINDOW`] → terminal end (kicks AirPlay).
  TerminalEnd,
  /// First stall (or after failed early FLAC switch) → generation-scoped re-LOAD.
  Reload,
}

/// Decide stall response from a snapshot of session fields (no locks / I/O).
#[expect(
  clippy::fn_params_excessive_bools,
  reason = "pure decision helper mirrors stall_watchdog_tick field snapshot for unit tests"
)]
const fn stall_action(
  paused: bool,
  body_stale: bool,
  ring_frames: usize,
  egress: EgressKind,
  early_flac_window: bool,
  repeat_within_window: bool,
) -> StallAction {
  if paused || !body_stale || ring_frames == 0 {
    return StallAction::None;
  }
  if matches!(egress, EgressKind::FlacLive) && early_flac_window {
    return StallAction::EarlyFlacFallback;
  }
  if repeat_within_window {
    return StallAction::TerminalEnd;
  }
  StallAction::Reload
}

struct StallSnapshot {
  paused: bool,
  last_write: Option<Instant>,
  ring_frames: usize,
  started_at: Instant,
  egress: EgressKind,
  last_stall: Option<Instant>,
}

pub(super) fn spawn_stall_watchdog(
  bridge: Arc<Bridge>,
  device_id: String,
  generation: u64,
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
  let (cancel_tx, mut cancel_rx) = oneshot::channel::<()>();
  let task = tokio::spawn(async move {
    loop {
      tokio::select! {
        _ = &mut cancel_rx => break,
        () = sleep(STALL_CHECK_INTERVAL) => {
          bridge.stall_watchdog_tick(&device_id, generation).await;
        }
      }
    }
  });
  (cancel_tx, task)
}

impl Bridge {
  /// Stall watchdog tick for a Playing generation (called from a detached task).
  async fn stall_watchdog_tick(self: &Arc<Self>, device_id: &str, generation: u64) {
    let Some(snapshot) = self.stall_snapshot(device_id, generation) else {
      return;
    };

    // Body age: last HTTP write, or time since session start if Cast never pulled.
    // Never-pulled while the ring has data after STALL_BODY_STALE is a stall.
    let last_write_age = snapshot.last_write.map(|at| at.elapsed());
    let started_age = snapshot.started_at.elapsed();
    let body_stale = body_is_stale(last_write_age, started_age);
    let early_flac_window = started_age < FLAC_FALLBACK_WINDOW;
    let repeat_within_window = snapshot.last_stall.is_some_and(|at| at.elapsed() < STALL_REPEAT_WINDOW);

    let mut action = stall_action(
      snapshot.paused,
      body_stale,
      snapshot.ring_frames,
      snapshot.egress,
      early_flac_window,
      repeat_within_window,
    );

    // Early FLAC stall → remember WAV and switch egress; on switch failure fall through
    // to terminal/reload as before (re-evaluate without the early-window branch).
    if action == StallAction::EarlyFlacFallback {
      tracing::warn!(
        %device_id,
        generation,
        "early FLAC session stall; falling back to WAV BUFFERED"
      );
      self.remember_flac_fallback(device_id);
      if self.switch_playing_to_wav(device_id, generation).await {
        return;
      }
      action = stall_action(
        snapshot.paused,
        body_stale,
        snapshot.ring_frames,
        snapshot.egress,
        false,
        repeat_within_window,
      );
    }

    match action {
      StallAction::None | StallAction::EarlyFlacFallback => {},
      StallAction::TerminalEnd => {
        self.terminal_stall_end(device_id, generation, "media stall repeated within window");
      },
      StallAction::Reload => {
        self.handle_stall_reload(device_id, generation).await;
      },
    }
  }

  fn stall_snapshot(&self, device_id: &str, generation: u64) -> Option<StallSnapshot> {
    let devices = self.devices.lock();
    let slot = devices.get(device_id)?;
    let DeviceState::Playing { generation: live_gen, session } = &slot.state else {
      return None;
    };
    if *live_gen != generation {
      return None;
    }
    let (_bytes, last_write) = session.media.progress();
    let snap = StallSnapshot {
      paused: session.paused,
      last_write,
      ring_frames: session.ring.available_frames(),
      started_at: session.started_at,
      egress: session.egress,
      last_stall: session.last_stall_reload_at,
    };
    drop(devices);
    Some(snap)
  }

  /// Stall Reload path: count FLAC strikes; two within the pair window → WAV, else re-LOAD.
  async fn handle_stall_reload(self: &Arc<Self>, device_id: &str, generation: u64) {
    let flac_strike_fallback = self.note_stall_reload_and_flac_strike(device_id, generation);

    if let Some(first_strike) = flac_strike_fallback {
      tracing::info!(
        %device_id,
        generation,
        first_strike = ?first_strike,
        second_strike = ?FlacStrikeReason::StallWatchdogReload,
        "FLAC session hit two failure strikes within pair window; falling back to WAV BUFFERED"
      );
      self.remember_flac_fallback(device_id);
      if self.switch_playing_to_wav(device_id, generation).await {
        return;
      }
      tracing::debug!(
        %device_id,
        generation,
        "FLAC strike fallback did not switch (session gone or already WAV); continuing stall re-LOAD"
      );
    }

    tracing::warn!(%device_id, generation, "media stall detected; re-LOADing Cast media");
    let ok = self.reload_playing_media(device_id, generation, "stall", None).await;
    if !ok {
      self.terminal_stall_end(device_id, generation, "stall re-LOAD failed");
    }
  }

  /// Mark stall re-LOAD time; on FLAC LIVE record a strike.
  ///
  /// Returns the prior strike reason when pair-window fallback is due (always `Some` when
  /// [`FlacStrikeDecision::FallbackToWav`] — the pure helper only falls back with a previous).
  fn note_stall_reload_and_flac_strike(&self, device_id: &str, generation: u64) -> Option<FlacStrikeReason> {
    let mut devices = self.devices.lock();
    let Some(DeviceState::Playing { generation: live_gen, session }) = devices.get_mut(device_id).map(|s| &mut s.state)
    else {
      return None;
    };
    if *live_gen != generation {
      return None;
    }
    let now = Instant::now();
    session.last_stall_reload_at = Some(now);
    if session.egress != EgressKind::FlacLive {
      return None;
    }
    let previous = session.flac_strike;
    let decision = apply_flac_strike(session, now, FlacStrikeReason::StallWatchdogReload);
    drop(devices);
    if decision == FlacStrikeDecision::FallbackToWav {
      previous.map(|(_, r)| r)
    } else {
      None
    }
  }

  /// Tear down Playing and kick AirPlay so the phone shows the session is dead.
  ///
  /// Runs on the bridge's detached set: this is called from the stall watchdog task, which
  /// teardown aborts — tearing down inline would cancel the teardown itself at its first
  /// await (no Cast STOP, no kick).
  fn terminal_stall_end(&self, device_id: &str, generation: u64, reason: &'static str) {
    tracing::warn!(%device_id, generation, reason, "terminal media stall; ending session");
    let taken = self.take_live_session_if_gen(device_id, generation);
    let kick_target = self.airplay.clone();
    let id = device_id.to_owned();
    self.spawn_detached(async move {
      if let Some(session) = taken {
        teardown_playing(*session, TeardownCastPolicy::StopBestEffort).await;
      } else {
        // Session already gone (race with replace/end) but media/Cast may still be live —
        // still kick AirPlay below so the phone cannot keep a zombie session.
        tracing::warn!(
          device_id = %id,
          generation,
          reason,
          "terminal stall but no live session to tear down"
        );
      }
      // Always kick even when take returned None so AirPlay clients do not keep streaming.
      if let Some(airplay) = kick_target
        && let Err(err) = airplay.kick_clients(&id).await
      {
        tracing::warn!(device_id = %id, error = %err, "AirPlay kick after terminal stall failed");
      }
    });
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn body_is_stale_never_pull_uses_started_age() {
    let just_under = STALL_BODY_STALE
      .checked_sub(Duration::from_millis(1))
      .expect("STALL_BODY_STALE > 1ms");
    assert!(!body_is_stale(None, just_under));
    assert!(body_is_stale(None, STALL_BODY_STALE));
    assert!(!body_is_stale(Some(just_under), Duration::from_secs(100)));
    assert!(body_is_stale(Some(STALL_BODY_STALE), Duration::from_secs(0)));
  }

  #[test]
  fn stall_action_never_pull_with_frames_is_not_none() {
    // (a) last_write None + ring has frames + started older than STALL_BODY_STALE
    let body_stale = body_is_stale(None, STALL_BODY_STALE);
    assert!(body_stale);
    let action = stall_action(false, body_stale, 1_000, EgressKind::WavBuffered, false, false);
    assert_eq!(action, StallAction::Reload);
    let early = stall_action(false, body_stale, 1_000, EgressKind::FlacLive, true, false);
    assert_eq!(early, StallAction::EarlyFlacFallback);
  }

  #[test]
  fn stall_action_paused_or_empty_ring_is_none() {
    // (b) paused → None
    assert_eq!(
      stall_action(true, true, 1_000, EgressKind::FlacLive, true, false),
      StallAction::None
    );
    // (c) ring_frames == 0 → None
    assert_eq!(
      stall_action(false, true, 0, EgressKind::FlacLive, true, false),
      StallAction::None
    );
    assert_eq!(
      stall_action(false, false, 1_000, EgressKind::FlacLive, true, false),
      StallAction::None
    );
  }

  #[test]
  fn stall_action_repeat_within_window_is_terminal_end() {
    // (d) second stall within STALL_REPEAT_WINDOW → TerminalEnd
    // TerminalEnd maps to terminal_stall_end which kicks AirPlay.
    assert_eq!(
      stall_action(false, true, 500, EgressKind::WavBuffered, false, true),
      StallAction::TerminalEnd
    );
    // Early FLAC window wins over repeat (matches production order: early fallback first).
    assert_eq!(
      stall_action(false, true, 500, EgressKind::FlacLive, true, true),
      StallAction::EarlyFlacFallback
    );
  }
}
