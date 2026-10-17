//! AirPlay pause / resume / flush / volume handling with re-anchor debounce timers.
//!
//! Timers and the Cast PAUSE / PLAY commands they lead to run on the session's task set, so
//! replace and end teardown abort them with the generation.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio::time::sleep;

use crate::airplay::airplay_db_to_cast_linear;
use crate::cast::CastPool;

use super::Bridge;
use super::state::DeviceState;

/// After a long pause the Cast media session may be gone or holding a stale buffer tail.
/// Re-LOAD instead of PLAY when the pause exceeded this budget.
const LONG_PAUSE_RELOAD: Duration = Duration::from_secs(30);

/// iOS timeline re-anchors emit pause/flush that are not user intent.
///
/// Debounce Cast PAUSE and flush re-LOAD for this window: a `Resumed` (or PCM restart)
/// cancels both and logs "re-anchor absorbed". A real pause/skip fires ~0.7 s late.
const REANCHOR_DEBOUNCE: Duration = Duration::from_millis(700);

/// Whether resume should skip Cast PLAY because a re-LOAD already owns the worker (D3).
#[must_use]
const fn resume_should_skip_play_for_reload(reload_in_flight: bool) -> bool {
  reload_in_flight
}

/// Whether a detached PAUSE/PLAY from `expected_gen` may still touch Cast for `device_id`.
///
/// Requires both session liveness and a matching live generation so a command from a replaced
/// generation cannot land on the new session while the old `session_alive` is still true.
fn detached_cast_control_allowed(alive: bool, live_gen: Option<u64>, expected_gen: u64) -> bool {
  alive && live_gen == Some(expected_gen)
}

/// Cast follow-up chosen by [`Bridge::plan_resume`] (bookkeeping already applied).
#[derive(Debug)]
enum ResumeAction {
  /// Pending re-anchor pause/flush cancelled; no Cast command.
  Absorb,
  Play {
    generation: u64,
    pool: Arc<CastPool>,
    session_alive: Arc<AtomicBool>,
  },
  Reload {
    generation: u64,
    reason: &'static str,
    /// Owed flush request this re-LOAD satisfies (skipped if already covered).
    flush_request: Option<u64>,
  },
}

/// Pure decision for a `Paused` event (unit-tested).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReanchorPauseDecision {
  /// Already paused (pending or committed); ignore duplicate.
  Ignore,
  /// Arm (or re-arm) the pause debounce timer; set local `paused`.
  ArmDebounce,
}

/// Pure decision for a `Resumed` event after inspecting pending debounce state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReanchorResumeDecision {
  /// Cancel pending pause and/or flush; no Cast command.
  Absorb,
  /// Cast was paused; issue PLAY.
  Play,
  /// Cast was paused longer than [`LONG_PAUSE_RELOAD`]; re-LOAD.
  Reload,
  /// A fired flush never got its re-LOAD (a pause abandoned it); re-LOAD so Cast does not
  /// replay the pre-flush buffer tail.
  ReloadOwedFlush,
  /// Nothing to do (not paused, no pending timers).
  None,
}

const fn reanchor_on_paused(already_paused: bool) -> ReanchorPauseDecision {
  if already_paused {
    ReanchorPauseDecision::Ignore
  } else {
    ReanchorPauseDecision::ArmDebounce
  }
}

#[expect(
  clippy::fn_params_excessive_bools,
  reason = "pure decision helper mirrors resume pending/paused flags for unit tests"
)]
const fn reanchor_on_resumed(
  pause_pending: bool,
  flush_pending: bool,
  paused: bool,
  long_pause: bool,
  flush_owed: bool,
) -> ReanchorResumeDecision {
  // An owed flush re-LOAD outranks re-anchor absorb: the ring was already cleared, so only a
  // fresh LOAD drops the old track Cast still buffers (PLAY would resume that tail).
  if flush_owed {
    return ReanchorResumeDecision::ReloadOwedFlush;
  }
  // Pending pause means Cast was never paused — always absorb (no PLAY).
  if pause_pending {
    return ReanchorResumeDecision::Absorb;
  }
  // Cast PAUSE already issued (or long-lived paused without pending timer).
  if paused {
    if long_pause {
      return ReanchorResumeDecision::Reload;
    }
    return ReanchorResumeDecision::Play;
  }
  if flush_pending {
    return ReanchorResumeDecision::Absorb;
  }
  ReanchorResumeDecision::None
}

/// Whether a pause debounce timer arm may issue Cast PAUSE.
const fn reanchor_pause_timer_may_fire(still_paused: bool, arm_matches: bool) -> bool {
  still_paused && arm_matches
}

/// Whether a flush debounce timer arm may issue re-LOAD.
const fn reanchor_flush_timer_may_fire(arm_matches: bool) -> bool {
  arm_matches
}

/// Pause re-anchor debounce: Cast PAUSE unless a resume aborts this task first.
async fn pause_debounce_timer(bridge: Arc<Bridge>, device_id: String, generation: u64, arm: u64) {
  sleep(REANCHOR_DEBOUNCE).await;
  bridge.fire_pause_debounce(&device_id, generation, arm);
}

/// Flush re-anchor debounce: re-LOAD unless a resume aborts this task first.
async fn flush_debounce_timer(bridge: Arc<Bridge>, device_id: String, generation: u64, arm: u64) {
  sleep(REANCHOR_DEBOUNCE).await;
  bridge.fire_flush_debounce(&device_id, generation, arm);
}

/// Cast PAUSE after the debounce elapsed; re-checks generation + liveness on the blocking side.
async fn cast_pause_command(
  bridge: Arc<Bridge>,
  device_id: String,
  generation: u64,
  pool: Arc<CastPool>,
  session_alive: Arc<AtomicBool>,
) {
  let id = device_id.clone();
  let pause_result = tokio::task::spawn_blocking(move || {
    let live_gen = bridge.session_generation(&id);
    if !detached_cast_control_allowed(session_alive.load(Ordering::Acquire), live_gen, generation) {
      return Ok(());
    }
    pool.pause(&id)
  })
  .await;
  match pause_result {
    Ok(Ok(())) => {},
    Ok(Err(err)) => {
      tracing::debug!(%device_id, generation, error = %err, "Cast pause failed");
    },
    Err(err) => {
      tracing::debug!(%device_id, generation, error = %err, "Cast pause task join failed");
    },
  }
}

/// Cast PLAY on resume; a failed PLAY falls back to a generation-scoped re-LOAD.
async fn cast_play_command(
  bridge: Arc<Bridge>,
  device_id: String,
  generation: u64,
  pool: Arc<CastPool>,
  session_alive: Arc<AtomicBool>,
) {
  let play_id = device_id.clone();
  let bridge_for_check = Arc::clone(&bridge);
  let play_result = tokio::task::spawn_blocking(move || {
    let live_gen = bridge_for_check.session_generation(&play_id);
    if !detached_cast_control_allowed(session_alive.load(Ordering::Acquire), live_gen, generation) {
      return Ok(());
    }
    pool.play(&play_id)
  })
  .await;
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
  pub(super) async fn handle_volume(&self, device_id: &str, volume_db: f32) {
    let level = airplay_db_to_cast_linear(volume_db);
    // Volume only applies while a bridge session is Playing for this device.
    let has_session = {
      let mut devices = self.devices.lock();
      let found = devices.get_mut(device_id).is_some_and(|slot| {
        if let DeviceState::Playing { session, .. } = &mut slot.state {
          *session.last_volume_linear.lock() = Some(level);
          true
        } else {
          false
        }
      });
      drop(devices);
      found
    };
    if !has_session {
      return;
    }
    // Bounded pool call; volume is coalesced in the Cast worker. Await keeps
    // volume ordered after prior events on this device worker without blocking
    // other devices (per-device workers).
    let pool = Arc::clone(&self.cast_pool);
    let id = device_id.to_owned();
    match tokio::task::spawn_blocking(move || pool.set_volume(&id, level)).await {
      Ok(Ok(())) => {},
      Ok(Err(err)) => {
        tracing::debug!(device_id = %device_id, error = %err, "Cast volume sync failed");
      },
      Err(err) => {
        tracing::debug!(device_id = %device_id, error = %err, "Cast volume task join failed");
      },
    }
  }

  /// Pause Cast media (AirPlay rate=0). Keeps HTTP + session in `Playing { paused }`.
  ///
  /// Local paused bookkeeping is synchronous; Cast PAUSE is **debounced** by
  /// [`REANCHOR_DEBOUNCE`] so iOS re-anchors that resume quickly never reach Cast.
  /// The timer is a session task spawned under the slot lock, so its abort handle is
  /// recorded before it can fire.
  pub(super) fn handle_pause(self: &Arc<Self>, device_id: &str) {
    let mut devices = self.devices.lock();
    let Some(DeviceState::Playing { generation: live_gen, session }) =
      devices.get_mut(device_id).map(|slot| &mut slot.state)
    else {
      drop(devices);
      tracing::debug!(%device_id, "dropping Paused; no Playing generation");
      return;
    };
    if matches!(reanchor_on_paused(session.paused), ReanchorPauseDecision::Ignore) {
      return;
    }
    let generation = *live_gen;
    session.paused = true;
    session.playback_paused.store(true, Ordering::Release);
    session.paused_at = Some(Instant::now());
    let arm = session.pause_debounce_arm.wrapping_add(1);
    session.pause_debounce_arm = arm;
    if let Some(prev) = session.pause_debounce_cancel.take() {
      prev.abort();
    }
    let timer = pause_debounce_timer(Arc::clone(self), device_id.to_owned(), generation, arm);
    session.pause_debounce_cancel = Some(session.tasks.spawn(device_id, timer));
    drop(devices);
    tracing::debug!(%device_id, generation, arm, "AirPlay paused; arming re-anchor pause debounce");
  }

  /// Resume Cast media after AirPlay playout restarts.
  ///
  /// Cancels pending pause/flush re-anchor timers first. Cast PLAY only if Cast was
  /// actually paused (debounce already fired); re-LOAD after a long pause or when a fired
  /// flush still owes its re-LOAD. Detached PLAY re-checks generation + `session_alive` so a
  /// replaced generation cannot PLAY the new session.
  pub(super) fn handle_resume(self: &Arc<Self>, device_id: &str) {
    let Some(action) = self.plan_resume(device_id) else {
      return;
    };
    match action {
      ResumeAction::Absorb => {},
      ResumeAction::Play { generation, pool, session_alive } => {
        tracing::info!(%device_id, generation, "AirPlay resumed; resuming Cast media");
        let command = cast_play_command(Arc::clone(self), device_id.to_owned(), generation, pool, session_alive);
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
  /// and re-LOAD gates never see a stale `paused` after a cancelled pending pause.
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
    let pause_pending = session.pause_debounce_cancel.is_some();
    let flush_pending = session.flush_debounce_cancel.is_some();
    let long_pause = session.paused_at.is_some_and(|at| at.elapsed() >= LONG_PAUSE_RELOAD);
    let flush_owed = session.flush_reload_owed();
    let decision = reanchor_on_resumed(pause_pending, flush_pending, session.paused, long_pause, flush_owed);
    // D3: flush/resume re-LOAD already on the Cast worker — PLAY would congest and time out.
    let reload_in_flight = session.inflight_load.lock().is_some() || session.reload_flight.try_lock().is_err();

    session.paused = false;
    session.playback_paused.store(false, Ordering::Release);
    session.paused_at = None;
    // Bump arms too: a timer already past its sleep (waiting on this lock) must not fire.
    if let Some(timer) = session.pause_debounce_cancel.take() {
      session.pause_debounce_arm = session.pause_debounce_arm.wrapping_add(1);
      timer.abort();
    }
    if let Some(timer) = session.flush_debounce_cancel.take() {
      session.flush_debounce_arm = session.flush_debounce_arm.wrapping_add(1);
      timer.abort();
    }

    let action = match decision {
      ReanchorResumeDecision::Absorb => {
        tracing::debug!(%device_id, generation, "re-anchor absorbed");
        ResumeAction::Absorb
      },
      ReanchorResumeDecision::Play if resume_should_skip_play_for_reload(reload_in_flight) => {
        tracing::debug!(%device_id, generation, "skip Cast PLAY; re-LOAD already in flight");
        ResumeAction::Absorb
      },
      ReanchorResumeDecision::Play => ResumeAction::Play {
        generation,
        pool: Arc::clone(&session.pool),
        session_alive: Arc::clone(&session.session_alive),
      },
      ReanchorResumeDecision::Reload => ResumeAction::Reload {
        generation,
        reason: "long_pause_resume",
        flush_request: None,
      },
      ReanchorResumeDecision::ReloadOwedFlush => ResumeAction::Reload {
        generation,
        reason: "flush_owed_resume",
        flush_request: Some(session.flush_reload_requested),
      },
      ReanchorResumeDecision::None => ResumeAction::Absorb,
    };
    drop(devices);
    Some(action)
  }

  /// Flush: ring already cleared by ingest. Re-LOAD is **debounced** by
  /// [`REANCHOR_DEBOUNCE`]: a quick resume/re-anchor cancels it; only sustained silence
  /// (or track-skip drain) issues a generation-scoped re-LOAD.
  pub(super) fn handle_flush(self: &Arc<Self>, device_id: &str) {
    let mut devices = self.devices.lock();
    let Some(DeviceState::Playing { generation: live_gen, session }) =
      devices.get_mut(device_id).map(|slot| &mut slot.state)
    else {
      drop(devices);
      tracing::debug!(%device_id, "dropping Flushed; no Playing generation");
      return;
    };
    let generation = *live_gen;
    let arm = session.flush_debounce_arm.wrapping_add(1);
    session.flush_debounce_arm = arm;
    let rearmed = session.flush_debounce_cancel.take().is_some_and(|prev| {
      prev.abort();
      true
    });
    let timer = flush_debounce_timer(Arc::clone(self), device_id.to_owned(), generation, arm);
    session.flush_debounce_cancel = Some(session.tasks.spawn(device_id, timer));
    drop(devices);
    tracing::debug!(%device_id, generation, arm, rearmed, "AirPlay flushed; arming re-anchor flush debounce");
  }

  /// Pause debounce timer fired: issue Cast PAUSE if still paused for this arm/generation.
  fn fire_pause_debounce(self: &Arc<Self>, device_id: &str, generation: u64, arm: u64) {
    let mut devices = self.devices.lock();
    let Some(DeviceState::Playing { generation: live_gen, session }) =
      devices.get_mut(device_id).map(|slot| &mut slot.state)
    else {
      return;
    };
    if *live_gen != generation || !session.session_alive.load(Ordering::Acquire) {
      return;
    }
    if !reanchor_pause_timer_may_fire(session.paused, session.pause_debounce_arm == arm) {
      return;
    }
    session.pause_debounce_cancel = None;
    let command = cast_pause_command(
      Arc::clone(self),
      device_id.to_owned(),
      generation,
      Arc::clone(&session.pool),
      Arc::clone(&session.session_alive),
    );
    let _pause = session.tasks.spawn(device_id, command);
    drop(devices);
    tracing::info!(%device_id, generation, "AirPlay pause debounce elapsed; pausing Cast media");
  }

  /// Flush debounce timer fired: re-LOAD if this arm is still current (no re-anchor).
  ///
  /// The request is recorded first; while paused the re-LOAD is owed to the next resume
  /// instead of being attempted (and dropped) now.
  fn fire_flush_debounce(self: &Arc<Self>, device_id: &str, generation: u64, arm: u64) {
    let mut devices = self.devices.lock();
    let Some(DeviceState::Playing { generation: live_gen, session }) =
      devices.get_mut(device_id).map(|slot| &mut slot.state)
    else {
      return;
    };
    if *live_gen != generation || !session.session_alive.load(Ordering::Acquire) {
      return;
    }
    if !reanchor_flush_timer_may_fire(session.flush_debounce_arm == arm) {
      return;
    }
    session.flush_debounce_cancel = None;
    session.flush_reload_requested = session.flush_reload_requested.saturating_add(1);
    let request = session.flush_reload_requested;
    if session.paused {
      drop(devices);
      tracing::info!(
        %device_id,
        generation,
        request,
        "AirPlay flush debounce elapsed while paused; re-LOAD owed to resume"
      );
      return;
    }
    let reload = self.reload_task(device_id, generation, "flush", Some(request));
    let _reload = session.tasks.spawn(device_id, reload);
    drop(devices);
    tracing::info!(%device_id, generation, request, "AirPlay flush debounce elapsed; re-LOADing Cast media");
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use parking_lot::Mutex;

  use crate::audio::PcmRing;
  use crate::bridge::load::{LoadVolumePolicy, ReloadGate, volume_after_load};
  use crate::bridge::teardown::{TeardownCastPolicy, teardown_playing};
  use crate::bridge::test_support::{insert_playing, test_playing_session};
  use crate::media::MediaServer;
  use crate::registry::DeviceRegistry;

  #[tokio::test]
  async fn handle_pause_marks_paused_without_ending_session() {
    let registry = Arc::new(DeviceRegistry::new());
    let pool = Arc::new(CastPool::new(None));
    let bridge = Arc::new(Bridge::new(registry, Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let ring = Arc::new(PcmRing::new(2, 64));
    insert_playing(&bridge, "dev-pause", 1, test_playing_session(media, "dev-pause", pool, ring));

    bridge.handle_pause("dev-pause");
    {
      let devices = bridge.devices.lock();
      let DeviceState::Playing { session, .. } = &devices.get("dev-pause").expect("slot").state else {
        panic!("expected Playing");
      };
      assert!(session.paused);
      assert!(session.paused_at.is_some());
      drop(devices);
    }
    assert!(bridge.has_session("dev-pause"));
    bridge.handle_session_end("dev-pause").await;
  }

  /// `handle_pause` is sync and returns after arming debounce; Cast PAUSE is delayed.
  #[tokio::test]
  async fn handle_pause_returns_without_awaiting_cast() {
    let registry = Arc::new(DeviceRegistry::new());
    let pool = Arc::new(CastPool::new(None));
    let bridge = Arc::new(Bridge::new(registry, Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    insert_playing(
      &bridge,
      "dev-pause-fast",
      1,
      test_playing_session(media, "dev-pause-fast", pool, Arc::new(PcmRing::new(2, 64))),
    );

    let start = Instant::now();
    bridge.handle_pause("dev-pause-fast");
    let elapsed = start.elapsed();
    assert!(
      elapsed < Duration::from_millis(50),
      "handle_pause must return without awaiting Cast PAUSE; elapsed={elapsed:?}"
    );
    {
      let devices = bridge.devices.lock();
      let DeviceState::Playing { session, .. } = &devices.get("dev-pause-fast").expect("slot").state else {
        panic!("expected Playing");
      };
      assert!(session.paused, "local paused set before Cast PAUSE");
      assert!(
        session.pause_debounce_cancel.is_some(),
        "pause debounce armed; Cast PAUSE not issued yet"
      );
      drop(devices);
    }
    bridge.handle_session_end("dev-pause-fast").await;
  }

  #[tokio::test]
  async fn handle_volume_updates_last_volume_on_active_session() {
    let registry = Arc::new(DeviceRegistry::new());
    let pool = Arc::new(CastPool::new(None));
    let bridge = Bridge::new(registry, Arc::clone(&pool));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let last_volume_linear = Arc::new(Mutex::new(None));

    let mut session = test_playing_session(media, "dev-vol", pool, Arc::new(PcmRing::new(2, 64)));
    session.last_volume_linear = Arc::clone(&last_volume_linear);
    insert_playing(&bridge, "dev-vol", 1, session);

    bridge.handle_volume("dev-vol", 0.0).await;
    let stored = *last_volume_linear.lock();
    assert_eq!(stored, Some(1.0));
    assert_eq!(volume_after_load(LoadVolumePolicy::PreserveDevice), None);
    assert_eq!(volume_after_load(LoadVolumePolicy::Rollover { last_volume: stored }), Some(1.0));

    bridge.handle_session_end("dev-vol").await;
  }

  #[tokio::test]
  async fn flush_debounces_within_window() {
    let pool = Arc::new(CastPool::new(None));
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    insert_playing(
      &bridge,
      "dev-flush",
      1,
      test_playing_session(media, "dev-flush", pool, Arc::new(PcmRing::new(2, 64))),
    );

    bridge.handle_flush("dev-flush");
    // Immediate second flush re-arms the same debounce window (still no re-LOAD yet).
    bridge.handle_flush("dev-flush");
    {
      let devices = bridge.devices.lock();
      let DeviceState::Playing { session, .. } = &devices.get("dev-flush").expect("slot").state else {
        panic!("expected Playing");
      };
      assert_eq!(
        session.flush_reload_requested, 0,
        "flush re-LOAD only after silence past REANCHOR_DEBOUNCE"
      );
      assert!(session.flush_debounce_cancel.is_some(), "flush debounce armed");
      assert_eq!(session.flush_debounce_arm, 2, "second flush re-arms");
      drop(devices);
    }
    bridge.handle_session_end("dev-flush").await;
  }

  #[test]
  fn reanchor_decision_helpers() {
    assert_eq!(reanchor_on_paused(false), ReanchorPauseDecision::ArmDebounce);
    assert_eq!(reanchor_on_paused(true), ReanchorPauseDecision::Ignore);

    assert_eq!(
      reanchor_on_resumed(true, false, true, false, false),
      ReanchorResumeDecision::Absorb
    );
    assert_eq!(
      reanchor_on_resumed(true, true, true, false, false),
      ReanchorResumeDecision::Absorb
    );
    assert_eq!(
      reanchor_on_resumed(false, true, false, false, false),
      ReanchorResumeDecision::Absorb
    );
    assert_eq!(
      reanchor_on_resumed(false, false, true, false, false),
      ReanchorResumeDecision::Play
    );
    assert_eq!(
      reanchor_on_resumed(false, false, true, true, false),
      ReanchorResumeDecision::Reload
    );
    assert_eq!(
      reanchor_on_resumed(false, false, false, false, false),
      ReanchorResumeDecision::None
    );
    // An owed flush re-LOAD wins over absorb (pending pause) and over PLAY (Cast paused).
    assert_eq!(
      reanchor_on_resumed(true, false, true, false, true),
      ReanchorResumeDecision::ReloadOwedFlush
    );
    assert_eq!(
      reanchor_on_resumed(false, false, true, false, true),
      ReanchorResumeDecision::ReloadOwedFlush
    );

    assert!(reanchor_pause_timer_may_fire(true, true));
    assert!(!reanchor_pause_timer_may_fire(false, true));
    assert!(!reanchor_pause_timer_may_fire(true, false));
    assert!(reanchor_flush_timer_may_fire(true));
    assert!(!reanchor_flush_timer_may_fire(false));
  }

  /// Burst of Flushed + Resumed within the window produces zero Cast re-LOADs.
  #[tokio::test]
  async fn reanchor_flush_then_resume_within_window_skips_reload() {
    let pool = Arc::new(CastPool::new(None));
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    insert_playing(
      &bridge,
      "dev-reanchor",
      1,
      test_playing_session(media, "dev-reanchor", pool, Arc::new(PcmRing::new(2, 64))),
    );

    // iOS re-anchor path: flush + pause then resume (PCM / rate) inside the window.
    bridge.handle_flush("dev-reanchor");
    bridge.handle_pause("dev-reanchor");
    sleep(Duration::from_millis(50)).await;
    bridge.handle_resume("dev-reanchor");

    // Wait past the original arm window; cancelled arms must not fire.
    sleep(REANCHOR_DEBOUNCE + Duration::from_millis(100)).await;
    {
      let devices = bridge.devices.lock();
      let DeviceState::Playing { session, .. } = &devices.get("dev-reanchor").expect("slot").state else {
        panic!("expected Playing");
      };
      assert_eq!(session.flush_reload_requested, 0, "re-anchor must skip flush re-LOAD");
      assert!(!session.flush_reload_owed());
      assert!(!session.paused, "resume cleared local pause");
      assert!(session.pause_debounce_cancel.is_none());
      assert!(session.flush_debounce_cancel.is_none());
      assert!(
        session.inflight_load.lock().is_none(),
        "no Cast re-LOAD in flight after absorbed re-anchor"
      );
      drop(devices);
    }
    bridge.handle_session_end("dev-reanchor").await;
  }

  /// Paused with no resume within the window produces exactly one Cast PAUSE path.
  #[tokio::test]
  async fn reanchor_pause_without_resume_issues_cast_pause() {
    let pool = Arc::new(CastPool::new(None));
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    insert_playing(
      &bridge,
      "dev-pause-fire",
      1,
      test_playing_session(media, "dev-pause-fire", pool, Arc::new(PcmRing::new(2, 64))),
    );

    bridge.handle_pause("dev-pause-fire");
    {
      let devices = bridge.devices.lock();
      let DeviceState::Playing { session, .. } = &devices.get("dev-pause-fire").expect("slot").state else {
        panic!("expected Playing");
      };
      assert!(session.paused);
      assert!(session.pause_debounce_cancel.is_some());
      assert_eq!(session.pause_debounce_arm, 1);
      drop(devices);
    }

    sleep(REANCHOR_DEBOUNCE + Duration::from_millis(100)).await;
    {
      let devices = bridge.devices.lock();
      let DeviceState::Playing { session, .. } = &devices.get("dev-pause-fire").expect("slot").state else {
        panic!("expected Playing");
      };
      assert!(session.paused, "local pause remains after Cast PAUSE path");
      assert!(
        session.pause_debounce_cancel.is_none(),
        "debounce consumed when timer fires Cast PAUSE"
      );
      assert_eq!(session.pause_debounce_arm, 1, "exactly one pause arm (not re-armed)");
      drop(devices);
    }
    bridge.handle_session_end("dev-pause-fire").await;
  }

  /// Real flush-then-silence produces one re-LOAD after the window.
  #[tokio::test]
  async fn reanchor_flush_then_silence_reloads() {
    let pool = Arc::new(CastPool::new(None));
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    insert_playing(
      &bridge,
      "dev-flush-fire",
      1,
      test_playing_session(media, "dev-flush-fire", pool, Arc::new(PcmRing::new(2, 64))),
    );

    bridge.handle_flush("dev-flush-fire");
    bridge.handle_flush("dev-flush-fire"); // coalesce arms
    {
      let devices = bridge.devices.lock();
      let DeviceState::Playing { session, .. } = &devices.get("dev-flush-fire").expect("slot").state else {
        panic!("expected Playing");
      };
      assert_eq!(session.flush_reload_requested, 0);
      assert_eq!(session.flush_debounce_arm, 2);
      drop(devices);
    }

    sleep(REANCHOR_DEBOUNCE + Duration::from_millis(150)).await;
    {
      let devices = bridge.devices.lock();
      let DeviceState::Playing { session, .. } = &devices.get("dev-flush-fire").expect("slot").state else {
        panic!("expected Playing");
      };
      assert_eq!(
        session.flush_reload_requested, 1,
        "silence past window must request exactly one flush re-LOAD"
      );
      assert!(session.flush_debounce_cancel.is_none());
      assert_eq!(session.flush_debounce_arm, 2, "coalesced to a single fire arm");
      drop(devices);
    }
    // Allow detached re-LOAD task to observe missing Cast worker and finish.
    sleep(Duration::from_millis(50)).await;
    bridge.handle_session_end("dev-flush-fire").await;
  }

  /// A resume that cancels a still-pending pause clears every piece of local pause state, so
  /// the stall watchdog and re-LOAD gates are not left muted by a stale `paused`.
  #[tokio::test]
  async fn resume_cancelling_pending_pause_clears_pause_state() {
    let pool = Arc::new(CastPool::new(None));
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    insert_playing(
      &bridge,
      "dev-resume-pending",
      1,
      test_playing_session(media, "dev-resume-pending", pool, Arc::new(PcmRing::new(2, 64))),
    );

    bridge.handle_pause("dev-resume-pending");
    let action = bridge.plan_resume("dev-resume-pending").expect("Playing");
    assert!(matches!(action, ResumeAction::Absorb), "pending pause absorbs: {action:?}");
    {
      let devices = bridge.devices.lock();
      let DeviceState::Playing { session, .. } = &devices.get("dev-resume-pending").expect("slot").state else {
        panic!("expected Playing");
      };
      assert!(!session.paused);
      assert!(!session.playback_paused.load(Ordering::Acquire));
      assert!(session.paused_at.is_none());
      assert!(session.pause_debounce_cancel.is_none());
      drop(devices);
    }
    assert!(matches!(
      bridge.reload_gate("dev-resume-pending", 1, None),
      ReloadGate::Proceed { .. }
    ));
    bridge.handle_session_end("dev-resume-pending").await;
  }

  /// Track skip while paused: the flush timer fires during the pause, so its re-LOAD is owed
  /// and the resume re-LOADs instead of resuming the pre-flush buffer tail with Cast PLAY.
  #[tokio::test]
  async fn flush_fired_while_paused_reloads_on_resume() {
    let pool = Arc::new(CastPool::new(None));
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    insert_playing(
      &bridge,
      "dev-flush-paused",
      1,
      test_playing_session(media, "dev-flush-paused", pool, Arc::new(PcmRing::new(2, 64))),
    );

    bridge.handle_pause("dev-flush-paused");
    bridge.handle_flush("dev-flush-paused");
    sleep(REANCHOR_DEBOUNCE + Duration::from_millis(150)).await;
    {
      let devices = bridge.devices.lock();
      let DeviceState::Playing { session, .. } = &devices.get("dev-flush-paused").expect("slot").state else {
        panic!("expected Playing");
      };
      assert!(session.paused);
      assert!(session.pause_debounce_cancel.is_none(), "Cast PAUSE path fired");
      assert!(session.flush_reload_owed(), "flush fired while paused stays owed");
      drop(devices);
    }

    let action = bridge.plan_resume("dev-flush-paused").expect("Playing");
    assert!(
      matches!(
        action,
        ResumeAction::Reload {
          generation: 1,
          flush_request: Some(1),
          ..
        }
      ),
      "resume after a paused flush must re-LOAD, got {action:?}"
    );
    bridge.handle_session_end("dev-flush-paused").await;
  }

  /// A flush re-LOAD that queued behind an in-flight re-LOAD and was then abandoned by a pause
  /// keeps its intent: the next resume re-LOADs even though the pause never reached Cast.
  #[tokio::test]
  async fn flush_reload_abandoned_by_pause_is_owed_to_resume() {
    let pool = Arc::new(CastPool::new(None));
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
    bridge.handle_pause("dev-flush-race");
    drop(in_flight);
    let reloaded = tokio::time::timeout(Duration::from_secs(2), queued)
      .await
      .expect("queued re-LOAD finishes")
      .expect("join");
    assert!(!reloaded, "pause must abort the queued flush re-LOAD");

    // Resume inside the debounce window: the pause never reached Cast, but the flush did.
    let action = bridge.plan_resume("dev-flush-race").expect("Playing");
    assert!(
      matches!(action, ResumeAction::Reload { flush_request: Some(1), .. }),
      "abandoned flush must be re-LOADed on resume, got {action:?}"
    );
    bridge.handle_session_end("dev-flush-race").await;
  }

  #[test]
  fn resume_skips_play_when_reload_in_flight() {
    assert!(resume_should_skip_play_for_reload(true));
    assert!(!resume_should_skip_play_for_reload(false));
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
