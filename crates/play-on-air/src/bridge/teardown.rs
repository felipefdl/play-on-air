//! Session end and replace teardown (media first, ownership clear, then best-effort Cast STOP).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::cast::CastControl;
use crate::media::MediaServerHandle;

use super::load::INFLIGHT_LOAD_JOIN_TIMEOUT;
use super::state::{DeviceState, PlayingSession};
use super::{Bridge, RingLookup};

/// How long teardown waits for the rollover async task after cancel/abort.
const ROLLOVER_TASK_JOIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Whether teardown should issue Cast STOP after media shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TeardownCastPolicy {
  /// Full end: media shutdown then timed Cast STOP best-effort.
  StopBestEffort,
  /// New LOAD will replace the Cast app session on this device.
  ///
  /// Skip STOP so the per-device pool worker is not HOL-blocked before the new LOAD.
  /// Old [`MediaServer`] is still shut down so the prior HTTP body stops immediately.
  SkipStopForReplace,
}

/// Ordered teardown steps for a full session end (media first, then Cast STOP).
///
/// [`Bridge::handle_session_end`] always runs these in order: media HTTP first,
/// clear pool ownership tracking, then timed best-effort Cast STOP. Media must
/// not wait on STOP success. Ownership clear must not wait on STOP either.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEndStep {
  /// Shut down the local media HTTP server (stop underrun immediately).
  MediaShutdown,
  /// Clear Cast pool ownership tracking so probes stand down (no false steal).
  ClearSessionOwnership,
  /// Best-effort Cast STOP with timeout (may fail or time out).
  CastStopBestEffort,
}

/// Shipped full session-end order (media → ownership clear → Cast STOP).
pub const fn session_end_steps() -> [SessionEndStep; 3] {
  [
    SessionEndStep::MediaShutdown,
    SessionEndStep::ClearSessionOwnership,
    SessionEndStep::CastStopBestEffort,
  ]
}

/// Whether replace teardown should skip Cast STOP (new LOAD replaces the app session).
const fn replace_skips_cast_stop(policy: TeardownCastPolicy) -> bool {
  matches!(policy, TeardownCastPolicy::SkipStopForReplace)
}

impl Bridge {
  /// Tear `session` down on the bridge's detached set (joined by `run` on exit).
  pub(super) fn detach_teardown(&self, session: PlayingSession, policy: TeardownCastPolicy) {
    self.spawn_detached(async move {
      let device_id = session.device_id.clone();
      teardown_playing(session, policy).await;
      tracing::debug!(%device_id, ?policy, "detached session teardown complete");
    });
  }

  /// End every live session and join detached teardowns (bridge exit), bounded by `budget`.
  ///
  /// Workers are already stopped, so a remaining `Starting` has no owner and goes `Idle`.
  pub(super) async fn end_all_sessions(&self, budget: Duration) {
    let live: Vec<Box<PlayingSession>> = {
      let mut devices = self.devices.lock();
      let sessions = devices
        .iter_mut()
        .filter_map(|(device_id, slot)| match std::mem::take(&mut slot.state) {
          DeviceState::Playing { generation, session } => {
            tracing::info!(%device_id, generation, "ending bridge session on shutdown");
            Some(session)
          },
          DeviceState::Starting { generation } => {
            tracing::debug!(%device_id, generation, "clearing orphaned Starting on shutdown");
            None
          },
          DeviceState::Following { generation, follower } => {
            tracing::debug!(%device_id, generation, "ending group follower on shutdown");
            follower.pump.stop();
            None
          },
          DeviceState::Idle => None,
        })
        .collect();
      drop(devices);
      sessions
    };
    for session in live {
      self.spawn_detached(teardown_playing(*session, TeardownCastPolicy::StopBestEffort));
    }

    let mut detached = std::mem::take(&mut *self.detached.lock());
    let drained = tokio::time::timeout(budget, async {
      while let Some(result) = detached.join_next().await {
        if let Err(err) = result
          && !err.is_cancelled()
        {
          tracing::warn!(error = %err, "detached bridge task panicked");
        }
      }
    })
    .await;
    if drained.is_err() {
      tracing::warn!(
        budget_ms = budget.as_millis(),
        "bridge teardown join timed out; aborting remaining"
      );
      detached.abort_all();
      while detached.join_next().await.is_some() {}
    }
    // Session hosts close after the teardowns sent their Cast STOP through them.
    self.forget_all_groups();
  }
}

pub(super) async fn teardown_playing(session: PlayingSession, policy: TeardownCastPolicy) {
  let device_id = session.device_id.clone();
  let media_key = session.route.media();
  if let Some(pump) = session.source {
    pump.stop();
  }
  // Replace path: clear late_stop_allowed BEFORE session_alive so a concurrent late LOAD
  // never observes (dead, late_stop_allowed=true). Intermediate (alive, !late_stop) is safe
  // (late_load_should_stop is false). Full end leaves late_stop_allowed true.
  if replace_skips_cast_stop(policy) {
    session.late_stop_allowed.store(false, Ordering::Release);
  } else {
    // Full end: stand ownership probes down immediately (before long joins / STOP I/O)
    // so a terminal-stall media absence cannot be declared as a false steal.
    // Do not clear on replace — a new LOAD may already own (or will own) active.
    session.pool.session_ended(&media_key);
  }
  session.session_alive.store(false, Ordering::Release);

  // Abort + join session-scoped work (re-anchor timers, Cast PAUSE/PLAY, re-LOAD tasks) so
  // nothing from this generation can touch Cast after teardown. A re-LOAD aborted mid-flight
  // leaves its blocking LOAD in `inflight_load`, joined below.
  session.tasks.abort_and_join(&device_id).await;

  if let Some(tx) = session.watchdog_cancel {
    let _cancelled = tx.send(());
  }
  if let Some(task) = session.watchdog_task {
    task.abort();
    let _watchdog_join = tokio::time::timeout(ROLLOVER_TASK_JOIN_TIMEOUT, task).await;
  }

  if let Some(tx) = session.rollover_cancel {
    let _cancelled = tx.send(());
  }
  if let Some(task) = session.rollover_task {
    task.abort();
    match tokio::time::timeout(ROLLOVER_TASK_JOIN_TIMEOUT, task).await {
      Ok(Ok(())) => {},
      Ok(Err(err)) if err.is_cancelled() => {},
      Ok(Err(err)) => {
        tracing::warn!(%device_id, error = %err, "rollover re-LOAD loop task panicked");
      },
      Err(_) => {
        tracing::warn!(%device_id, "rollover re-LOAD loop join timed out");
      },
    }
  }

  let inflight = session.inflight_load.lock().take();
  if let Some(handle) = inflight {
    match tokio::time::timeout(INFLIGHT_LOAD_JOIN_TIMEOUT, handle).await {
      Ok(Ok(())) => {},
      Ok(Err(err)) if err.is_cancelled() => {},
      Ok(Err(err)) => {
        tracing::warn!(%device_id, error = %err, "in-flight Cast LOAD task panicked");
      },
      Err(_) => {
        tracing::warn!(%device_id, "in-flight Cast LOAD join timed out");
      },
    }
  }

  let media = session.media;
  let pool = session.pool;
  if tokio::task::spawn_blocking(move || end_media_and_maybe_cast_stop(media, pool.as_ref(), &media_key, policy))
    .await
    .is_err()
  {
    tracing::warn!(%device_id, "session teardown task panicked");
  }
}

/// Run media shutdown then optional Cast STOP (blocking; call from `spawn_blocking`).
fn end_media_and_maybe_cast_stop(
  media: MediaServerHandle,
  pool: &dyn CastControl,
  device_id: &str,
  policy: TeardownCastPolicy,
) {
  // Always stop media HTTP first so underrun ends even if Cast STOP hangs.
  // `shutdown` / Drop invalidates live bodies (Empty + generation bump) before abort.
  media.shutdown();
  if replace_skips_cast_stop(policy) {
    // New LOAD is about to replace the Cast app session; avoid HOL-blocking it.
    // Do not session_ended here — that would clear the superseding LOAD's active.
    return;
  }
  // Ownership was already stood down via `session_ended` at the start of full
  // teardown. Issue Cast STOP best-effort (no-ops if SessionEnded already cleared
  // active; still useful when only stop_best_effort is used from other paths).
  // Retry once on timeout/failure so Nest/Cast does not keep pulling a dead URL.
  let stop_timeout = Duration::from_secs(2);
  if !pool.stop_best_effort(device_id, stop_timeout) {
    tracing::warn!(%device_id, "Cast STOP best-effort failed or timed out; retrying once");
    let _retry_ok = pool.stop_best_effort(device_id, stop_timeout);
  }
}

impl Bridge {
  /// End the bridge session for `device_id` if any (media down + Cast STOP best-effort).
  ///
  /// Used by tests and shutdown-style callers that end one device's session directly.
  pub async fn end_session(&self, device_id: &str) {
    self.handle_session_end(device_id).await;
  }

  /// Cast reported that another sender took the session on worker `cast_key` (a speaker, or a
  /// multizone session host): end the bridge session that played there, with its whole group.
  ///
  /// Returns the AirPlay devices whose senders should be disconnected (the owner and every
  /// group follower), so the app does not have to wait for the AirPlay stack to emit `Ended`.
  pub async fn end_cast_session(&self, cast_key: &str) -> Vec<String> {
    let owner = self.owner_for_cast_key(cast_key).unwrap_or_else(|| cast_key.to_owned());
    let group = self.session_group(&owner);
    self.handle_session_end(&owner).await;
    let mut ended = vec![owner];
    if let Some(uuid) = group {
      let (followers, host) = self.end_group(&uuid);
      ended.extend(followers);
      if let Some(key) = host {
        self.close_session_host(key);
      }
    }
    ended
  }

  /// Tear down the current live generation for `device_id` (full STOP).
  pub(super) async fn handle_session_end(&self, device_id: &str) {
    let removed = self.take_live_session(device_id);
    let Some((_generation, session)) = removed else {
      return;
    };
    teardown_playing(*session, TeardownCastPolicy::StopBestEffort).await;
    tracing::info!(%device_id, "bridge session ended (media dropped; Cast STOP best-effort)");
  }

  /// Resolve `Ended` with generation / ring defense in depth.
  ///
  /// For `Playing`: tear down only when the live session's ring is no longer the
  /// receiver's current ring (genuine end) or the receiver is gone. Ring still
  /// current ⇒ stale `Ended` for a prior generation.
  ///
  /// For `Starting`: always clear that generation. Ring still current means the
  /// client ended while we were still starting — not a stale event.
  pub(super) async fn handle_ended(self: &Arc<Self>, device_id: &str, rings: &dyn RingLookup) {
    enum EndedDecision {
      Starting { generation: u64 },
      Playing { generation: u64, stale: bool },
      Following { generation: u64, stale: bool },
    }

    let decision = {
      let devices = self.devices.lock();
      let Some(slot) = devices.get(device_id) else {
        return;
      };
      let result = match &slot.state {
        DeviceState::Playing { generation, session } => {
          let still_current = rings
            .ring_for(device_id)
            .is_some_and(|current| Arc::ptr_eq(&current, session.airplay_ring()));
          Some(EndedDecision::Playing {
            generation: *generation,
            stale: still_current,
          })
        },
        DeviceState::Following { generation, follower } => {
          let still_current = rings
            .ring_for(device_id)
            .is_some_and(|current| Arc::ptr_eq(&current, follower.pump.source()));
          Some(EndedDecision::Following {
            generation: *generation,
            stale: still_current,
          })
        },
        DeviceState::Starting { generation } => Some(EndedDecision::Starting { generation: *generation }),
        DeviceState::Idle => {
          tracing::debug!(%device_id, "dropping Ended; no live session generation");
          None
        },
      };
      drop(devices);
      result
    };

    match decision {
      None => {},
      Some(EndedDecision::Starting { generation }) => {
        self.clear_starting_if(device_id, generation);
        tracing::info!(%device_id, generation, "bridge Starting cancelled by Ended");
        self.group_check_owner(device_id);
      },
      Some(
        EndedDecision::Playing { generation, stale: true } | EndedDecision::Following { generation, stale: true },
      ) => {
        tracing::debug!(%device_id, generation, "dropping stale Ended (session ring still current)");
      },
      Some(EndedDecision::Following { generation, stale: false }) => {
        self.end_following(device_id, generation);
      },
      Some(EndedDecision::Playing { generation, stale: false }) => {
        // A group owner leaving while members still play hands its Cast session over.
        if self.group_owner_left(device_id, generation) {
          return;
        }
        let removed = self.take_live_session_if_gen(device_id, generation);
        if let Some(session) = removed {
          teardown_playing(*session, TeardownCastPolicy::StopBestEffort).await;
          tracing::info!(%device_id, generation, "bridge session ended (media dropped; Cast STOP best-effort)");
        }
        self.group_check_owner(device_id);
      },
    }
  }

  fn take_live_session(&self, device_id: &str) -> Option<(u64, Box<PlayingSession>)> {
    let mut devices = self.devices.lock();
    let slot = devices.get_mut(device_id)?;
    let result = match std::mem::replace(&mut slot.state, DeviceState::Idle) {
      DeviceState::Playing { generation, session } => Some((generation, session)),
      DeviceState::Idle => None,
      DeviceState::Starting { generation } => {
        // Cancel Starting (leave Idle) so ownership kick / external end cannot leave
        // `has_session` true forever.
        tracing::debug!(%device_id, generation, "cleared Starting on session end");
        None
      },
      DeviceState::Following { generation, follower } => {
        tracing::debug!(%device_id, generation, "cleared group follower on session end");
        follower.pump.stop();
        None
      },
    };
    drop(devices);
    result
  }

  pub(super) fn take_live_session_if_gen(&self, device_id: &str, generation: u64) -> Option<Box<PlayingSession>> {
    let mut devices = self.devices.lock();
    let slot = devices.get_mut(device_id)?;
    let matches_gen = matches!(
      &slot.state,
      DeviceState::Playing { generation: g, .. } if *g == generation
    );
    if !matches_gen {
      return None;
    }
    let DeviceState::Playing { session, .. } = std::mem::replace(&mut slot.state, DeviceState::Idle) else {
      // matches_gen guaranteed Playing; restore Idle if the impossible happens.
      slot.state = DeviceState::Idle;
      return None;
    };
    drop(devices);
    Some(session)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::cast::CastControl;
  use crate::cast::CastPool;
  use std::sync::atomic::{AtomicBool, AtomicUsize};
  use std::time::Instant;

  use parking_lot::Mutex;
  use tokio::sync::oneshot;
  use tokio::time::sleep;

  use crate::audio::PcmRing;
  use crate::bridge::load::late_load_should_stop;
  use crate::bridge::state::{DeviceSlot, EgressKind};
  use crate::bridge::test_support::{FixedRingLookup, http_get_status_ok, insert_playing, test_playing_session};
  use crate::media::MediaServer;
  use crate::registry::{Device, DeviceRegistry};

  #[test]
  fn session_end_steps_media_before_cast_stop() {
    let steps = session_end_steps();
    assert_eq!(steps.len(), 3);
    assert_eq!(steps[0], SessionEndStep::MediaShutdown);
    assert_eq!(steps[1], SessionEndStep::ClearSessionOwnership);
    assert_eq!(steps[2], SessionEndStep::CastStopBestEffort);
  }

  #[test]
  fn replace_teardown_skips_cast_stop() {
    assert!(replace_skips_cast_stop(TeardownCastPolicy::SkipStopForReplace));
    assert!(!replace_skips_cast_stop(TeardownCastPolicy::StopBestEffort));
  }

  #[tokio::test]
  async fn stale_ended_dropped_when_ring_still_current() {
    let registry = Arc::new(DeviceRegistry::new());
    let pool: Arc<dyn CastControl> = Arc::new(CastPool::new(None));
    let bridge = Arc::new(Bridge::new(registry, Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let ring = Arc::new(PcmRing::new(2, 64));
    insert_playing(
      &bridge,
      "dev-stale-end",
      3,
      test_playing_session(media, "dev-stale-end", pool, Arc::clone(&ring)),
    );
    let rings = FixedRingLookup { current: Some(Arc::clone(&ring)) };

    bridge.handle_ended("dev-stale-end", &rings).await;
    assert!(bridge.has_session("dev-stale-end"), "stale Ended must not tear down");
    assert_eq!(bridge.session_generation("dev-stale-end"), Some(3));
    bridge.handle_session_end("dev-stale-end").await;
  }

  #[tokio::test]
  async fn ended_tears_down_when_ring_no_longer_current() {
    let registry = Arc::new(DeviceRegistry::new());
    let pool: Arc<dyn CastControl> = Arc::new(CastPool::new(None));
    let bridge = Arc::new(Bridge::new(registry, Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let health_url = format!("{}/health", media.base_url);
    let ring = Arc::new(PcmRing::new(2, 64));
    insert_playing(
      &bridge,
      "dev-end",
      1,
      test_playing_session(media, "dev-end", pool, Arc::clone(&ring)),
    );
    // Receiver installed a placeholder / new ring after genuine end.
    let rings = FixedRingLookup {
      current: Some(Arc::new(PcmRing::new(2, 64))),
    };

    bridge.handle_ended("dev-end", &rings).await;
    assert!(!bridge.has_session("dev-end"));
    assert!(!http_get_status_ok(&health_url).await);
  }

  /// Replace and full end abort the generation's session tasks (Cast PAUSE/PLAY, re-LOAD,
  /// debounce timers) instead of leaving them detached; stale-generation spawns never run.
  #[tokio::test]
  async fn teardown_aborts_session_tasks_on_replace_and_end() {
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None))));
    let pool = Arc::clone(&bridge.cast_pool);
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    insert_playing(
      &bridge,
      "dev-tasks",
      1,
      test_playing_session(media, "dev-tasks", Arc::clone(&pool), Arc::new(PcmRing::new(2, 64))),
    );
    let (replaced_tx, replaced_rx) = oneshot::channel::<()>();
    bridge.spawn_session_task("dev-tasks", 1, async move {
      let _held = replaced_tx;
      std::future::pending::<()>().await;
    });

    // Replacement start: the prior generation tears down on the bridge's detached set.
    let (new_gen, prior) = bridge.accept_start("dev-tasks");
    assert_eq!(new_gen, 2);
    bridge.detach_teardown(*prior.expect("prior Playing"), TeardownCastPolicy::SkipStopForReplace);
    let replaced = tokio::time::timeout(Duration::from_secs(3), replaced_rx).await;
    assert!(
      matches!(replaced, Ok(Err(_))),
      "replace teardown must abort the old generation's session tasks"
    );

    // Work spawned for the replaced generation is dropped unrun.
    let (stale_tx, stale_rx) = oneshot::channel::<()>();
    bridge.spawn_session_task("dev-tasks", 1, async move {
      let _sent = stale_tx.send(());
    });
    let stale = tokio::time::timeout(Duration::from_secs(1), stale_rx).await;
    assert!(matches!(stale, Ok(Err(_))), "stale-generation task must not run");

    let media_end = MediaServer::start("127.0.0.1").await.expect("media");
    insert_playing(
      &bridge,
      "dev-tasks",
      3,
      test_playing_session(media_end, "dev-tasks", pool, Arc::new(PcmRing::new(2, 64))),
    );
    let (ended_tx, ended_rx) = oneshot::channel::<()>();
    bridge.spawn_session_task("dev-tasks", 3, async move {
      let _held = ended_tx;
      std::future::pending::<()>().await;
    });
    bridge.handle_session_end("dev-tasks").await;
    let ended = tokio::time::timeout(Duration::from_secs(1), ended_rx).await;
    assert!(
      matches!(ended, Ok(Err(_))),
      "full end must join the session's tasks before returning"
    );
  }

  #[tokio::test]
  async fn handle_session_end_shuts_media_before_cast_stop_and_does_not_hang() {
    let registry = Arc::new(DeviceRegistry::new());
    let _appeared = registry.appear(Device {
      id: "dev-1".to_owned(),
      name: "Test".to_owned(),
      model: String::new(),
      host: "127.0.0.1".to_owned(),
      hostname: "test.local".to_owned(),
      port: 9,
      last_seen: Instant::now(),
      instance: "dev-1".to_owned(),
      pending_leave_deadline: None,
      pending_leave_since: None,
    });
    let pool: Arc<dyn CastControl> = Arc::new(CastPool::new(None));
    let bridge = Bridge::new(Arc::clone(&registry), Arc::clone(&pool));

    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let health_url = format!("{}/health", media.base_url);
    assert!(http_get_status_ok(&health_url).await, "media must be up before end");

    insert_playing(
      &bridge,
      "dev-1",
      1,
      test_playing_session(media, "dev-1", Arc::clone(&pool), Arc::new(PcmRing::new(2, 64))),
    );

    let start = Instant::now();
    bridge.handle_session_end("dev-1").await;
    let elapsed = start.elapsed();
    assert!(
      elapsed < Duration::from_secs(4),
      "session end must not hang on Cast STOP; elapsed={elapsed:?}"
    );
    assert!(!bridge.has_session("dev-1"), "session removed from live map");
    assert!(
      !http_get_status_ok(&health_url).await,
      "media.shutdown must run on session end so body stops"
    );
  }

  #[tokio::test]
  async fn session_end_cancels_rollover_reload_loop() {
    let registry = Arc::new(DeviceRegistry::new());
    let pool: Arc<dyn CastControl> = Arc::new(CastPool::new(None));
    let bridge = Bridge::new(registry, Arc::clone(&pool));

    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let rollover = media.rollover_signal();
    let (cancel_tx, mut cancel_rx) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
      let mut seen = 0_u64;
      loop {
        tokio::select! {
          _ = &mut cancel_rx => break,
          count = rollover.wait_past(seen) => {
            seen = count;
          },
        }
      }
    });

    let mut session = test_playing_session(media, "dev-roll", pool, Arc::new(PcmRing::new(2, 64)));
    session.egress = EgressKind::WavBuffered;
    session.rollover_cancel = Some(cancel_tx);
    session.rollover_task = Some(task);
    insert_playing(&bridge, "dev-roll", 1, session);

    bridge.handle_session_end("dev-roll").await;
    assert!(!bridge.has_session("dev-roll"));
  }

  #[tokio::test]
  async fn session_end_joins_inflight_load_and_marks_session_dead() {
    let registry = Arc::new(DeviceRegistry::new());
    let pool: Arc<dyn CastControl> = Arc::new(CastPool::new(None));
    let bridge = Bridge::new(registry, Arc::clone(&pool));
    let media = MediaServer::start("127.0.0.1").await.expect("media");

    let session_alive = Arc::new(AtomicBool::new(true));
    let inflight_load = Arc::new(Mutex::new(None));
    let alive_for_load = Arc::clone(&session_alive);
    let started = Arc::new(AtomicBool::new(false));
    let started_flag = Arc::clone(&started);

    let handle = tokio::task::spawn_blocking(move || {
      started_flag.store(true, Ordering::Release);
      std::thread::sleep(Duration::from_millis(200));
      let still_alive = alive_for_load.load(Ordering::Acquire);
      assert!(!still_alive, "session must be marked dead before inflight load finishes join");
    });
    *inflight_load.lock() = Some(handle);

    let mut session = test_playing_session(media, "dev-inflight", pool, Arc::new(PcmRing::new(2, 64)));
    session.session_alive = Arc::clone(&session_alive);
    session.inflight_load = Arc::clone(&inflight_load);
    insert_playing(&bridge, "dev-inflight", 1, session);

    let wait_start = Instant::now();
    while !started.load(Ordering::Acquire) && wait_start.elapsed() < Duration::from_secs(2) {
      sleep(Duration::from_millis(5)).await;
    }
    assert!(started.load(Ordering::Acquire), "inflight load must start");

    bridge.handle_session_end("dev-inflight").await;
    assert!(!session_alive.load(Ordering::Acquire));
    assert!(inflight_load.lock().is_none(), "inflight load handle must be taken and joined");
    assert!(!bridge.has_session("dev-inflight"));
  }

  /// Rollover parks on cancel while a blocking LOAD sits in `inflight_load`.
  /// Teardown must abort+join the async task, then take and join the LOAD handle.
  #[tokio::test]
  async fn session_end_joins_rollover_task_then_inflight_load() {
    let registry = Arc::new(DeviceRegistry::new());
    let pool: Arc<dyn CastControl> = Arc::new(CastPool::new(None));
    let bridge = Bridge::new(registry, Arc::clone(&pool));
    let media = MediaServer::start("127.0.0.1").await.expect("media");

    let session_alive = Arc::new(AtomicBool::new(true));
    let inflight_load = Arc::new(Mutex::new(None));
    let load_finished = Arc::new(AtomicBool::new(false));
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

    let inflight_for_task = Arc::clone(&inflight_load);
    let finished_flag = Arc::clone(&load_finished);
    let task = tokio::spawn(async move {
      let done = Arc::clone(&finished_flag);
      let handle = tokio::task::spawn_blocking(move || {
        std::thread::sleep(Duration::from_millis(200));
        done.store(true, Ordering::Release);
      });
      *inflight_for_task.lock() = Some(handle);
      match cancel_rx.await {
        Ok(()) | Err(_) => {},
      }
    });

    let mut session = test_playing_session(media, "dev-join-order", pool, Arc::new(PcmRing::new(2, 64)));
    session.session_alive = Arc::clone(&session_alive);
    session.inflight_load = Arc::clone(&inflight_load);
    session.rollover_cancel = Some(cancel_tx);
    session.rollover_task = Some(task);
    insert_playing(&bridge, "dev-join-order", 1, session);

    let wait_start = Instant::now();
    while inflight_load.lock().is_none() && wait_start.elapsed() < Duration::from_secs(2) {
      sleep(Duration::from_millis(5)).await;
    }
    assert!(inflight_load.lock().is_some(), "rollover task must publish inflight LOAD");

    bridge.handle_session_end("dev-join-order").await;
    assert!(!session_alive.load(Ordering::Acquire));
    assert!(
      load_finished.load(Ordering::Acquire),
      "teardown must join inflight blocking LOAD after joining rollover task"
    );
    assert!(inflight_load.lock().is_none());
    assert!(!bridge.has_session("dev-join-order"));
  }

  #[tokio::test]
  async fn end_session_clears_starting() {
    let bridge = Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None)));
    {
      let mut devices = bridge.devices.lock();
      drop(devices.insert(
        "dev-start-end".to_owned(),
        DeviceSlot {
          generation: 2,
          state: DeviceState::Starting { generation: 2 },
        },
      ));
    }
    assert!(bridge.has_session("dev-start-end"));
    bridge.end_session("dev-start-end").await;
    assert!(
      !bridge.has_session("dev-start-end"),
      "end_session must clear Starting so has_session cannot stick"
    );
  }

  #[tokio::test]
  async fn ended_while_starting_with_current_ring_clears() {
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None))));
    let ring = Arc::new(PcmRing::new(2, 64));
    {
      let mut devices = bridge.devices.lock();
      drop(devices.insert(
        "dev-start-ended".to_owned(),
        DeviceSlot {
          generation: 1,
          state: DeviceState::Starting { generation: 1 },
        },
      ));
    }
    // Ring still current would have been treated as "stale" before the fix; Starting always clears.
    let rings = FixedRingLookup { current: Some(Arc::clone(&ring)) };
    bridge.handle_ended("dev-start-ended", &rings).await;
    assert!(
      !bridge.has_session("dev-start-ended"),
      "Ended while Starting must clear even when ring is still current"
    );
  }

  /// Old LOAD finishes after replace cleared `late_stop_allowed` → STOP must not be issued.
  #[test]
  fn late_stop_suppressed_when_replace_disallows() {
    let session_alive = Arc::new(AtomicBool::new(true));
    let late_stop_allowed = Arc::new(AtomicBool::new(true));
    let stop_count = Arc::new(AtomicUsize::new(0));

    // Simulate an in-flight LOAD that will finish after replace teardown flips flags.
    let alive = Arc::clone(&session_alive);
    let late_stop = Arc::clone(&late_stop_allowed);
    let stops = Arc::clone(&stop_count);
    let gate = Arc::new(std::sync::Barrier::new(2));
    let gate_load = Arc::clone(&gate);
    let load = std::thread::spawn(move || {
      let _party = gate_load.wait();
      // Production load closure decision after cast_load_media returns:
      if late_load_should_stop(alive.load(Ordering::Acquire), late_stop.load(Ordering::Acquire)) {
        let _: usize = stops.fetch_add(1, Ordering::SeqCst);
      }
    });

    // Replace teardown order: late_stop_allowed=false FIRST, then session_alive=false.
    assert!(
      replace_skips_cast_stop(TeardownCastPolicy::SkipStopForReplace),
      "replace path is the one that must suppress late STOP"
    );
    late_stop_allowed.store(false, Ordering::Release);
    session_alive.store(false, Ordering::Release);
    let _party = gate.wait();
    load.join().expect("load thread");

    assert_eq!(
      stop_count.load(Ordering::SeqCst),
      0,
      "late STOP must be suppressed when replace disallows it"
    );
  }

  /// Load samples the intermediate replace store: `late_stop_allowed=false` while still alive.
  /// That pair must never STOP; only the reverse order creates a dangerous torn window.
  #[test]
  fn replace_teardown_intermediate_state_suppresses_stop() {
    let session_alive = Arc::new(AtomicBool::new(true));
    let late_stop_allowed = Arc::new(AtomicBool::new(true));
    let stop_count = Arc::new(AtomicUsize::new(0));
    let mid = Arc::new(std::sync::Barrier::new(2));
    let after = Arc::new(std::sync::Barrier::new(2));

    let alive = Arc::clone(&session_alive);
    let late_stop = Arc::clone(&late_stop_allowed);
    let stops = Arc::clone(&stop_count);
    let mid_load = Arc::clone(&mid);
    let after_load = Arc::clone(&after);
    let load = std::thread::spawn(move || {
      // Sample after first store only (late_stop cleared, alive still true).
      let _mid = mid_load.wait();
      if late_load_should_stop(alive.load(Ordering::Acquire), late_stop.load(Ordering::Acquire)) {
        let _: usize = stops.fetch_add(1, Ordering::SeqCst);
      }
      // Sample after second store (both false).
      let _after = after_load.wait();
      if late_load_should_stop(alive.load(Ordering::Acquire), late_stop.load(Ordering::Acquire)) {
        let _: usize = stops.fetch_add(1, Ordering::SeqCst);
      }
    });

    // Production replace order: late_stop first, then alive.
    late_stop_allowed.store(false, Ordering::Release);
    assert!(
      session_alive.load(Ordering::Acquire) && !late_stop_allowed.load(Ordering::Acquire),
      "intermediate replace state must be (alive, !late_stop)"
    );
    assert!(
      !late_load_should_stop(true, false),
      "intermediate (alive, !late_stop) must not STOP"
    );
    let _mid = mid.wait();
    session_alive.store(false, Ordering::Release);
    let _after = after.wait();
    load.join().expect("load thread");

    assert_eq!(
      stop_count.load(Ordering::SeqCst),
      0,
      "neither intermediate nor final replace state may issue late STOP"
    );
    // Document the inverted order that this store sequence forbids:
    assert!(
      late_load_should_stop(false, true),
      "torn (dead, late_stop_allowed) would STOP; replace order must never produce it"
    );
  }

  /// Full end leaves `late_stop_allowed` true so a late LOAD still STOPs (no revive).
  #[test]
  fn late_stop_issued_when_full_end_allows() {
    let session_alive = Arc::new(AtomicBool::new(true));
    let late_stop_allowed = Arc::new(AtomicBool::new(true));
    let stop_count = Arc::new(AtomicUsize::new(0));

    let alive = Arc::clone(&session_alive);
    let late_stop = Arc::clone(&late_stop_allowed);
    let stops = Arc::clone(&stop_count);
    let gate = Arc::new(std::sync::Barrier::new(2));
    let gate_load = Arc::clone(&gate);
    let load = std::thread::spawn(move || {
      let _party = gate_load.wait();
      if late_load_should_stop(alive.load(Ordering::Acquire), late_stop.load(Ordering::Acquire)) {
        let _: usize = stops.fetch_add(1, Ordering::SeqCst);
      }
    });

    // Full end: only session_alive flips; late_stop_allowed stays true.
    session_alive.store(false, Ordering::Release);
    assert!(!replace_skips_cast_stop(TeardownCastPolicy::StopBestEffort));
    let _party = gate.wait();
    load.join().expect("load thread");

    assert_eq!(stop_count.load(Ordering::SeqCst), 1, "full end must still STOP after late LOAD");
  }

  #[tokio::test]
  async fn replace_teardown_clears_late_stop_allowed() {
    let pool: Arc<dyn CastControl> = Arc::new(CastPool::new(None));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let session = test_playing_session(media, "dev-replace-stop", pool, Arc::new(PcmRing::new(2, 64)));
    let late_stop_allowed = Arc::clone(&session.late_stop_allowed);
    let session_alive = Arc::clone(&session.session_alive);
    assert!(late_stop_allowed.load(Ordering::Acquire));
    assert!(session_alive.load(Ordering::Acquire));

    teardown_playing(session, TeardownCastPolicy::SkipStopForReplace).await;

    assert!(!session_alive.load(Ordering::Acquire));
    assert!(
      !late_stop_allowed.load(Ordering::Acquire),
      "SkipStopForReplace must clear late_stop_allowed before joining inflight LOAD"
    );
  }
}
