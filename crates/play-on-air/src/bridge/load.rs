//! Cast LOAD helpers and generation-scoped re-LOAD coordination (single flight per session).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::cast::{CastPool, CastStreamKind, CastTrack, MediaLoadRequest};
use crate::error::{Error, Result};
use crate::media::RolloverSignal;

use super::Bridge;
use super::state::{DeviceState, EgressKind};

/// How long teardown waits for an in-flight blocking Cast LOAD.
///
/// Cast pool `COMMAND_TIMEOUT` is 6 s; keep a small margin so a slow LOAD is joined
/// instead of detached, without re-introducing the old 30 s HOL risk.
pub(super) const INFLIGHT_LOAD_JOIN_TIMEOUT: Duration = Duration::from_secs(8);

/// How Cast volume is applied after a progressive media LOAD.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum LoadVolumePolicy {
  /// Initial session start: leave the Cast device volume unchanged.
  ///
  /// Forcing 1.0 overwrote the speaker's current level every stream start.
  PreserveDevice,
  /// Content-Length rollover re-LOAD: re-apply last AirPlay linear volume, or skip if unknown.
  Rollover { last_volume: Option<f32> },
}

/// Resolve the linear volume to set after LOAD, if any.
pub(super) const fn volume_after_load(policy: LoadVolumePolicy) -> Option<f32> {
  match policy {
    LoadVolumePolicy::PreserveDevice => None,
    LoadVolumePolicy::Rollover { last_volume } => last_volume,
  }
}

/// Whether a late-completing Cast LOAD should issue STOP because the session died.
///
/// Replace teardown stores `late_stop_allowed = false` **before** `session_alive = false` so a
/// concurrent load never observes `(dead, late_stop_allowed)` (serialized pool:
/// Load(old) → Load(new) → Stop would kill the replacement session). Intermediate
/// `(alive, !late_stop_allowed)` is safe: this returns false.
pub(super) const fn late_load_should_stop(alive: bool, late_stop_allowed: bool) -> bool {
  !alive && late_stop_allowed
}

/// Whether discarding a wire-successful re-LOAD requires Cast STOP (B1-shaped orphan).
///
/// True when the client paused (session still alive but bridge will not accept media) or when
/// full end allows late STOP. False on replace (`!late_stop_allowed`) so we never STOP the
/// superseding session.
#[must_use]
const fn discard_successful_reload_should_stop(paused: bool, alive: bool, late_stop_allowed: bool) -> bool {
  paused || late_load_should_stop(alive, late_stop_allowed)
}

/// Whether a detached re-LOAD may still run for this Playing generation.
///
/// Abort when generation moved on, the session died, or the client paused (B2:
/// `resume_play_failed` re-LOAD must not complete after a later pause).
#[must_use]
const fn reload_still_valid(live_gen: u64, expected_gen: u64, alive: bool, paused: bool) -> bool {
  live_gen == expected_gen && alive && !paused
}

/// Cast LOAD helper (shared by initial start, rollover, flush, stall, resume).
pub(super) fn cast_load_media(
  pool: &CastPool,
  device_id: &str,
  request: MediaLoadRequest,
  volume: LoadVolumePolicy,
) -> Result<crate::cast::MediaSessionRef> {
  let session = pool.load(device_id, request)?;
  if let Some(level) = volume_after_load(volume)
    && let Err(err) = pool.set_volume(device_id, level)
  {
    tracing::debug!(error = %err, "post-load Cast volume set failed");
  }
  Ok(session)
}

/// Abort + join any prior in-flight LOAD so a new publish cannot race a detached handle.
async fn join_prior_inflight_load(inflight_load: &Mutex<Option<tokio::task::JoinHandle<()>>>, device_id: &str) {
  let prior = inflight_load.lock().take();
  let Some(handle) = prior else {
    return;
  };
  handle.abort();
  match tokio::time::timeout(INFLIGHT_LOAD_JOIN_TIMEOUT, handle).await {
    Ok(Ok(())) => {},
    Ok(Err(err)) if err.is_cancelled() => {},
    Ok(Err(err)) => {
      tracing::warn!(%device_id, error = %err, "prior in-flight Cast LOAD join failed");
    },
    Err(_) => {
      tracing::warn!(%device_id, "prior in-flight Cast LOAD join timed out");
    },
  }
}

/// Spawn a task that re-LOADs the same stream URL each time `LiveWav` hits its body cap.
#[expect(
  clippy::too_many_arguments,
  reason = "rollover loop needs session liveness, volume, inflight load, and reload flight"
)]
pub(super) fn spawn_rollover_reload_loop(
  device_id: String,
  stream_url: String,
  track: Arc<Mutex<CastTrack>>,
  pool: Arc<CastPool>,
  rollover: Arc<RolloverSignal>,
  session_alive: Arc<AtomicBool>,
  late_stop_allowed: Arc<AtomicBool>,
  last_volume_linear: Arc<Mutex<Option<f32>>>,
  inflight_load: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
  reload_flight: Arc<tokio::sync::Mutex<()>>,
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
  let (cancel_tx, mut cancel_rx) = oneshot::channel::<()>();
  let task = tokio::spawn(async move {
    let mut seen = 0_u64;
    loop {
      tokio::select! {
        _ = &mut cancel_rx => {
          tracing::debug!(%device_id, "LiveWav rollover re-LOAD loop cancelled");
          break;
        },
        count = rollover.wait_past(seen) => {
          seen = count;
          if !session_alive.load(Ordering::Acquire) {
            tracing::debug!(%device_id, "skipping LiveWav rollover re-LOAD; session ended");
            break;
          }
          tracing::info!(%device_id, rollover = seen, %stream_url, "LiveWav rollover: re-LOADing Cast stream");
          let load_pool = Arc::clone(&pool);
          let id = device_id.clone();
          let url = stream_url.clone();
          let load_track = track.lock().clone();
          let last_volume = *last_volume_linear.lock();
          let alive = Arc::clone(&session_alive);
          let late_stop = Arc::clone(&late_stop_allowed);
          // Share reload_flight with reload_playing_media so concurrent re-LOAD entry
          // points cannot overwrite inflight_load without joining (detached LOAD).
          let flight = reload_flight.lock().await;
          // Single-flight: finish any prior LOAD before publishing (no detached race).
          join_prior_inflight_load(&inflight_load, &device_id).await;
          if !session_alive.load(Ordering::Acquire) {
            tracing::debug!(%device_id, "skipping LiveWav rollover re-LOAD; session ended after prior join");
            break;
          }
          // Result channel lets this loop observe LOAD while `inflight_load` stays joinable by teardown.
          let (result_tx, result_rx) = oneshot::channel();
          // Publish under the same lock as the alive re-check so teardown cannot
          // take `None` in a spawn→store gap while LOAD keeps running detached.
          {
            let mut slot = inflight_load.lock();
            if !session_alive.load(Ordering::Acquire) {
              tracing::debug!(%device_id, "skipping LiveWav rollover re-LOAD; session ended before spawn");
              break;
            }
            let load_task = tokio::task::spawn_blocking(move || {
              let result = cast_load_media(
                &load_pool,
                &id,
                MediaLoadRequest::wav(url, CastStreamKind::Buffered).with_track(load_track),
                LoadVolumePolicy::Rollover { last_volume },
              );
              // Late LOAD after full end: STOP so playback cannot revive against a dead HTTP server.
              // Replace path clears late_stop_allowed so STOP cannot kill the new generation.
              if late_load_should_stop(alive.load(Ordering::Acquire), late_stop.load(Ordering::Acquire)) {
                let _ok = load_pool.stop_best_effort(&id, Duration::from_secs(2));
              }
              let _sent = result_tx.send(result);
            });
            *slot = Some(load_task);
          }
          let load_result = result_rx.await;
          // Clear our slot if teardown has not already taken the JoinHandle.
          // Drop the parking_lot guard before await (guard is !Send).
          let pending_join = inflight_load.lock().take();
          if let Some(join_handle) = pending_join {
            match join_handle.await {
              Ok(()) => {},
              Err(err) if err.is_cancelled() => {},
              Err(err) => {
                tracing::warn!(%device_id, error = %err, "LiveWav rollover re-LOAD task join failed");
              },
            }
          }
          // Drop flight before the next select wait so concurrent reload_playing_media can run.
          drop(flight);
          match load_result {
            Ok(Ok(session)) => {
              if session_alive.load(Ordering::Acquire) {
                tracing::info!(
                  %device_id,
                  transport_id = %session.transport_id,
                  media_session_id = session.media_session_id,
                  "LiveWav rollover Cast re-LOAD ok"
                );
              }
            },
            Ok(Err(err)) => {
              tracing::warn!(%device_id, error = %err, "LiveWav rollover Cast re-LOAD failed");
            },
            Err(_recv) => {
              // Teardown aborted this task or dropped the sender path; join handled above / by teardown.
              tracing::debug!(%device_id, "LiveWav rollover re-LOAD result dropped (session teardown)");
              break;
            },
          }
          if !session_alive.load(Ordering::Acquire) {
            break;
          }
        },
      }
    }
  });
  (cancel_tx, task)
}

/// Outcome of a re-LOAD validity gate (generation, liveness, pause, flush coverage).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReloadGate {
  /// Generation moved on, the session ended, or the client paused.
  Abort,
  /// The flush this re-LOAD was for is already covered by a LOAD accepted meanwhile.
  AlreadySatisfied,
  /// Proceed; a LOAD published now covers flush requests up to `flush_requested`.
  Proceed { flush_requested: u64 },
}

impl Bridge {
  /// Spawn a generation-scoped re-LOAD on the session task set (gen checks inside reload).
  pub(super) fn spawn_reload_playing_media(
    self: &Arc<Self>,
    device_id: &str,
    generation: u64,
    reason: &'static str,
    flush_request: Option<u64>,
  ) {
    let task = self.reload_task(device_id, generation, reason, flush_request);
    self.spawn_session_task(device_id, generation, task);
  }

  /// Future that runs [`Bridge::reload_playing_media`] (for spawning on a session task set).
  pub(super) fn reload_task(
    self: &Arc<Self>,
    device_id: &str,
    generation: u64,
    reason: &'static str,
    flush_request: Option<u64>,
  ) -> impl Future<Output = ()> + Send + use<> {
    let bridge = Arc::clone(self);
    let id = device_id.to_owned();
    async move {
      let _ok = bridge.reload_playing_media(&id, generation, reason, flush_request).await;
    }
  }

  /// Re-LOAD the current Playing media URL. Returns whether LOAD succeeded while still live.
  ///
  /// `flush_request` marks a re-LOAD owed to a fired flush: after waiting out any in-flight
  /// re-LOAD it is re-evaluated and skipped (returning `true`) when a LOAD published after that
  /// flush was already accepted. An accepted LOAD records the flush requests it covers, so an
  /// abandoned flush (pause, failure) stays owed for the next resume.
  #[expect(
    clippy::too_many_lines,
    reason = "flight serialize + multi-stage pause/gen validity gates must stay ordered (B2)"
  )]
  pub(super) async fn reload_playing_media(
    &self,
    device_id: &str,
    generation: u64,
    reason: &str,
    flush_request: Option<u64>,
  ) -> bool {
    let load_plan = {
      let devices = self.devices.lock();
      let Some(slot) = devices.get(device_id) else {
        return false;
      };
      let DeviceState::Playing { generation: live_gen, session } = &slot.state else {
        return false;
      };
      if !reload_still_valid(
        *live_gen,
        generation,
        session.session_alive.load(Ordering::Acquire),
        session.paused,
      ) {
        return false;
      }
      let request = match session.egress {
        EgressKind::FlacLive => MediaLoadRequest::flac(session.stream_url.clone(), CastStreamKind::Live),
        EgressKind::WavBuffered => MediaLoadRequest::wav(session.stream_url.clone(), CastStreamKind::Buffered),
      }
      .with_track(session.track.lock().clone());
      let last_volume = *session.last_volume_linear.lock();
      let plan = (
        Arc::clone(&session.pool),
        request,
        LoadVolumePolicy::Rollover { last_volume },
        Arc::clone(&session.session_alive),
        Arc::clone(&session.late_stop_allowed),
        Arc::clone(&session.inflight_load),
        Arc::clone(&session.reload_flight),
        Arc::clone(&session.playback_paused),
      );
      drop(devices);
      Some(plan)
    };
    let Some((pool, request, volume, alive, late_stop_allowed, inflight_load, reload_flight, playback_paused)) =
      load_plan
    else {
      return false;
    };

    // Serialize the whole join → spawn → store → await → take region so concurrent
    // callers (flush spawn + resume spawn + stall + rollover) cannot both pass
    // join_prior and both publish — second overwrite would detach a LOAD.
    let _flight = reload_flight.lock().await;

    // Re-check after acquiring the flight lock (pause/end can race while waiting, and the
    // re-LOAD we waited on may already cover this flush).
    match self.reload_gate(device_id, generation, flush_request) {
      ReloadGate::Abort => {
        tracing::debug!(%device_id, generation, reason, "re-LOAD aborted after flight lock (paused or stale)");
        return false;
      },
      ReloadGate::AlreadySatisfied => {
        tracing::debug!(%device_id, generation, reason, "flush re-LOAD already covered by a later LOAD");
        return true;
      },
      ReloadGate::Proceed { .. } => {},
    }

    // Single-flight: finish any prior LOAD before publishing a new one (no detached race).
    join_prior_inflight_load(&inflight_load, device_id).await;

    // State can change while joining the prior load (B2: pause during wait). The flush count
    // read here is what this LOAD covers once accepted (it starts after those flushes).
    let covers_flush = match self.reload_gate(device_id, generation, flush_request) {
      ReloadGate::Abort => {
        tracing::debug!(%device_id, generation, reason, "re-LOAD aborted after prior join (paused or stale)");
        return false;
      },
      ReloadGate::AlreadySatisfied => return true,
      ReloadGate::Proceed { flush_requested } => flush_requested,
    };

    let id = device_id.to_owned();
    // Kept for post-LOAD discard STOP (pause after Ok is B1-shaped orphan without this).
    let pool_for_discard = Arc::clone(&pool);
    let alive_for_discard = Arc::clone(&alive);
    let late_stop_for_discard = Arc::clone(&late_stop_allowed);
    let paused_for_discard = Arc::clone(&playback_paused);
    let (result_tx, result_rx) = oneshot::channel();
    {
      let mut slot = inflight_load.lock();
      if !alive.load(Ordering::Acquire) || playback_paused.load(Ordering::Acquire) {
        return false;
      }
      let paused_flag = Arc::clone(&playback_paused);
      let load_task = tokio::task::spawn_blocking(move || {
        // Last check immediately before the blocking pool LOAD (B2).
        if paused_flag.load(Ordering::Acquire) || !alive.load(Ordering::Acquire) {
          let _sent = result_tx.send(Err(Error::Bridge("re-LOAD aborted: paused or session ended".to_owned())));
          return;
        }
        let result = cast_load_media(&pool, &id, request, volume);
        // If pause landed during LOAD, treat success as late and STOP (B1/B2).
        if paused_flag.load(Ordering::Acquire) {
          let _ok = pool.stop_best_effort(&id, Duration::from_secs(2));
          let _sent = result_tx.send(Err(Error::Bridge("re-LOAD aborted: paused during LOAD".to_owned())));
          return;
        }
        // SkipStopForReplace means a replacement start owns the device; late STOP after old
        // LOAD would kill the new session on the serialized pool.
        if late_load_should_stop(alive.load(Ordering::Acquire), late_stop_allowed.load(Ordering::Acquire)) {
          let _ok = pool.stop_best_effort(&id, Duration::from_secs(2));
        }
        let _sent = result_tx.send(result);
      });
      *slot = Some(load_task);
    }

    let load_result = result_rx.await;
    let pending_join = inflight_load.lock().take();
    if let Some(join_handle) = pending_join {
      match join_handle.await {
        Ok(()) => {},
        Err(err) if err.is_cancelled() => {},
        Err(err) => {
          tracing::warn!(%device_id, generation, error = %err, "re-LOAD task join failed");
        },
      }
    }

    match load_result {
      Ok(Ok(session)) => {
        // Final gate: do not accept success if pause/gen/alive won the race after LOAD returned.
        if self.accept_reload(device_id, generation, covers_flush) {
          tracing::info!(
            %device_id,
            generation,
            reason,
            transport_id = %session.transport_id,
            media_session_id = session.media_session_id,
            "Cast re-LOAD ok"
          );
          return true;
        }
        // Wire success was not accepted — STOP unless replace forbids it (B1 orphan otherwise).
        let paused = paused_for_discard.load(Ordering::Acquire);
        let alive_now = alive_for_discard.load(Ordering::Acquire);
        let late_stop = late_stop_for_discard.load(Ordering::Acquire);
        if discard_successful_reload_should_stop(paused, alive_now, late_stop) {
          tracing::debug!(
            %device_id,
            generation,
            reason,
            paused,
            "Cast re-LOAD ok but discarded; stopping unacknowledged session"
          );
          let stop_id = device_id.to_owned();
          match tokio::task::spawn_blocking(move || {
            let _ok = pool_for_discard.stop_best_effort(&stop_id, Duration::from_secs(2));
          })
          .await
          {
            Ok(()) => {},
            Err(err) => {
              tracing::warn!(%device_id, generation, error = %err, "discard STOP task join failed");
            },
          }
        } else {
          tracing::debug!(
            %device_id,
            generation,
            reason,
            "Cast re-LOAD ok but session stale (replace); discarding without STOP"
          );
        }
      },
      Ok(Err(err)) => {
        tracing::warn!(%device_id, generation, reason, error = %err, "Cast re-LOAD failed");
      },
      Err(_) => {
        tracing::debug!(%device_id, generation, reason, "Cast re-LOAD result dropped (session teardown)");
      },
    }
    false
  }

  /// Validity gate for a re-LOAD of `generation` (see [`ReloadGate`]).
  pub(super) fn reload_gate(&self, device_id: &str, generation: u64, flush_request: Option<u64>) -> ReloadGate {
    let devices = self.devices.lock();
    let gate = match devices.get(device_id).map(|slot| &slot.state) {
      Some(DeviceState::Playing { generation: live_gen, session }) => {
        if !reload_still_valid(
          *live_gen,
          generation,
          session.session_alive.load(Ordering::Acquire),
          session.paused,
        ) {
          ReloadGate::Abort
        } else if flush_request.is_some_and(|request| session.flush_reload_done >= request) {
          ReloadGate::AlreadySatisfied
        } else {
          ReloadGate::Proceed {
            flush_requested: session.flush_reload_requested,
          }
        }
      },
      Some(DeviceState::Idle | DeviceState::Starting { .. }) | None => ReloadGate::Abort,
    };
    drop(devices);
    gate
  }

  /// Accept a wire-successful re-LOAD if the generation is still live, alive, and unpaused;
  /// record the flush requests it covers.
  fn accept_reload(&self, device_id: &str, generation: u64, covers_flush: u64) -> bool {
    let mut devices = self.devices.lock();
    let accepted = match devices.get_mut(device_id).map(|slot| &mut slot.state) {
      Some(DeviceState::Playing { generation: live_gen, session })
        if reload_still_valid(
          *live_gen,
          generation,
          session.session_alive.load(Ordering::Acquire),
          session.paused,
        ) =>
      {
        session.flush_reload_done = session.flush_reload_done.max(covers_flush);
        true
      },
      Some(_) | None => false,
    };
    drop(devices);
    accepted
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::atomic::AtomicUsize;

  use tokio::time::sleep;

  use crate::audio::PcmRing;
  use crate::bridge::test_support::{insert_playing, test_playing_session};
  use crate::media::MediaServer;
  use crate::registry::DeviceRegistry;

  #[derive(Default)]
  struct ReloadFlightCounters {
    spawned: AtomicUsize,
    joined: AtomicUsize,
    in_region: AtomicUsize,
    max_in_region: AtomicUsize,
  }

  /// Mirrors production join → publish → await → take under `reload_flight`.
  async fn simulated_reload_flight_region(
    reload_flight: Arc<tokio::sync::Mutex<()>>,
    inflight_load: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    counters: Arc<ReloadFlightCounters>,
    delay: Duration,
  ) {
    let flight = reload_flight.lock().await;
    let cur = counters.in_region.fetch_add(1, Ordering::SeqCst) + 1;
    let _: usize = counters.max_in_region.fetch_max(cur, Ordering::SeqCst);

    join_prior_inflight_load(&inflight_load, "flight-test").await;

    let (result_tx, result_rx) = oneshot::channel::<()>();
    {
      let mut slot = inflight_load.lock();
      let load_task = tokio::spawn(async move {
        sleep(delay).await;
        let _sent = result_tx.send(());
      });
      let _: usize = counters.spawned.fetch_add(1, Ordering::SeqCst);
      *slot = Some(load_task);
    }

    match result_rx.await {
      Ok(()) | Err(_) => {},
    }
    let pending_join = inflight_load.lock().take();
    if let Some(join_handle) = pending_join {
      match join_handle.await {
        Ok(()) | Err(_) => {
          let _: usize = counters.joined.fetch_add(1, Ordering::SeqCst);
        },
      }
    }
    let _: usize = counters.in_region.fetch_sub(1, Ordering::SeqCst);
    drop(flight);
  }

  #[test]
  fn volume_after_load_preserve_device_and_rollover() {
    assert_eq!(volume_after_load(LoadVolumePolicy::PreserveDevice), None);
    assert_eq!(volume_after_load(LoadVolumePolicy::Rollover { last_volume: None }), None);
    assert_eq!(
      volume_after_load(LoadVolumePolicy::Rollover { last_volume: Some(0.42) }),
      Some(0.42)
    );
  }

  #[test]
  fn inflight_load_join_timeout_covers_cast_command_timeout() {
    // Cast pool COMMAND_TIMEOUT is 6s; teardown joins with a small margin.
    assert!(
      INFLIGHT_LOAD_JOIN_TIMEOUT >= Duration::from_secs(6),
      "INFLIGHT_LOAD_JOIN_TIMEOUT={INFLIGHT_LOAD_JOIN_TIMEOUT:?} must be >= 6s (Cast cmd)"
    );
    assert!(
      INFLIGHT_LOAD_JOIN_TIMEOUT <= Duration::from_secs(12),
      "INFLIGHT_LOAD_JOIN_TIMEOUT={INFLIGHT_LOAD_JOIN_TIMEOUT:?} must stay near pool budget (not 30s era)"
    );
  }

  /// Concurrent join → publish → await → take regions under `reload_flight` never orphan a handle.
  ///
  /// Production `reload_playing_media` and the `LiveWav` rollover loop share
  /// `PlayingSession::reload_flight` for this same critical section (flush spawn + resume
  /// spawn + stall can otherwise both pass `join_prior` and overwrite without abort).
  #[tokio::test]
  async fn reload_flight_serializes_concurrent_publish_without_orphan() {
    let reload_flight = Arc::new(tokio::sync::Mutex::new(()));
    let inflight_load = Arc::new(Mutex::new(None));
    let counters = Arc::new(ReloadFlightCounters::default());
    let delay = Duration::from_millis(40);
    let mut handles = Vec::new();
    for _ in 0..3 {
      handles.push(tokio::spawn(simulated_reload_flight_region(
        Arc::clone(&reload_flight),
        Arc::clone(&inflight_load),
        Arc::clone(&counters),
        delay,
      )));
    }
    for handle in handles {
      handle.await.expect("region task");
    }

    assert_eq!(
      counters.max_in_region.load(Ordering::SeqCst),
      1,
      "reload_flight must admit only one join/publish/await/take region at a time"
    );
    assert_eq!(
      counters.spawned.load(Ordering::SeqCst),
      counters.joined.load(Ordering::SeqCst),
      "every published inflight handle must be joined (no detached overwrite)"
    );
    assert!(
      inflight_load.lock().is_none(),
      "no leftover inflight handle after concurrent reloads"
    );
  }

  #[tokio::test]
  async fn rollover_signal_invokes_reload_path_without_panic() {
    let pool = Arc::new(CastPool::new(None));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let rollover = media.rollover_signal();
    let rollover_for_task = Arc::clone(&rollover);
    let pool_task = Arc::clone(&pool);
    let task = tokio::spawn(async move {
      let count = rollover_for_task.wait_past(0).await;
      assert!(count > 0);
      let load_pool = Arc::clone(&pool_task);
      let result = tokio::task::spawn_blocking(move || {
        cast_load_media(
          &load_pool,
          "missing-device",
          MediaLoadRequest::wav("http://127.0.0.1:9/stream".to_owned(), CastStreamKind::Buffered),
          LoadVolumePolicy::Rollover { last_volume: None },
        )
      })
      .await;
      // No worker → load errors; must not panic. Goes through pool.load (stamps last_load).
      assert!(matches!(result, Ok(Err(_))));
    });

    rollover.signal();
    let finished = tokio::time::timeout(Duration::from_secs(3), task).await;
    assert!(finished.is_ok(), "rollover re-LOAD path must complete");
    media.shutdown();
  }

  #[test]
  fn media_load_request_default_flac_is_live() {
    let req = MediaLoadRequest::flac("http://127.0.0.1/stream", CastStreamKind::Live);
    assert_eq!(req.content_type, "audio/flac");
    assert_eq!(req.stream_kind, CastStreamKind::Live);
  }

  #[test]
  fn late_load_should_stop_only_when_dead_and_allowed() {
    assert!(!late_load_should_stop(true, true));
    assert!(!late_load_should_stop(true, false));
    assert!(late_load_should_stop(false, true));
    assert!(!late_load_should_stop(false, false));
  }

  #[test]
  fn reload_still_valid_requires_gen_alive_and_not_paused() {
    assert!(reload_still_valid(1, 1, true, false));
    assert!(!reload_still_valid(1, 1, true, true), "paused must abort re-LOAD");
    assert!(!reload_still_valid(1, 1, false, false), "dead session must abort");
    assert!(!reload_still_valid(2, 1, true, false), "generation mismatch must abort");
  }

  /// Post-Ok discard of a successful re-LOAD must STOP on pause / full end, never on replace.
  #[test]
  fn discard_successful_reload_stop_policy() {
    // Pause after LOAD returned Ok while session still alive → STOP (B1 orphan otherwise).
    assert!(discard_successful_reload_should_stop(true, true, true));
    assert!(discard_successful_reload_should_stop(true, true, false));
    // Full end: dead + late_stop_allowed → STOP.
    assert!(discard_successful_reload_should_stop(false, false, true));
    // Replace: dead + !late_stop_allowed → do not STOP the superseding session.
    assert!(!discard_successful_reload_should_stop(false, false, false));
    // Accepted path would not discard; still no STOP when valid-shaped flags.
    assert!(!discard_successful_reload_should_stop(false, true, true));
  }

  #[tokio::test]
  async fn pause_invalidates_in_flight_reload_plan() {
    let pool = Arc::new(CastPool::new(None));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let ring = Arc::new(PcmRing::new(2, 4_096));
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool)));
    let session = test_playing_session(media, "dev-b2", Arc::clone(&pool), ring);
    let playback_paused = Arc::clone(&session.playback_paused);
    insert_playing(&bridge, "dev-b2", 1, session);

    assert!(matches!(bridge.reload_gate("dev-b2", 1, None), ReloadGate::Proceed { .. }));

    // Simulate handle_pause marking paused without awaiting Cast.
    {
      let mut devices = bridge.devices.lock();
      let DeviceState::Playing { session: playing, .. } = &mut devices.get_mut("dev-b2").unwrap().state else {
        panic!("expected Playing");
      };
      playing.paused = true;
      playing.playback_paused.store(true, Ordering::Release);
      drop(devices);
    }
    assert!(playback_paused.load(Ordering::Acquire));
    assert_eq!(
      bridge.reload_gate("dev-b2", 1, None),
      ReloadGate::Abort,
      "re-LOAD must abort once paused"
    );
    assert!(!bridge.reload_playing_media("dev-b2", 1, "resume_play_failed", None).await);
  }

  /// A flush re-LOAD queued behind an in-flight re-LOAD re-evaluates once the flight lock frees:
  /// it skips when a LOAD covering the flush was accepted meanwhile, and proceeds otherwise.
  #[tokio::test]
  async fn queued_flush_reload_reevaluates_after_in_flight_reload() {
    let pool = Arc::new(CastPool::new(None));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool)));
    let mut playing = test_playing_session(media, "dev-flush-q", Arc::clone(&pool), Arc::new(PcmRing::new(2, 64)));
    playing.flush_reload_requested = 1;
    let reload_flight = Arc::clone(&playing.reload_flight);
    insert_playing(&bridge, "dev-flush-q", 1, playing);

    // In-flight re-LOAD owns the flight lock while the flush timer's re-LOAD queues behind it.
    let in_flight = reload_flight.lock().await;
    let queued = tokio::spawn({
      let bridge_for_task = Arc::clone(&bridge);
      async move { bridge_for_task.reload_playing_media("dev-flush-q", 1, "flush", Some(1)).await }
    });
    sleep(Duration::from_millis(20)).await;
    assert!(!queued.is_finished(), "flush re-LOAD must wait for the in-flight re-LOAD");
    // The in-flight LOAD was published after this flush and got accepted.
    assert!(bridge.accept_reload("dev-flush-q", 1, 1));
    drop(in_flight);
    let skipped = tokio::time::timeout(Duration::from_secs(2), queued)
      .await
      .expect("queued re-LOAD finishes")
      .expect("join");
    assert!(skipped, "covered flush re-LOAD must be skipped as satisfied (no second LOAD)");

    // A flush requested after that LOAD is not covered: it proceeds to a real LOAD (fails here
    // without a Cast worker) and stays owed.
    {
      let mut devices = bridge.devices.lock();
      let DeviceState::Playing { session, .. } = &mut devices.get_mut("dev-flush-q").unwrap().state else {
        panic!("expected Playing");
      };
      session.flush_reload_requested = 2;
      drop(devices);
    }
    assert!(
      !bridge.reload_playing_media("dev-flush-q", 1, "flush", Some(2)).await,
      "uncovered flush must attempt the LOAD"
    );
    {
      let devices = bridge.devices.lock();
      let DeviceState::Playing { session, .. } = &devices.get("dev-flush-q").unwrap().state else {
        panic!("expected Playing");
      };
      assert!(session.flush_reload_owed(), "failed flush re-LOAD keeps the request owed");
      drop(devices);
    }
    bridge.handle_session_end("dev-flush-q").await;
  }
}
