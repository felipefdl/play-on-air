//! Per-device session state: generation-stamped slots and the live `Playing` session.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio::task::{AbortHandle, JoinError, JoinSet};

use crate::audio::PcmRing;
use crate::cast::CastPool;
use crate::media::MediaServerHandle;

use super::fallback::FlacStrikeReason;

/// How long teardown waits for aborted session-scoped tasks to unwind.
const SESSION_TASK_JOIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Cast egress selected for a bridge session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EgressKind {
  /// Chunked `audio/flac` with Cast `streamType` LIVE.
  FlacLive,
  /// Progressive WAV with Content-Length and Cast `streamType` BUFFERED.
  WavBuffered,
}

/// Choose Cast egress for `device_id` given process-lifetime FLAC rejection memory.
pub(super) const fn select_egress(device_remembered_wav: bool) -> EgressKind {
  if device_remembered_wav {
    EgressKind::WavBuffered
  } else {
    EgressKind::FlacLive
  }
}

/// Live media + Cast control state for a `Playing` generation.
pub(super) struct PlayingSession {
  pub(super) media: MediaServerHandle,
  pub(super) device_id: String,
  pub(super) pool: Arc<CastPool>,
  /// PCM ring feeding this session's media body.
  pub(super) ring: Arc<PcmRing>,
  pub(super) stream_url: String,
  pub(super) cast_name: String,
  pub(super) channels: u16,
  pub(super) sample_rate: u32,
  pub(super) egress: EgressKind,
  /// AirPlay reported paused (local; stall watchdog honors this immediately).
  ///
  /// Cast PAUSE is deferred by `REANCHOR_DEBOUNCE` so iOS re-anchors can cancel.
  pub(super) paused: bool,
  /// Cross-thread mirror of [`Self::paused`] for in-flight re-LOAD abort checks (B2).
  pub(super) playback_paused: Arc<AtomicBool>,
  /// When the current pause began (for long-pause re-LOAD).
  pub(super) paused_at: Option<Instant>,
  /// Flush re-LOADs requested so far (bumped when a flush debounce timer fires).
  pub(super) flush_reload_requested: u64,
  /// Highest [`Self::flush_reload_requested`] value covered by an accepted re-LOAD.
  ///
  /// A request stays owed while `done < requested`: a pause (or a competing re-LOAD that
  /// started before the flush) must not drop it, or Cast replays the pre-flush buffer tail.
  pub(super) flush_reload_done: u64,
  /// Last stall-driven re-LOAD (repeat → kick).
  pub(super) last_stall_reload_at: Option<Instant>,
  /// When this Playing generation became active (FLAC early-fallback window).
  pub(super) started_at: Instant,
  /// Last FLAC failure strike for this Playing generation (pair-window late fallback).
  pub(super) flac_strike: Option<(Instant, FlacStrikeReason)>,
  /// Arm id for pause debounce; timer callbacks must match to fire Cast PAUSE.
  pub(super) pause_debounce_arm: u64,
  /// Pending pause debounce timer (task in [`Self::tasks`]; abort cancels the fire).
  pub(super) pause_debounce_cancel: Option<AbortHandle>,
  /// Arm id for flush debounce; timer callbacks must match to fire re-LOAD.
  pub(super) flush_debounce_arm: u64,
  /// Pending flush debounce timer (task in [`Self::tasks`]; abort cancels the fire).
  pub(super) flush_debounce_cancel: Option<AbortHandle>,
  /// Detached Cast PAUSE/PLAY, re-LOADs, and debounce timers owned by this generation.
  pub(super) tasks: SessionTasks,
  /// Drop / send to stop the `LiveWav` Content-Length rollover re-LOAD loop.
  pub(super) rollover_cancel: Option<oneshot::Sender<()>>,
  pub(super) rollover_task: Option<tokio::task::JoinHandle<()>>,
  /// Cancel the stall watchdog task.
  pub(super) watchdog_cancel: Option<oneshot::Sender<()>>,
  pub(super) watchdog_task: Option<tokio::task::JoinHandle<()>>,
  /// Cleared on teardown so late LOAD paths STOP instead of reviving playback
  /// (when [`Self::late_stop_allowed`] is still true). On replace, cleared **after**
  /// `late_stop_allowed` so the pair is never observed as `(dead, late_stop_allowed)`.
  pub(super) session_alive: Arc<AtomicBool>,
  /// Replace path sets this false **before** clearing [`Self::session_alive`] so a
  /// late-completing old LOAD does not STOP the new session.
  ///
  /// Full end leaves this true: after `session_alive` is false, late LOAD still STOPs so
  /// playback cannot revive against a dead media server.
  pub(super) late_stop_allowed: Arc<AtomicBool>,
  /// Last AirPlay volume as Cast linear level (updated by volume events).
  pub(super) last_volume_linear: Arc<Mutex<Option<f32>>>,
  /// In-flight blocking Cast LOAD (rollover / reload); awaited on teardown before media STOP.
  pub(super) inflight_load: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
  /// Serializes join → publish → await → take of `inflight_load` across concurrent re-LOAD
  /// entry points (flush spawn, resume spawn, stall, rollover) so a second caller cannot
  /// overwrite a `JoinHandle` without aborting it (detached LOAD).
  pub(super) reload_flight: Arc<tokio::sync::Mutex<()>>,
}

/// Per-device generation-stamped session state.
#[derive(Default)]
pub(super) enum DeviceState {
  #[default]
  Idle,
  /// In-flight session start (prebuffer / Cast LOAD). Ring identity is only on the event path.
  Starting { generation: u64 },
  Playing {
    generation: u64,
    session: Box<PlayingSession>,
  },
}

/// Monotonic generation counter + current state for one device.
pub(super) struct DeviceSlot {
  /// Last generation stamped when a session start was accepted (0 = never).
  pub(super) generation: u64,
  pub(super) state: DeviceState,
}

impl Default for DeviceSlot {
  fn default() -> Self {
    Self { generation: 0, state: DeviceState::Idle }
  }
}

impl DeviceSlot {
  pub(super) const fn is_live(&self) -> bool {
    matches!(self.state, DeviceState::Starting { .. } | DeviceState::Playing { .. })
  }

  pub(super) const fn live_gen(&self) -> Option<u64> {
    match self.state {
      DeviceState::Starting { generation } | DeviceState::Playing { generation, .. } => Some(generation),
      DeviceState::Idle => None,
    }
  }
}

impl PlayingSession {
  /// Whether a fired flush still needs a re-LOAD that has not landed yet.
  pub(super) const fn flush_reload_owed(&self) -> bool {
    self.flush_reload_requested > self.flush_reload_done
  }
}

/// Session-scoped detached work: Cast PAUSE/PLAY, re-LOADs, and re-anchor debounce timers.
///
/// Owned by [`PlayingSession`] so replace and full-end teardown abort and join every task;
/// nothing spawned for a generation can touch Cast after that generation is gone.
#[derive(Default)]
pub(super) struct SessionTasks {
  set: JoinSet<()>,
}

impl SessionTasks {
  /// Spawn `task` for this session, reaping already finished tasks first.
  pub(super) fn spawn<F>(&mut self, device_id: &str, task: F) -> AbortHandle
  where
    F: Future<Output = ()> + Send + 'static,
  {
    while let Some(result) = self.set.try_join_next() {
      note_task_exit(device_id, result);
    }
    self.set.spawn(task)
  }

  /// Abort every task and wait (bounded) for them to unwind.
  pub(super) async fn abort_and_join(mut self, device_id: &str) {
    self.set.abort_all();
    let drained = tokio::time::timeout(SESSION_TASK_JOIN_TIMEOUT, async {
      while let Some(result) = self.set.join_next().await {
        note_task_exit(device_id, result);
      }
    })
    .await;
    if drained.is_err() {
      tracing::warn!(%device_id, "session task join timed out after abort");
    }
  }
}

fn note_task_exit(device_id: &str, result: Result<(), JoinError>) {
  if let Err(err) = result
    && !err.is_cancelled()
  {
    tracing::warn!(%device_id, error = %err, "session task panicked");
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::bridge::Bridge;
  use crate::registry::DeviceRegistry;

  #[test]
  fn select_egress_defaults_to_flac_live() {
    assert_eq!(select_egress(false), EgressKind::FlacLive);
    assert_eq!(select_egress(true), EgressKind::WavBuffered);
  }

  #[test]
  fn has_session_counts_starting_and_playing_not_idle() {
    let bridge = Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None)));
    assert!(!bridge.has_session("dev"));

    {
      let mut devices = bridge.devices.lock();
      drop(devices.insert(
        "dev".to_owned(),
        DeviceSlot {
          generation: 1,
          state: DeviceState::Starting { generation: 1 },
        },
      ));
    }
    assert!(bridge.has_session("dev"));
    assert_eq!(bridge.session_generation("dev"), Some(1));

    {
      let mut devices = bridge.devices.lock();
      drop(devices.insert("dev".to_owned(), DeviceSlot { generation: 2, state: DeviceState::Idle }));
    }
    assert!(!bridge.has_session("dev"));
    assert_eq!(bridge.session_generation("dev"), None);
  }
}
