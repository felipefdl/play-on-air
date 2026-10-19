//! Per-device session state: generation-stamped slots and the live `Playing` session.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio::task::{AbortHandle, JoinError, JoinSet};

use crate::audio::PcmRing;
use crate::cast::{CastControl, CastTrack};
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
  /// AirPlay device that owns this session (its events drive it).
  pub(super) device_id: String,
  pub(super) pool: Arc<dyn CastControl>,
  /// Cast workers for media commands and for per-speaker volume.
  pub(super) route: Arc<CastRoute>,
  /// AirPlay multi-select group this session plays for (Cast multizone), if any.
  pub(super) group: Option<String>,
  /// PCM ring feeding this session's media body.
  pub(super) ring: Arc<PcmRing>,
  /// Forwards another receiver's AirPlay ring into [`Self::ring`] after a group owner hand-over.
  pub(super) source: Option<RingPump>,
  pub(super) stream_url: String,
  pub(super) cast_name: String,
  pub(super) channels: u16,
  pub(super) sample_rate: u32,
  pub(super) egress: EgressKind,
  /// AirPlay reported paused (local; stall watchdog honors this immediately).
  ///
  /// Each pause sends Cast PAUSE at once; Cast PAUSE / PLAY run in event order under
  /// [`Self::transport_flight`].
  pub(super) paused: bool,
  /// Cross-thread mirror of [`Self::paused`] for in-flight re-LOAD abort checks (B2).
  pub(super) playback_paused: Arc<AtomicBool>,
  /// When the current pause began (for long-pause re-LOAD).
  pub(super) paused_at: Option<Instant>,
  /// Flush re-LOADs requested so far (bumped by every AirPlay flush; the next resume owes one).
  pub(super) flush_reload_requested: u64,
  /// Highest [`Self::flush_reload_requested`] value covered by an accepted re-LOAD.
  ///
  /// A request stays owed while `done < requested`: a pause (or a competing re-LOAD that
  /// started before the flush) must not drop it, or Cast replays the pre-flush buffer tail.
  pub(super) flush_reload_done: u64,
  /// Last stall-driven re-LOAD (repeat → kick).
  pub(super) last_stall_reload_at: Option<Instant>,
  /// A stall re-LOAD a pause abandoned: Cast's media is still stalled, so the next resume
  /// re-LOADs instead of sending PLAY. Cleared by any accepted re-LOAD.
  pub(super) stall_reload_owed: bool,
  /// When this Playing generation became active (FLAC early-fallback window).
  pub(super) started_at: Instant,
  /// Last FLAC failure strike for this Playing generation (pair-window late fallback).
  pub(super) flac_strike: Option<(Instant, FlacStrikeReason)>,
  /// Serializes Cast PAUSE / PLAY: each re-reads [`Self::playback_paused`] under it right before
  /// the wire command, so whichever runs last matches the latest AirPlay state.
  pub(super) transport_flight: Arc<tokio::sync::Mutex<()>>,
  /// Detached Cast PAUSE/PLAY and re-LOADs owned by this generation.
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
  /// Now-playing metadata currently shown on Cast; every (re-)LOAD sends it.
  pub(super) track: Arc<Mutex<CastTrack>>,
  /// In-flight blocking Cast LOAD (rollover / reload); awaited on teardown before media STOP.
  pub(super) inflight_load: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
  /// Serializes join → publish → await → take of `inflight_load` across concurrent re-LOAD
  /// entry points (flush spawn, resume spawn, stall, rollover) so a second caller cannot
  /// overwrite a `JoinHandle` without aborting it (detached LOAD).
  pub(super) reload_flight: Arc<tokio::sync::Mutex<()>>,
  /// Present when this session times Cast playback to the AirPlay timeline.
  ///
  /// Absent for an uncalibrated model, a Google Cast group, and the WAV fallback.
  pub(super) sync: Option<Arc<super::sync::SyncSession>>,
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
  /// Member of a Cast multizone group whose owner session feeds Cast; its AirPlay audio is
  /// drained (or, after a hand-over, forwarded) instead of loaded.
  Following {
    generation: u64,
    follower: Box<FollowingSession>,
  },
}

/// AirPlay stream of a group member that plays through the group owner's Cast session.
pub(super) struct FollowingSession {
  /// AirPlay multi-select group id.
  pub(super) group: String,
  /// Stream sample rate (hand-over needs the owner's format).
  pub(super) sample_rate: u32,
  /// Drains the member's ring until it becomes the group's media source.
  pub(super) pump: RingPump,
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
    matches!(
      self.state,
      DeviceState::Starting { .. } | DeviceState::Playing { .. } | DeviceState::Following { .. }
    )
  }

  pub(super) const fn live_gen(&self) -> Option<u64> {
    match self.state {
      DeviceState::Starting { generation }
      | DeviceState::Playing { generation, .. }
      | DeviceState::Following { generation, .. } => Some(generation),
      DeviceState::Idle => None,
    }
  }
}

impl PlayingSession {
  /// Whether a fired flush still needs a re-LOAD that has not landed yet.
  pub(super) const fn flush_reload_owed(&self) -> bool {
    self.flush_reload_requested > self.flush_reload_done
  }

  /// The AirPlay ring this session's audio comes from (the owner's receiver ring).
  pub(super) fn airplay_ring(&self) -> &Arc<PcmRing> {
    self.source.as_ref().map_or(&self.ring, RingPump::source)
  }
}

/// Pool worker keys a session's Cast commands go to.
///
/// Media commands (LOAD, PAUSE, PLAY, STOP, metadata) follow the Cast session: the owner's
/// speaker, then the multizone session host once the group expanded. The volume a re-LOAD
/// reapplies goes to the owner's own receiver; AirPlay volume events route in `volume`.
#[derive(Debug)]
pub(super) struct CastRoute {
  media: Mutex<String>,
  volume: Mutex<String>,
}

impl CastRoute {
  pub(super) fn new(media: &str, volume: &str) -> Self {
    Self {
      media: Mutex::new(media.to_owned()),
      volume: Mutex::new(volume.to_owned()),
    }
  }

  /// Worker hosting the Cast media session.
  pub(super) fn media(&self) -> String {
    self.media.lock().clone()
  }

  /// Worker of the owner's own speaker.
  pub(super) fn volume(&self) -> String {
    self.volume.lock().clone()
  }

  pub(super) fn set_media(&self, key: &str) {
    key.clone_into(&mut self.media.lock());
  }

  pub(super) fn set_volume(&self, key: &str) {
    key.clone_into(&mut self.volume.lock());
  }
}

/// How often a [`RingPump`] moves audio.
const PUMP_INTERVAL: Duration = Duration::from_millis(10);

/// Frames moved per pop (bounded per lock hold).
const PUMP_CHUNK_FRAMES: usize = 4_096;

/// Drains a group member's AirPlay ring; once retargeted, forwards it into the group's media ring.
///
/// Every member of one AirPlay multi-select receives the same audio on the sender's PTP
/// timeline, so a follower's ring can replace the departed owner's ring without a re-LOAD.
pub(super) struct RingPump {
  source: Arc<PcmRing>,
  target: Arc<Mutex<Option<Arc<PcmRing>>>>,
  task: tokio::task::JoinHandle<()>,
}

impl RingPump {
  /// Start draining `source` (discarding until [`Self::forward_into`]).
  pub(super) fn spawn(source: Arc<PcmRing>) -> Self {
    let target: Arc<Mutex<Option<Arc<PcmRing>>>> = Arc::new(Mutex::new(None));
    let task_source = Arc::clone(&source);
    let task_target = Arc::clone(&target);
    let task = tokio::spawn(async move {
      let mut chunk = Vec::with_capacity(PUMP_CHUNK_FRAMES * usize::from(task_source.channels()));
      let mut tick = tokio::time::interval(PUMP_INTERVAL);
      tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
      loop {
        let _instant = tick.tick().await;
        let forward = task_target.lock().clone();
        let Some(sink) = forward else {
          task_source.clear();
          continue;
        };
        while task_source.pop_f32(PUMP_CHUNK_FRAMES, &mut chunk) > 0 {
          sink.push_f32(&chunk);
        }
      }
    });
    Self { source, target, task }
  }

  /// The AirPlay ring being drained.
  pub(super) const fn source(&self) -> &Arc<PcmRing> {
    &self.source
  }

  /// Forward everything from now on into `sink`.
  pub(super) fn forward_into(&self, sink: Arc<PcmRing>) {
    *self.target.lock() = Some(sink);
  }

  /// Stop pumping.
  pub(super) fn stop(self) {
    self.task.abort();
  }
}

/// Session-scoped detached work: Cast PAUSE/PLAY (and the pause diagnostics probe) and re-LOADs.
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
  use crate::cast::CastPool;
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
