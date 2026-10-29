//! Session bridge: AirPlay PCM → lossless Cast egress (FLAC LIVE, WAV BUFFERED fallback).
//!
//! Per-device generation-stamped state machine:
//! `Idle → Starting{generation} → Playing{generation} → Idle`.
//!
//! Ownership of background work:
//! - per-session detached work (Cast PAUSE/PLAY, re-LOAD, re-anchor timers) lives in the
//!   session's task set and is aborted + joined by its teardown (replace or full end);
//! - replace / terminal-stall teardowns run on the bridge's detached set, which
//!   [`Bridge::run`] joins (bounded) on exit after ending every live session.
//!
//! Module map: `worker` (event routing + shutdown drain), `start` (prebuffer, initial LOAD),
//! `playback` (pause / resume / flush), `volume` (AirPlay volume to a speaker or to a speaker
//! group's members), `load` (re-LOAD coordination), `stall` (watchdog), `fallback`
//! (FLAC → WAV), `teardown` (session end), `state` (slots + session), `remote` (now-playing
//! metadata to Cast, Cast-side play / pause / volume to the sender), `group` (AirPlay
//! multi-select played as one Google Cast multizone session).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use crate::airplay::{AirPlayManager, AirPlaySessionEvent};
use crate::audio::PcmRing;
use crate::cast::CastControl;
use crate::registry::DeviceRegistry;

use self::group::CastGroup;
use self::state::{DeviceSlot, DeviceState};
use self::volume::GroupVolume;

mod fallback;
mod group;
mod load;
mod playback;
mod remote;
mod stall;
mod start;
mod state;
mod teardown;
#[cfg(test)]
mod test_support;
mod volume;
mod worker;

/// Bound for joining device workers once shutdown is requested.
const SHUTDOWN_WORKER_JOIN: Duration = Duration::from_secs(3);
/// Bound for ending live sessions (media, Cast STOP) and detached teardowns on bridge exit.
const SHUTDOWN_TEARDOWN: Duration = Duration::from_secs(5);
/// Upper bound for [`Bridge::run`] to return after [`Bridge::request_shutdown`].
pub const SHUTDOWN_TIMEOUT: Duration = SHUTDOWN_WORKER_JOIN.saturating_add(SHUTDOWN_TEARDOWN);

/// Orchestrates media HTTP + Cast load for AirPlay lifecycle events.
pub struct Bridge {
  registry: Arc<DeviceRegistry>,
  cast_pool: Arc<dyn CastControl>,
  /// Optional AirPlay manager for seeding `GET_PARAMETER volume` from Cast after LOAD.
  airplay: Option<Arc<AirPlayManager>>,
  /// Per-device generation-stamped state machine.
  devices: Mutex<HashMap<String, DeviceSlot>>,
  /// Devices that rejected FLAC this process lifetime (always use WAV BUFFERED).
  flac_fallback: Mutex<HashSet<String>>,
  /// Optional barrier waited once at the start of each `handle_session_start`.
  ///
  /// Tests use this to prove multi-device starts run concurrently (no HOL blocking).
  start_barrier: Mutex<Option<Arc<tokio::sync::Barrier>>>,
  /// Set once by [`Bridge::request_shutdown`]; `run` and device workers stop on it.
  shutdown: watch::Sender<bool>,
  /// Teardowns that outlive their session slot (replace, terminal stall); joined on exit.
  detached: Mutex<JoinSet<()>>,
  /// AirPlay multi-select groups played as one Cast multizone session, by `groupUUID`.
  ///
  /// Lock order: `devices` before `groups` when both are held.
  groups: Mutex<HashMap<String, CastGroup>>,
  /// Bridge-originated events (a group member promoted to start its own Cast session),
  /// routed through [`Bridge::run`] to the member's device worker.
  redispatch: mpsc::UnboundedSender<AirPlaySessionEvent>,
  redispatch_rx: Mutex<Option<mpsc::UnboundedReceiver<AirPlaySessionEvent>>>,
  /// Member volume of speaker-group receivers whose session plays elsewhere, by registry id.
  ///
  /// Never held together with another bridge lock.
  group_volumes: Mutex<HashMap<String, GroupVolume>>,
}

impl std::fmt::Debug for Bridge {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let live = {
      let devices = self.devices.lock();
      devices.values().filter(|slot| slot.is_live()).count()
    };
    let fallback_n = self.flac_fallback.lock().len();
    f.debug_struct("Bridge")
      .field("registry", &self.registry)
      .field("cast_pool", &self.cast_pool)
      .field("airplay", &self.airplay.is_some())
      .field("live_sessions", &live)
      .field("flac_fallback_devices", &fallback_n)
      .finish_non_exhaustive()
  }
}

/// Lookup of PCM rings by device id (implemented by [`crate::airplay::AirPlayManager`]).
pub trait RingLookup: Send + Sync {
  /// Shared ring for a device, if present.
  fn ring_for(&self, device_id: &str) -> Option<Arc<PcmRing>>;
}

impl RingLookup for AirPlayManager {
  fn ring_for(&self, device_id: &str) -> Option<Arc<PcmRing>> {
    self.pcm_ring(device_id)
  }
}

impl Bridge {
  /// Create a bridge over the shared device registry and Cast control (the warm pool).
  pub fn new(registry: Arc<DeviceRegistry>, cast_pool: Arc<dyn CastControl>) -> Self {
    let (redispatch, redispatch_rx) = mpsc::unbounded_channel();
    Self {
      registry,
      cast_pool,
      airplay: None,
      devices: Mutex::new(HashMap::new()),
      flac_fallback: Mutex::new(HashSet::new()),
      start_barrier: Mutex::new(None),
      shutdown: watch::Sender::new(false),
      detached: Mutex::new(JoinSet::new()),
      groups: Mutex::new(HashMap::new()),
      redispatch,
      redispatch_rx: Mutex::new(Some(redispatch_rx)),
      group_volumes: Mutex::new(HashMap::new()),
    }
  }

  /// Ask [`Bridge::run`] to stop: close event intake, stop device workers, end every live
  /// session (media first, then best-effort Cast STOP), and join detached teardowns.
  ///
  /// `run` returns within [`SHUTDOWN_TIMEOUT`]; queued AirPlay events are dropped.
  pub fn request_shutdown(&self) {
    let _was_requested = self.shutdown.send_replace(true);
  }

  /// Run `task` on the bridge-owned detached set (joined by `run` on exit).
  fn spawn_detached<F>(&self, task: F)
  where
    F: Future<Output = ()> + Send + 'static,
  {
    let mut detached = self.detached.lock();
    while let Some(result) = detached.try_join_next() {
      if let Err(err) = result
        && !err.is_cancelled()
      {
        tracing::warn!(error = %err, "detached bridge task panicked");
      }
    }
    let _abort = detached.spawn(task);
    drop(detached);
  }

  /// Spawn `task` on the task set of `device_id`'s live Playing `generation`.
  ///
  /// A replaced or ended generation drops the task unrun, so stale work never starts.
  fn spawn_session_task<F>(&self, device_id: &str, generation: u64, task: F)
  where
    F: Future<Output = ()> + Send + 'static,
  {
    let mut devices = self.devices.lock();
    match devices.get_mut(device_id).map(|slot| &mut slot.state) {
      Some(DeviceState::Playing { generation: live_gen, session }) if *live_gen == generation => {
        let _task = session.tasks.spawn(device_id, task);
        drop(devices);
      },
      Some(_) | None => {
        drop(devices);
        tracing::debug!(%device_id, generation, "dropping session task for stale generation");
      },
    }
  }

  /// Attach the AirPlay manager so successful Cast LOAD can refresh reported volume.
  #[must_use]
  pub fn with_airplay(mut self, airplay: Arc<AirPlayManager>) -> Self {
    self.airplay = Some(airplay);
    self
  }

  /// Install a barrier that each session start waits on once (tests only).
  pub fn set_start_barrier(&self, barrier: Arc<tokio::sync::Barrier>) {
    *self.start_barrier.lock() = Some(barrier);
  }

  /// Whether a live bridge session exists for `device_id`.
  ///
  /// `Starting` and `Playing` count as live; `Idle` does not.
  #[must_use]
  pub fn has_session(&self, device_id: &str) -> bool {
    self.devices.lock().get(device_id).is_some_and(DeviceSlot::is_live)
  }

  /// Current live session generation for `device_id`, if any.
  #[must_use]
  pub fn session_generation(&self, device_id: &str) -> Option<u64> {
    self.devices.lock().get(device_id).and_then(DeviceSlot::live_gen)
  }
}
