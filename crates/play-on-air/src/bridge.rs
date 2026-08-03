//! Session bridge: AirPlay PCM → lossless Cast egress (FLAC LIVE, WAV BUFFERED fallback).
//!
//! Per-device generation-stamped state machine:
//! `Idle → Starting{generation} → Playing{generation} → Idle`.
//!
//! Teardown liveness is tracked by a detached task on replace (prior generation, skip Cast STOP)
//! or by awaiting teardown on the full-end path; the device slot is `Idle` when no live session.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::sleep;

use crate::airplay::{AirPlayManager, AirPlaySessionEvent, airplay_db_to_cast_linear, cast_linear_to_airplay_db};
use crate::audio::PcmRing;
use crate::cast::{CastPool, CastStreamKind, MediaLoadRequest};
use crate::error::{Error, Result};
use crate::media::{MediaContent, MediaServer, MediaServerHandle, RolloverSignal};
use crate::net::advertise_host_for_peer;
use crate::registry::DeviceRegistry;

/// Frames of real PCM required before Cast LOAD (~0.5 s at 48 kHz).
///
/// Prebuffer feeds the first real-PCM HTTP chunks after silence preroll; the media
/// server's silence preroll builds the Cast-side cushion; `LIVE_LEAD` maintains it.
const PREBUFFER_FRAMES: usize = 24_000;
/// Max prebuffer poll iterations (160 × 50 ms = 8 s). Fail if still incomplete.
const PREBUFFER_POLLS: u32 = 160;
const PREBUFFER_POLL: Duration = Duration::from_millis(50);
/// How long the main `run` loop waits for per-device workers after the event channel closes.
const WORKER_JOIN_TIMEOUT: Duration = Duration::from_secs(30);
/// How long teardown waits for the rollover async task after cancel/abort.
const ROLLOVER_TASK_JOIN_TIMEOUT: Duration = Duration::from_secs(2);
/// How long teardown waits for an in-flight blocking Cast LOAD.
///
/// Cast pool `COMMAND_TIMEOUT` is 6 s; keep a small margin so a slow LOAD is joined
/// instead of detached, without re-introducing the old 30 s HOL risk.
const INFLIGHT_LOAD_JOIN_TIMEOUT: Duration = Duration::from_secs(8);
/// After a long pause the Cast media session may be gone or holding a stale buffer tail.
/// Re-LOAD instead of PLAY when the pause exceeded this budget.
const LONG_PAUSE_RELOAD: Duration = Duration::from_secs(30);
/// Collapse multiple AirPlay flushes within this window to a single Cast re-LOAD.
const FLUSH_RELOAD_DEBOUNCE: Duration = Duration::from_secs(1);
/// Stall watchdog poll interval (cheap `MediaServer::progress` read).
const STALL_CHECK_INTERVAL: Duration = Duration::from_secs(2);
/// `last_body_write` older than this while PCM is available ⇒ Cast stopped pulling.
const STALL_BODY_STALE: Duration = Duration::from_secs(5);
/// Second stall within this window after a stall re-LOAD kicks the session.
const STALL_REPEAT_WINDOW: Duration = Duration::from_secs(60);
/// Early-session FLAC → WAV fallback window after a successful FLAC LOAD.
const FLAC_FALLBACK_WINDOW: Duration = Duration::from_secs(10);

/// How Cast volume is applied after a progressive media LOAD.
#[derive(Debug, Clone, Copy, PartialEq)]
enum LoadVolumePolicy {
  /// Initial session start: leave the Cast device volume unchanged.
  ///
  /// Forcing 1.0 overwrote the speaker's current level every stream start.
  PreserveDevice,
  /// Content-Length rollover re-LOAD: re-apply last AirPlay linear volume, or skip if unknown.
  Rollover { last_volume: Option<f32> },
}

/// Resolve the linear volume to set after LOAD, if any.
const fn volume_after_load(policy: LoadVolumePolicy) -> Option<f32> {
  match policy {
    LoadVolumePolicy::PreserveDevice => None,
    LoadVolumePolicy::Rollover { last_volume } => last_volume,
  }
}

/// Cast egress selected for a bridge session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EgressKind {
  /// Chunked `audio/flac` with Cast `streamType` LIVE.
  FlacLive,
  /// Progressive WAV with Content-Length and Cast `streamType` BUFFERED.
  WavBuffered,
}

/// PCM layout advertised to the media server and Cast load.
#[derive(Debug, Clone, Copy)]
struct StreamFormat {
  channels: u16,
  sample_rate: u32,
}

/// Choose Cast egress for `device_id` given process-lifetime FLAC rejection memory.
const fn select_egress(device_remembered_wav: bool) -> EgressKind {
  if device_remembered_wav {
    EgressKind::WavBuffered
  } else {
    EgressKind::FlacLive
  }
}

/// Whether teardown should issue Cast STOP after media shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TeardownCastPolicy {
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
/// then timed best-effort Cast STOP. Media must not wait on STOP success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEndStep {
  /// Shut down the local media HTTP server (stop underrun immediately).
  MediaShutdown,
  /// Best-effort Cast STOP with timeout (may fail or time out).
  CastStopBestEffort,
}

/// Shipped full session-end order (media first, then Cast STOP).
pub const fn session_end_steps() -> [SessionEndStep; 2] {
  [SessionEndStep::MediaShutdown, SessionEndStep::CastStopBestEffort]
}

/// Whether replace teardown should skip Cast STOP (new LOAD replaces the app session).
const fn replace_skips_cast_stop(policy: TeardownCastPolicy) -> bool {
  matches!(policy, TeardownCastPolicy::SkipStopForReplace)
}

/// Whether a late-completing Cast LOAD should issue STOP because the session died.
///
/// Replace teardown sets `late_stop_allowed` false so a superseded generation cannot STOP the
/// device after the new generation's LOAD (serialized pool: Load(old) → Load(new) → Stop would
/// kill the replacement session).
const fn late_load_should_stop(alive: bool, late_stop_allowed: bool) -> bool {
  !alive && late_stop_allowed
}

/// True when the Cast error is transport-shaped (timeout, disconnect, no session/worker).
///
/// Those must not poison process-lifetime FLAC→WAV memory; the caller may still fall back to
/// WAV for *this* session only.
fn is_transport_shaped_cast_error(err: &Error) -> bool {
  // Join / bridge wrapper around a failed pool call is not a decisive media rejection.
  let msg = match err {
    Error::Cast(msg) | Error::Bridge(msg) => msg.as_str(),
    Error::Io(_) => return true,
    Error::Config { .. } | Error::Discovery(_) | Error::Media(_) | Error::AirPlay(_) | Error::Audio(_) => return false,
  };
  let lower = msg.to_ascii_lowercase();
  lower.contains("timeout")
    || lower.contains("timed out")
    || lower.contains("disconnected")
    || lower.contains("reply dropped")
    || lower.contains("no warm cast worker")
    || lower.contains("no active session")
    || lower.contains("not connected")
    || lower.contains("reconnect backoff")
    || lower.contains("broken pipe")
    || lower.contains("connection")
    || lower.contains("connect ")
}

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

/// Clears `Starting{generation}` if still armed when dropped (panic safety for session start).
struct StartingGuard<'a> {
  bridge: &'a Bridge,
  device_id: String,
  generation: u64,
  armed: bool,
}

impl<'a> StartingGuard<'a> {
  fn arm(bridge: &'a Bridge, device_id: &str, generation: u64) -> Self {
    Self {
      bridge,
      device_id: device_id.to_owned(),
      generation,
      armed: true,
    }
  }

  const fn disarm(&mut self) {
    self.armed = false;
  }
}

impl Drop for StartingGuard<'_> {
  fn drop(&mut self) {
    if self.armed {
      self.bridge.clear_starting_if(&self.device_id, self.generation);
    }
  }
}

/// Tracks per-device session workers; aborts all on drop so `Bridge::run` abort cannot leave zombies.
struct DeviceWorkerSet {
  senders: HashMap<String, mpsc::UnboundedSender<AirPlaySessionEvent>>,
  set: JoinSet<()>,
}

impl DeviceWorkerSet {
  fn new() -> Self {
    Self {
      senders: HashMap::new(),
      set: JoinSet::new(),
    }
  }

  fn spawn_worker(
    &mut self,
    bridge: Arc<Bridge>,
    rings: Arc<dyn RingLookup>,
    device_id: String,
  ) -> mpsc::UnboundedSender<AirPlaySessionEvent> {
    let (tx, rx) = mpsc::unbounded_channel();
    let worker_id = device_id.clone();
    let _abort = self.set.spawn(async move {
      device_worker_loop(bridge, rings, worker_id, rx).await;
    });
    let sender = tx.clone();
    drop(self.senders.insert(device_id, tx));
    sender
  }

  /// Drop inboxes, wait for workers to drain, then abort any still running.
  async fn shutdown_graceful(&mut self) {
    self.senders.clear();
    let deadline = tokio::time::Instant::now() + WORKER_JOIN_TIMEOUT;
    while !self.set.is_empty() {
      let now = tokio::time::Instant::now();
      if now >= deadline {
        tracing::warn!("device session workers join timed out; aborting remaining");
        self.set.abort_all();
        break;
      }
      let remaining = deadline - now;
      match tokio::time::timeout(remaining, self.set.join_next()).await {
        Ok(Some(Ok(()))) => {},
        Ok(Some(Err(err))) => {
          if !err.is_cancelled() {
            tracing::warn!(error = %err, "device session worker panicked");
          }
        },
        Ok(None) => break,
        Err(_) => {
          tracing::warn!("device session workers join timed out; aborting remaining");
          self.set.abort_all();
          break;
        },
      }
    }
    // Drain aborted / finished tasks so JoinSet drops cleanly.
    while self.set.join_next().await.is_some() {}
  }
}

impl Drop for DeviceWorkerSet {
  fn drop(&mut self) {
    self.senders.clear();
    self.set.abort_all();
  }
}

/// Live media + Cast control state for a `Playing` generation.
struct PlayingSession {
  media: MediaServerHandle,
  device_id: String,
  pool: Arc<CastPool>,
  /// PCM ring feeding this session's media body.
  ring: Arc<PcmRing>,
  stream_url: String,
  cast_name: String,
  channels: u16,
  sample_rate: u32,
  egress: EgressKind,
  /// AirPlay reported paused; Cast PAUSE issued (session stays Playing).
  paused: bool,
  /// When the current pause began (for long-pause re-LOAD).
  paused_at: Option<Instant>,
  /// Last flush-driven re-LOAD (debounce).
  last_flush_reload_at: Option<Instant>,
  /// Last stall-driven re-LOAD (repeat → kick).
  last_stall_reload_at: Option<Instant>,
  /// When this Playing generation became active (FLAC early-fallback window).
  started_at: Instant,
  /// Drop / send to stop the `LiveWav` Content-Length rollover re-LOAD loop.
  rollover_cancel: Option<oneshot::Sender<()>>,
  rollover_task: Option<tokio::task::JoinHandle<()>>,
  /// Cancel the stall watchdog task.
  watchdog_cancel: Option<oneshot::Sender<()>>,
  watchdog_task: Option<tokio::task::JoinHandle<()>>,
  /// Cleared first on teardown so late LOAD paths STOP instead of reviving playback
  /// (when [`Self::late_stop_allowed`] is still true).
  session_alive: Arc<AtomicBool>,
  /// Replace path sets this false so a late-completing old LOAD does not STOP the new session.
  ///
  /// Full end leaves this true: after `session_alive` is false, late LOAD still STOPs so
  /// playback cannot revive against a dead media server.
  late_stop_allowed: Arc<AtomicBool>,
  /// Last AirPlay volume as Cast linear level (updated by volume events).
  last_volume_linear: Arc<Mutex<Option<f32>>>,
  /// In-flight blocking Cast LOAD (rollover / reload); awaited on teardown before media STOP.
  inflight_load: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
  /// Serializes join → publish → await → take of `inflight_load` across concurrent re-LOAD
  /// entry points (flush spawn, resume spawn, stall, rollover) so a second caller cannot
  /// overwrite a `JoinHandle` without aborting it (detached LOAD).
  reload_flight: Arc<tokio::sync::Mutex<()>>,
}

/// Per-device generation-stamped session state.
#[derive(Default)]
enum DeviceState {
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
struct DeviceSlot {
  /// Last generation stamped when a session start was accepted (0 = never).
  generation: u64,
  state: DeviceState,
}

impl Default for DeviceSlot {
  fn default() -> Self {
    Self { generation: 0, state: DeviceState::Idle }
  }
}

impl DeviceSlot {
  const fn is_live(&self) -> bool {
    matches!(self.state, DeviceState::Starting { .. } | DeviceState::Playing { .. })
  }

  const fn live_gen(&self) -> Option<u64> {
    match self.state {
      DeviceState::Starting { generation } | DeviceState::Playing { generation, .. } => Some(generation),
      DeviceState::Idle => None,
    }
  }
}

/// Orchestrates media HTTP + Cast load for AirPlay lifecycle events.
pub struct Bridge {
  registry: Arc<DeviceRegistry>,
  cast_pool: Arc<CastPool>,
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

impl Bridge {
  /// Create a bridge over the shared device registry and warm Cast pool.
  pub fn new(registry: Arc<DeviceRegistry>, cast_pool: Arc<CastPool>) -> Self {
    Self {
      registry,
      cast_pool,
      airplay: None,
      devices: Mutex::new(HashMap::new()),
      flac_fallback: Mutex::new(HashSet::new()),
      start_barrier: Mutex::new(None),
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

  /// Run until the session event channel closes.
  ///
  /// Events are routed to a small per-`device_id` worker so one device's slow
  /// Cast LOAD cannot head-of-line-block another device. Ordering within a
  /// device (Started / Ended / Volume) remains sequential.
  ///
  /// Workers are tracked in a [`JoinSet`] and aborted when `run` returns or is
  /// dropped/aborted, so nested tasks cannot outlive the bridge task as zombies.
  pub async fn run(
    self: Arc<Self>,
    mut events: mpsc::UnboundedReceiver<AirPlaySessionEvent>,
    rings: Arc<dyn RingLookup>,
  ) {
    let mut workers = DeviceWorkerSet::new();

    while let Some(event) = events.recv().await {
      let device_id = event_device_id(&event).to_owned();
      if !workers.senders.contains_key(&device_id) {
        let _tx = workers.spawn_worker(Arc::clone(&self), Arc::clone(&rings), device_id.clone());
      }
      let Some(sender) = workers.senders.get(&device_id).cloned() else {
        tracing::error!(%device_id, "device session worker missing after spawn");
        continue;
      };
      match sender.send(event) {
        Ok(()) => {},
        Err(mpsc::error::SendError(failed_event)) => {
          // Dead worker: recreate and retry the same event (do not discard).
          tracing::warn!(%device_id, "device session worker dropped; recreating and retrying event");
          drop(workers.senders.remove(&device_id));
          let tx = workers.spawn_worker(Arc::clone(&self), Arc::clone(&rings), device_id.clone());
          if tx.send(failed_event).is_err() {
            tracing::error!(%device_id, "device session worker failed to accept retried event");
          }
        },
      }
    }

    workers.shutdown_graceful().await;
  }

  async fn handle_session_start(
    self: &Arc<Self>,
    device_id: &str,
    sample_rate: u32,
    ring: Arc<PcmRing>,
    rings: Arc<dyn RingLookup>,
  ) -> Result<()> {
    let is_current = |expected: &Arc<PcmRing>| -> bool {
      rings.ring_for(device_id).is_some_and(|current| Arc::ptr_eq(&current, expected))
    };

    // A stale Started means the client already restarted the stream (a fresh
    // event with the rebuilt ring is queued behind this one) or the receiver
    // was withdrawn. Skip instead of bridging a ring that no longer feeds.
    if !is_current(&ring) {
      tracing::info!(%device_id, "skipping stale AirPlay session start (ring rebuilt or receiver gone)");
      return Ok(());
    }

    // Test seam: both concurrent starts must reach this wait for the barrier to open.
    let start_gate = self.start_barrier.lock().clone();
    if let Some(gate) = start_gate {
      let _party = gate.wait().await;
    }

    // Accept start: bump generation, install Starting, detach prior Playing teardown.
    // Do NOT await prior Cast STOP — new LOAD replaces the app session (SkipStopForReplace).
    // Every non-install exit after this point must clear Starting for this generation.
    // StartingGuard also clears on panic between accept and Playing install.
    let (generation, prior) = self.accept_start(device_id);
    let mut starting_guard = StartingGuard::arm(self, device_id, generation);

    if let Some(old) = prior {
      tracing::info!(
        %device_id,
        new_gen = generation,
        "detaching prior session teardown (skip Cast STOP; new LOAD replaces)"
      );
      detach_teardown(*old, TeardownCastPolicy::SkipStopForReplace);
    }

    let Some(device) = self.registry.get(device_id) else {
      self.clear_starting_if(device_id, generation);
      return Err(Error::Bridge(format!("unknown device {device_id}")));
    };

    let fmt = StreamFormat {
      channels: ring.channels().max(1),
      sample_rate: sample_rate.max(1),
    };

    match wait_for_prebuffer(device_id, &ring, rings.as_ref()).await {
      Ok(true) => {},
      Ok(false) => {
        self.clear_starting_if(device_id, generation);
        return Ok(());
      },
      Err(err) => {
        self.clear_starting_if(device_id, generation);
        return Err(err);
      },
    }

    if !is_current(&ring) || !self.starting_still(device_id, generation) {
      tracing::info!(%device_id, generation, "session restarted before Cast load; skipping stale start");
      self.clear_starting_if(device_id, generation);
      return Ok(());
    }

    let result = self.start_cast_session(device_id, generation, &device, ring, fmt).await;
    // Disarm only when this generation is live Playing (install kept the session).
    // clear_starting_if is a no-op on Playing; keep explicit error clears + guard for panics.
    if result.is_ok()
      && self
        .devices
        .lock()
        .get(device_id)
        .is_some_and(|slot| matches!(slot.state, DeviceState::Playing { generation: g, .. } if g == generation))
    {
      starting_guard.disarm();
    }
    result
  }

  /// Bump generation, install `Starting`, return prior `Playing` session if any.
  fn accept_start(&self, device_id: &str) -> (u64, Option<Box<PlayingSession>>) {
    let mut devices = self.devices.lock();
    let slot = devices.entry(device_id.to_owned()).or_default();
    slot.generation = slot.generation.saturating_add(1);
    let generation = slot.generation;
    let previous = std::mem::replace(&mut slot.state, DeviceState::Starting { generation });
    let prior = match previous {
      DeviceState::Playing { generation: old_gen, session } => {
        // Prior session tears down on a detached task; slot is already Starting{new}.
        // Skip Cast STOP on that path so the new LOAD is not HOL-blocked.
        tracing::debug!(%device_id, old_gen, new_gen = generation, "prior Playing moved to detached drain");
        Some(session)
      },
      DeviceState::Starting { generation: old_gen } => {
        tracing::debug!(%device_id, old_gen, new_gen = generation, "superseding in-flight Starting");
        None
      },
      DeviceState::Idle => None,
    };
    drop(devices);
    (generation, prior)
  }

  fn clear_starting_if(&self, device_id: &str, generation: u64) {
    let mut devices = self.devices.lock();
    let Some(slot) = devices.get_mut(device_id) else {
      return;
    };
    if matches!(slot.state, DeviceState::Starting { generation: g } if g == generation) {
      slot.state = DeviceState::Idle;
    }
    drop(devices);
  }

  fn starting_still(&self, device_id: &str, generation: u64) -> bool {
    let devices = self.devices.lock();
    devices
      .get(device_id)
      .is_some_and(|slot| matches!(slot.state, DeviceState::Starting { generation: g } if g == generation))
  }

  async fn start_cast_session(
    self: &Arc<Self>,
    device_id: &str,
    generation: u64,
    device: &crate::registry::Device,
    ring: Arc<PcmRing>,
    fmt: StreamFormat,
  ) -> Result<()> {
    let host = advertise_host_for_peer(&device.host);
    let media = match MediaServer::start(&host).await {
      Ok(media) => media,
      Err(err) => {
        self.clear_starting_if(device_id, generation);
        return Err(err);
      },
    };
    let stream_url = media.stream_url();

    let remembered_wav = self.flac_fallback.lock().contains(device_id);
    let mut egress = select_egress(remembered_wav);

    tracing::info!(
      %device_id,
      generation,
      cast = %device.host,
      %stream_url,
      ?egress,
      frames = ring.available_frames(),
      "starting Cast bridge session"
    );

    let session_ring = Arc::clone(&ring);
    let load_result = self
      .initial_cast_load(device_id, generation, device, &media, &ring, fmt, &stream_url, &mut egress)
      .await;

    match load_result {
      Ok(session) => {
        // Post-LOAD liveness: do not install if Ended / superseding start cancelled Starting.
        if !self.starting_still(device_id, generation) {
          tracing::info!(
            %device_id,
            generation,
            "Cast LOAD completed but start was cancelled; discarding session"
          );
          media.shutdown();
          let pool = Arc::clone(&self.cast_pool);
          let id = device_id.to_owned();
          let _stop = tokio::task::spawn_blocking(move || {
            pool.stop_best_effort(&id, Duration::from_secs(2));
          })
          .await;
          self.clear_starting_if(device_id, generation);
          return Ok(());
        }
        tracing::info!(
          %device_id,
          generation,
          cast = %device.host,
          transport_id = %session.transport_id,
          media_session_id = session.media_session_id,
          %stream_url,
          ?egress,
          "bridge session Cast load ok"
        );
        self
          .install_playing_after_load(device_id, generation, device, media, session_ring, stream_url, fmt, egress)
          .await;
        Ok(())
      },
      Err(err) => {
        media.shutdown();
        self.clear_starting_if(device_id, generation);
        tracing::warn!(%device_id, generation, error = %err, "Cast load failed (device may be offline)");
        Err(err)
      },
    }
  }

  /// Perform the initial Cast LOAD, with FLAC → WAV fallback on LOAD error.
  #[expect(
    clippy::too_many_arguments,
    reason = "LOAD needs device identity, media content inputs, and egress out-param"
  )]
  async fn initial_cast_load(
    &self,
    device_id: &str,
    generation: u64,
    device: &crate::registry::Device,
    media: &MediaServerHandle,
    ring: &Arc<PcmRing>,
    fmt: StreamFormat,
    stream_url: &str,
    egress: &mut EgressKind,
  ) -> Result<crate::cast::MediaSessionRef> {
    match *egress {
      EgressKind::FlacLive => {
        media.set_content(MediaContent::LiveFlac {
          ring: Arc::clone(ring),
          channels: fmt.channels,
          sample_rate: fmt.sample_rate,
        });
        let flac_result = self
          .blocking_load(
            device_id,
            MediaLoadRequest::flac(stream_url.to_owned(), CastStreamKind::Live).with_title(device.name.clone()),
          )
          .await?;
        match flac_result {
          Ok(session) => Ok(session),
          Err(flac_err) => {
            // Always WAV for this session. Process-lifetime memory only on decisive rejection
            // (not timeout / disconnect / no worker) so transport blips do not poison FLAC forever.
            let remember = !is_transport_shaped_cast_error(&flac_err);
            tracing::warn!(
              %device_id,
              generation,
              error = %flac_err,
              remember_fallback = remember,
              "Cast FLAC LIVE load failed; falling back to WAV BUFFERED"
            );
            if remember {
              self.remember_flac_fallback(device_id);
            }
            *egress = EgressKind::WavBuffered;
            media.set_content(MediaContent::LiveWav {
              ring: Arc::clone(ring),
              channels: fmt.channels,
              sample_rate: fmt.sample_rate,
            });
            self
              .blocking_load(
                device_id,
                MediaLoadRequest::wav(stream_url.to_owned(), CastStreamKind::Buffered).with_title(device.name.clone()),
              )
              .await?
          },
        }
      },
      EgressKind::WavBuffered => {
        media.set_content(MediaContent::LiveWav {
          ring: Arc::clone(ring),
          channels: fmt.channels,
          sample_rate: fmt.sample_rate,
        });
        self
          .blocking_load(
            device_id,
            MediaLoadRequest::wav(stream_url.to_owned(), CastStreamKind::Buffered).with_title(device.name.clone()),
          )
          .await?
      },
    }
  }

  async fn blocking_load(
    &self,
    device_id: &str,
    request: MediaLoadRequest,
  ) -> Result<std::result::Result<crate::cast::MediaSessionRef, Error>> {
    let pool = Arc::clone(&self.cast_pool);
    let load_device_id = device_id.to_owned();
    tokio::task::spawn_blocking(move || {
      cast_load_media(&pool, &load_device_id, request, LoadVolumePolicy::PreserveDevice)
    })
    .await
    .map_err(|join_err| Error::Bridge(format!("Cast load task join: {join_err}")))
  }

  #[expect(
    clippy::too_many_arguments,
    reason = "playing install needs media handle, ring, and session identity"
  )]
  async fn install_playing_after_load(
    self: &Arc<Self>,
    device_id: &str,
    generation: u64,
    device: &crate::registry::Device,
    media: MediaServerHandle,
    session_ring: Arc<PcmRing>,
    stream_url: String,
    fmt: StreamFormat,
    egress: EgressKind,
  ) {
    let cast_linear = self.sync_reported_volume_after_load(device_id).await;
    let session_alive = Arc::new(AtomicBool::new(true));
    let late_stop_allowed = Arc::new(AtomicBool::new(true));
    let last_volume_linear = Arc::new(Mutex::new(cast_linear));
    let inflight_load = Arc::new(Mutex::new(None));
    let reload_flight = Arc::new(tokio::sync::Mutex::new(()));

    let (rollover_cancel, rollover_task) = if egress == EgressKind::WavBuffered {
      let (cancel, task) = spawn_rollover_reload_loop(
        device_id.to_owned(),
        stream_url.clone(),
        device.name.clone(),
        Arc::clone(&self.cast_pool),
        media.rollover_signal(),
        Arc::clone(&session_alive),
        Arc::clone(&late_stop_allowed),
        Arc::clone(&last_volume_linear),
        Arc::clone(&inflight_load),
        Arc::clone(&reload_flight),
      );
      (Some(cancel), Some(task))
    } else {
      (None, None)
    };

    let (watchdog_cancel, watchdog_task) = spawn_stall_watchdog(Arc::clone(self), device_id.to_owned(), generation);

    let playing = PlayingSession {
      media,
      device_id: device_id.to_owned(),
      pool: Arc::clone(&self.cast_pool),
      ring: session_ring,
      stream_url,
      cast_name: device.name.clone(),
      channels: fmt.channels,
      sample_rate: fmt.sample_rate,
      egress,
      paused: false,
      paused_at: None,
      last_flush_reload_at: None,
      last_stall_reload_at: None,
      started_at: Instant::now(),
      rollover_cancel,
      rollover_task,
      watchdog_cancel: Some(watchdog_cancel),
      watchdog_task: Some(watchdog_task),
      session_alive,
      late_stop_allowed,
      last_volume_linear,
      inflight_load,
      reload_flight,
    };

    let orphan = {
      let mut devices = self.devices.lock();
      let result = match devices.get_mut(device_id) {
        Some(slot) if matches!(slot.state, DeviceState::Starting { generation: g } if g == generation) => {
          slot.state = DeviceState::Playing { generation, session: Box::new(playing) };
          None
        },
        Some(_) | None => Some(playing),
      };
      drop(devices);
      result
    };

    if let Some(orphaned) = orphan {
      tracing::info!(%device_id, generation, "start superseded before Playing install; tearing down");
      detach_teardown(orphaned, TeardownCastPolicy::StopBestEffort);
    }
  }

  fn remember_flac_fallback(&self, device_id: &str) {
    let mut set = self.flac_fallback.lock();
    if set.insert(device_id.to_owned()) {
      tracing::info!(
        %device_id,
        "remembering WAV BUFFERED fallback for device (FLAC rejected or early stall)"
      );
    }
    drop(set);
  }

  /// Read Cast volume after LOAD and refresh AirPlay `GET_PARAMETER` if a manager is attached.
  async fn sync_reported_volume_after_load(&self, device_id: &str) -> Option<f32> {
    let pool = Arc::clone(&self.cast_pool);
    let id = device_id.to_owned();
    let level = match tokio::task::spawn_blocking(move || pool.get_volume(&id)).await {
      Ok(Ok(level)) => level,
      Ok(Err(err)) => {
        tracing::debug!(%device_id, error = %err, "Cast get_volume after LOAD failed");
        return None;
      },
      Err(err) => {
        tracing::debug!(%device_id, error = %err, "Cast get_volume task join failed");
        return None;
      },
    };
    let db = cast_linear_to_airplay_db(level);
    if let Some(airplay) = &self.airplay {
      airplay.set_reported_volume_db(device_id, db);
    }
    tracing::info!(
      %device_id,
      cast_linear = level,
      airplay_db = db,
      "synced AirPlay reported volume from Cast"
    );
    Some(level)
  }

  /// End the bridge session for `device_id` if any (media down + Cast STOP best-effort).
  ///
  /// Used by the Cast ownership-loss path so the app can tear media without waiting
  /// for the AirPlay stack to emit `Ended`.
  pub async fn end_session(&self, device_id: &str) {
    self.handle_session_end(device_id).await;
  }

  /// Tear down the current live generation for `device_id` (full STOP).
  async fn handle_session_end(&self, device_id: &str) {
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
  async fn handle_ended(&self, device_id: &str, rings: &dyn RingLookup) {
    enum EndedDecision {
      Starting { generation: u64 },
      Playing { generation: u64, stale: bool },
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
            .is_some_and(|current| Arc::ptr_eq(&current, &session.ring));
          Some(EndedDecision::Playing {
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
      },
      Some(EndedDecision::Playing { generation, stale: true }) => {
        tracing::debug!(%device_id, generation, "dropping stale Ended (session ring still current)");
      },
      Some(EndedDecision::Playing { generation, stale: false }) => {
        let removed = self.take_live_session_if_gen(device_id, generation);
        if let Some(session) = removed {
          teardown_playing(*session, TeardownCastPolicy::StopBestEffort).await;
          tracing::info!(%device_id, generation, "bridge session ended (media dropped; Cast STOP best-effort)");
        }
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
    };
    drop(devices);
    result
  }

  fn take_live_session_if_gen(&self, device_id: &str, generation: u64) -> Option<Box<PlayingSession>> {
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

  async fn handle_volume(&self, device_id: &str, volume_db: f32) {
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
  /// Local paused bookkeeping is synchronous; Cast PAUSE is detached so the device worker
  /// never awaits Cast control I/O (same idea as [`Self::spawn_reload_playing_media`]).
  fn handle_pause(self: &Arc<Self>, device_id: &str) {
    let cast_pause = {
      let mut devices = self.devices.lock();
      let Some(slot) = devices.get_mut(device_id) else {
        return;
      };
      let DeviceState::Playing { generation, session } = &mut slot.state else {
        drop(devices);
        tracing::debug!(%device_id, "dropping Paused; no Playing generation");
        return;
      };
      if session.paused {
        return;
      }
      session.paused = true;
      session.paused_at = Some(Instant::now());
      let plan = (
        *generation,
        Arc::clone(&session.session_alive),
        Arc::clone(&session.pool),
        device_id.to_owned(),
      );
      drop(devices);
      plan
    };
    let (generation, alive, pool, id) = cast_pause;
    tracing::info!(%device_id, generation, "AirPlay paused; pausing Cast media");
    drop(tokio::spawn(async move {
      let log_id = id.clone();
      let pause_result = tokio::task::spawn_blocking(move || {
        if !alive.load(Ordering::Acquire) {
          return Ok(());
        }
        pool.pause(&id)
      })
      .await;
      match pause_result {
        Ok(Ok(())) => {},
        Ok(Err(err)) => {
          tracing::debug!(device_id = %log_id, generation, error = %err, "Cast pause failed");
        },
        Err(err) => {
          tracing::debug!(device_id = %log_id, generation, error = %err, "Cast pause task join failed");
        },
      }
    }));
  }

  /// Resume Cast media after AirPlay playout restarts.
  ///
  /// Local unpause is synchronous; Cast PLAY (and long-pause re-LOAD) are detached so the
  /// device worker does not await Cast control I/O.
  fn handle_resume(self: &Arc<Self>, device_id: &str) {
    let action = {
      let mut devices = self.devices.lock();
      let Some(slot) = devices.get_mut(device_id) else {
        return;
      };
      let DeviceState::Playing { generation, session } = &mut slot.state else {
        drop(devices);
        tracing::debug!(%device_id, "dropping Resumed; no Playing generation");
        return;
      };
      let long_pause = session.paused_at.is_some_and(|at| at.elapsed() >= LONG_PAUSE_RELOAD);
      session.paused = false;
      session.paused_at = None;
      let action = if long_pause {
        ResumeAction::Reload { generation: *generation }
      } else {
        ResumeAction::Play {
          generation: *generation,
          pool: Arc::clone(&session.pool),
          session_alive: Arc::clone(&session.session_alive),
        }
      };
      drop(devices);
      action
    };

    match action {
      ResumeAction::Play { generation, pool, session_alive } => {
        tracing::info!(%device_id, generation, "AirPlay resumed; resuming Cast media");
        let bridge = Arc::clone(self);
        let id = device_id.to_owned();
        drop(tokio::spawn(async move {
          let play_id = id.clone();
          let play_result = tokio::task::spawn_blocking(move || {
            if !session_alive.load(Ordering::Acquire) {
              return Ok(());
            }
            pool.play(&play_id)
          })
          .await;
          let play_ok = matches!(play_result, Ok(Ok(())));
          if !play_ok {
            if let Ok(Err(err)) = &play_result {
              tracing::debug!(device_id = %id, generation, error = %err, "Cast play failed; re-LOADing");
            }
            if let Err(err) = &play_result {
              tracing::debug!(device_id = %id, generation, error = %err, "Cast play task join failed; re-LOADing");
            }
            // Off the device worker so Paused/Ended can proceed while re-LOAD runs.
            bridge.spawn_reload_playing_media(&id, generation, "resume_play_failed");
          }
        }));
      },
      ResumeAction::Reload { generation } => {
        tracing::info!(%device_id, generation, "AirPlay resumed after long pause; re-LOADing Cast media");
        // Detached: long Cast re-LOAD must not HOL-block the device worker FIFO.
        self.spawn_reload_playing_media(device_id, generation, "long_pause_resume");
      },
    }
  }

  /// Flush: ring already cleared by ingest; re-LOAD to discard Cast's ~2 s buffer.
  fn handle_flush(self: &Arc<Self>, device_id: &str) {
    let decision = {
      let mut devices = self.devices.lock();
      let Some(slot) = devices.get_mut(device_id) else {
        return;
      };
      let DeviceState::Playing { generation, session } = &mut slot.state else {
        drop(devices);
        tracing::debug!(%device_id, "dropping Flushed; no Playing generation");
        return;
      };
      let decision = if session
        .last_flush_reload_at
        .is_some_and(|at| at.elapsed() < FLUSH_RELOAD_DEBOUNCE)
      {
        tracing::debug!(%device_id, generation = *generation, "debounce: skipping flush re-LOAD");
        None
      } else {
        session.last_flush_reload_at = Some(Instant::now());
        Some(*generation)
      };
      drop(devices);
      decision
    };
    let Some(generation) = decision else {
      return;
    };
    tracing::info!(%device_id, generation, "AirPlay flushed; re-LOADing Cast media");
    // Detached generation-scoped re-LOAD keeps flush off the worker FIFO.
    self.spawn_reload_playing_media(device_id, generation, "flush");
  }

  /// Spawn a generation-scoped re-LOAD off the device worker (gen checks inside reload).
  fn spawn_reload_playing_media(self: &Arc<Self>, device_id: &str, generation: u64, reason: &'static str) {
    let bridge = Arc::clone(self);
    let id = device_id.to_owned();
    drop(tokio::spawn(async move {
      let _ok = bridge.reload_playing_media(&id, generation, reason).await;
    }));
  }

  /// Stall watchdog tick for a Playing generation (called from a detached task).
  async fn stall_watchdog_tick(self: &Arc<Self>, device_id: &str, generation: u64) {
    let snapshot = {
      let devices = self.devices.lock();
      let Some(slot) = devices.get(device_id) else {
        return;
      };
      let DeviceState::Playing { generation: live_gen, session } = &slot.state else {
        return;
      };
      if *live_gen != generation {
        return;
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
      snap
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
        self
          .terminal_stall_end(device_id, generation, "media stall repeated within window")
          .await;
      },
      StallAction::Reload => {
        {
          let mut devices = self.devices.lock();
          if let Some(DeviceState::Playing { generation: live_gen, session }) =
            devices.get_mut(device_id).map(|s| &mut s.state)
            && *live_gen == generation
          {
            session.last_stall_reload_at = Some(Instant::now());
          }
          drop(devices);
        }

        tracing::warn!(%device_id, generation, "media stall detected; re-LOADing Cast media");
        let ok = self.reload_playing_media(device_id, generation, "stall").await;
        if !ok {
          self.terminal_stall_end(device_id, generation, "stall re-LOAD failed").await;
        }
      },
    }
  }

  /// Tear down Playing and kick AirPlay so the phone shows the session is dead.
  async fn terminal_stall_end(self: &Arc<Self>, device_id: &str, generation: u64, reason: &str) {
    tracing::warn!(%device_id, generation, reason, "terminal media stall; ending session");
    if let Some(session) = self.take_live_session_if_gen(device_id, generation) {
      teardown_playing(*session, TeardownCastPolicy::StopBestEffort).await;
    }
    if let Some(airplay) = self.airplay.as_ref()
      && let Err(err) = airplay.kick_clients(device_id).await
    {
      tracing::warn!(%device_id, error = %err, "AirPlay kick after terminal stall failed");
    }
  }

  /// Re-LOAD the current Playing media URL. Returns whether LOAD succeeded while still live.
  async fn reload_playing_media(&self, device_id: &str, generation: u64, reason: &str) -> bool {
    let load_plan = {
      let devices = self.devices.lock();
      let Some(slot) = devices.get(device_id) else {
        return false;
      };
      let DeviceState::Playing { generation: live_gen, session } = &slot.state else {
        return false;
      };
      if *live_gen != generation || !session.session_alive.load(Ordering::Acquire) {
        return false;
      }
      let request = match session.egress {
        EgressKind::FlacLive => MediaLoadRequest::flac(session.stream_url.clone(), CastStreamKind::Live),
        EgressKind::WavBuffered => MediaLoadRequest::wav(session.stream_url.clone(), CastStreamKind::Buffered),
      }
      .with_title(session.cast_name.clone());
      let last_volume = *session.last_volume_linear.lock();
      let plan = (
        Arc::clone(&session.pool),
        request,
        LoadVolumePolicy::Rollover { last_volume },
        Arc::clone(&session.session_alive),
        Arc::clone(&session.late_stop_allowed),
        Arc::clone(&session.inflight_load),
        Arc::clone(&session.reload_flight),
      );
      drop(devices);
      Some(plan)
    };
    let Some((pool, request, volume, alive, late_stop_allowed, inflight_load, reload_flight)) = load_plan else {
      return false;
    };

    // Serialize the whole join → spawn → store → await → take region so concurrent
    // callers (flush spawn + resume spawn + stall + rollover) cannot both pass
    // join_prior and both publish — second overwrite would detach a LOAD.
    let _flight = reload_flight.lock().await;

    // Single-flight: finish any prior LOAD before publishing a new one (no detached race).
    join_prior_inflight_load(&inflight_load, device_id).await;

    let id = device_id.to_owned();
    let (result_tx, result_rx) = oneshot::channel();
    {
      let mut slot = inflight_load.lock();
      if !alive.load(Ordering::Acquire) {
        return false;
      }
      let load_task = tokio::task::spawn_blocking(move || {
        let result = cast_load_media(&pool, &id, request, volume);
        // SkipStopForReplace means a replacement start owns the device; late STOP after old
        // LOAD would kill the new session on the serialized pool.
        if late_load_should_stop(alive.load(Ordering::Acquire), late_stop_allowed.load(Ordering::Acquire)) {
          pool.stop_best_effort(&id, Duration::from_secs(2));
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
        tracing::info!(
          %device_id,
          generation,
          reason,
          transport_id = %session.transport_id,
          media_session_id = session.media_session_id,
          "Cast re-LOAD ok"
        );
        true
      },
      Ok(Err(err)) => {
        tracing::warn!(%device_id, generation, reason, error = %err, "Cast re-LOAD failed");
        false
      },
      Err(_) => {
        tracing::debug!(%device_id, generation, reason, "Cast re-LOAD result dropped (session teardown)");
        false
      },
    }
  }

  /// Pool re-LOAD recovered media (IDLE/BUFFERING path). Early FLAC → WAV when in window.
  pub async fn on_media_recovered(self: &Arc<Self>, device_id: &str) {
    let early_flac_gen = {
      let devices = self.devices.lock();
      let Some(slot) = devices.get(device_id) else {
        drop(devices);
        tracing::info!(%device_id, "Cast pool re-LOAD recovered media (no bridge session)");
        return;
      };
      let early = match &slot.state {
        DeviceState::Playing { generation, session }
          if session.egress == EgressKind::FlacLive && session.started_at.elapsed() < FLAC_FALLBACK_WINDOW =>
        {
          Some(*generation)
        },
        DeviceState::Idle | DeviceState::Starting { .. } | DeviceState::Playing { .. } => {
          let generation = slot.live_gen();
          tracing::info!(
            %device_id,
            session_generation = ?generation,
            "Cast pool re-LOAD recovered media session"
          );
          None
        },
      };
      drop(devices);
      early
    };
    let Some(generation) = early_flac_gen else {
      return;
    };

    tracing::warn!(
      %device_id,
      generation,
      "early FLAC media_recovered (IDLE recovery); falling back to WAV BUFFERED"
    );
    self.remember_flac_fallback(device_id);
    let switched = self.switch_playing_to_wav(device_id, generation).await;
    if !switched {
      tracing::debug!(%device_id, generation, "early FLAC media_recovered fallback did not switch (session gone or already WAV)");
    }
  }

  /// Switch a live FLAC session to WAV BUFFERED (early stall fallback). Returns success.
  async fn switch_playing_to_wav(self: &Arc<Self>, device_id: &str, generation: u64) -> bool {
    let switched = {
      let mut devices = self.devices.lock();
      let Some(slot) = devices.get_mut(device_id) else {
        return false;
      };
      let DeviceState::Playing { generation: live_gen, session } = &mut slot.state else {
        return false;
      };
      if *live_gen != generation || session.egress != EgressKind::FlacLive {
        return false;
      }
      session.media.set_content(MediaContent::LiveWav {
        ring: Arc::clone(&session.ring),
        channels: session.channels,
        sample_rate: session.sample_rate,
      });
      session.egress = EgressKind::WavBuffered;
      // Start rollover for WAV if not already running.
      if session.rollover_task.is_none() {
        let (cancel, task) = spawn_rollover_reload_loop(
          session.device_id.clone(),
          session.stream_url.clone(),
          session.cast_name.clone(),
          Arc::clone(&session.pool),
          session.media.rollover_signal(),
          Arc::clone(&session.session_alive),
          Arc::clone(&session.late_stop_allowed),
          Arc::clone(&session.last_volume_linear),
          Arc::clone(&session.inflight_load),
          Arc::clone(&session.reload_flight),
        );
        session.rollover_cancel = Some(cancel);
        session.rollover_task = Some(task);
      }
      session.last_stall_reload_at = Some(Instant::now());
      drop(devices);
      true
    };
    if !switched {
      return false;
    }
    self.reload_playing_media(device_id, generation, "flac_to_wav_fallback").await
  }
}

struct StallSnapshot {
  paused: bool,
  last_write: Option<Instant>,
  ring_frames: usize,
  started_at: Instant,
  egress: EgressKind,
  last_stall: Option<Instant>,
}

enum ResumeAction {
  Play {
    generation: u64,
    pool: Arc<CastPool>,
    session_alive: Arc<AtomicBool>,
  },
  Reload {
    generation: u64,
  },
}

const fn event_device_id(event: &AirPlaySessionEvent) -> &str {
  match event {
    AirPlaySessionEvent::Started { device_id, .. }
    | AirPlaySessionEvent::Ended { device_id }
    | AirPlaySessionEvent::Paused { device_id }
    | AirPlaySessionEvent::Resumed { device_id }
    | AirPlaySessionEvent::Flushed { device_id }
    | AirPlaySessionEvent::Volume { device_id, .. } => device_id.as_str(),
  }
}

async fn device_worker_loop(
  bridge: Arc<Bridge>,
  rings: Arc<dyn RingLookup>,
  device_id: String,
  mut events: mpsc::UnboundedReceiver<AirPlaySessionEvent>,
) {
  while let Some(event) = events.recv().await {
    match event {
      AirPlaySessionEvent::Started {
        device_id: event_device,
        sample_rate,
        ring,
      } => {
        if let Err(err) = bridge
          .handle_session_start(&event_device, sample_rate, ring, Arc::clone(&rings))
          .await
        {
          tracing::error!(device_id = %event_device, error = %err, "failed to start Cast bridge session");
        }
      },
      AirPlaySessionEvent::Ended { device_id: event_device } => {
        bridge.handle_ended(&event_device, rings.as_ref()).await;
      },
      AirPlaySessionEvent::Paused { device_id: event_device } => {
        bridge.handle_pause(&event_device);
      },
      AirPlaySessionEvent::Resumed { device_id: event_device } => {
        bridge.handle_resume(&event_device);
      },
      AirPlaySessionEvent::Flushed { device_id: event_device } => {
        bridge.handle_flush(&event_device);
      },
      AirPlaySessionEvent::Volume { device_id: event_device, volume_db } => {
        bridge.handle_volume(&event_device, volume_db).await;
      },
    }
  }
  tracing::debug!(%device_id, "device session worker exited");
}

/// Poll until a full prebuffer is available, or the ring is superseded.
///
/// Returns `Ok(true)` only when [`PREBUFFER_FRAMES`] complete frames are ready.
/// Returns `Ok(false)` if the session restarted (stale ring). Errors on timeout.
async fn wait_for_prebuffer(device_id: &str, ring: &Arc<PcmRing>, rings: &dyn RingLookup) -> Result<bool> {
  for _ in 0..PREBUFFER_POLLS {
    let still_current = rings.ring_for(device_id).is_some_and(|current| Arc::ptr_eq(&current, ring));
    if !still_current {
      tracing::info!(%device_id, "session restarted during prebuffer; skipping stale start");
      return Ok(false);
    }
    if ring.available_frames() >= PREBUFFER_FRAMES {
      return Ok(true);
    }
    sleep(PREBUFFER_POLL).await;
  }

  let available = ring.available_frames();
  if available >= PREBUFFER_FRAMES {
    return Ok(true);
  }
  Err(Error::Bridge(format!(
    "prebuffer timeout after 8s: {available} frames available, need {PREBUFFER_FRAMES}"
  )))
}

/// Cast LOAD helper (shared by initial start, rollover, flush, stall, resume).
fn cast_load_media(
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
fn spawn_rollover_reload_loop(
  device_id: String,
  stream_url: String,
  cast_name: String,
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
          let title = cast_name.clone();
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
                MediaLoadRequest::wav(url, CastStreamKind::Buffered).with_title(title),
                LoadVolumePolicy::Rollover { last_volume },
              );
              // Late LOAD after full end: STOP so playback cannot revive against a dead HTTP server.
              // Replace path clears late_stop_allowed so STOP cannot kill the new generation.
              if late_load_should_stop(alive.load(Ordering::Acquire), late_stop.load(Ordering::Acquire)) {
                load_pool.stop_best_effort(&id, Duration::from_secs(2));
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

fn spawn_stall_watchdog(
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

fn detach_teardown(session: PlayingSession, policy: TeardownCastPolicy) {
  drop(tokio::spawn(async move {
    let device_id = session.device_id.clone();
    teardown_playing(session, policy).await;
    tracing::debug!(%device_id, ?policy, "detached session teardown complete");
  }));
}

async fn teardown_playing(session: PlayingSession, policy: TeardownCastPolicy) {
  let device_id = session.device_id.clone();
  session.session_alive.store(false, Ordering::Release);
  // Replace path: new generation owns the device; late-completing old LOAD must not STOP.
  if replace_skips_cast_stop(policy) {
    session.late_stop_allowed.store(false, Ordering::Release);
  }

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
  let end_device_id = session.device_id;
  if tokio::task::spawn_blocking(move || end_media_and_maybe_cast_stop(media, &pool, &end_device_id, policy))
    .await
    .is_err()
  {
    tracing::warn!(%device_id, "session teardown task panicked");
  }
}

/// Run media shutdown then optional Cast STOP (blocking; call from `spawn_blocking`).
fn end_media_and_maybe_cast_stop(
  media: MediaServerHandle,
  pool: &CastPool,
  device_id: &str,
  policy: TeardownCastPolicy,
) {
  // Always stop media HTTP first so underrun ends even if Cast STOP hangs.
  media.shutdown();
  if replace_skips_cast_stop(policy) {
    // New LOAD is about to replace the Cast app session; avoid HOL-blocking it.
    return;
  }
  pool.stop_best_effort(device_id, Duration::from_secs(2));
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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::registry::Device;
  use std::sync::atomic::AtomicUsize;
  use std::time::{Duration, Instant};

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

  /// Minimal playing session for teardown / pause tests (no live Cast worker).
  fn test_playing_session(
    media: MediaServerHandle,
    device_id: &str,
    pool: Arc<CastPool>,
    ring: Arc<PcmRing>,
  ) -> PlayingSession {
    PlayingSession {
      media,
      device_id: device_id.to_owned(),
      pool,
      ring,
      stream_url: "http://127.0.0.1:9/stream".to_owned(),
      cast_name: "test".to_owned(),
      channels: 2,
      sample_rate: 48_000,
      egress: EgressKind::FlacLive,
      paused: false,
      paused_at: None,
      last_flush_reload_at: None,
      last_stall_reload_at: None,
      started_at: Instant::now(),
      rollover_cancel: None,
      rollover_task: None,
      watchdog_cancel: None,
      watchdog_task: None,
      session_alive: Arc::new(AtomicBool::new(true)),
      late_stop_allowed: Arc::new(AtomicBool::new(true)),
      last_volume_linear: Arc::new(Mutex::new(None)),
      inflight_load: Arc::new(Mutex::new(None)),
      reload_flight: Arc::new(tokio::sync::Mutex::new(())),
    }
  }

  fn insert_playing(bridge: &Bridge, device_id: &str, generation: u64, session: PlayingSession) {
    let mut devices = bridge.devices.lock();
    let slot = devices.entry(device_id.to_owned()).or_default();
    slot.generation = generation;
    slot.state = DeviceState::Playing { generation, session: Box::new(session) };
    drop(devices);
  }

  #[test]
  fn session_end_steps_media_before_cast_stop() {
    let steps = session_end_steps();
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0], SessionEndStep::MediaShutdown);
    assert_eq!(steps[1], SessionEndStep::CastStopBestEffort);
  }

  #[test]
  fn replace_teardown_skips_cast_stop() {
    assert!(replace_skips_cast_stop(TeardownCastPolicy::SkipStopForReplace));
    assert!(!replace_skips_cast_stop(TeardownCastPolicy::StopBestEffort));
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

  #[test]
  fn accept_start_bumps_generation_and_returns_prior_playing() {
    let bridge = Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None)));
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
    let media = runtime.block_on(async { MediaServer::start("127.0.0.1").await.expect("media") });
    let pool = Arc::new(CastPool::new(None));
    let ring1 = Arc::new(PcmRing::new(2, 64));
    insert_playing(&bridge, "dev", 1, test_playing_session(media, "dev", pool, Arc::clone(&ring1)));

    let (generation, prior) = bridge.accept_start("dev");
    assert_eq!(generation, 2);
    assert!(prior.is_some());
    assert!(bridge.has_session("dev"));
    assert!(matches!(
      bridge.devices.lock().get("dev").map(|s| &s.state),
      Some(DeviceState::Starting { generation: 2 })
    ));
    // Detached teardown of prior would normally run; drop it here to shut media.
    if let Some(old) = prior {
      runtime.block_on(teardown_playing(*old, TeardownCastPolicy::SkipStopForReplace));
    }
  }

  #[test]
  fn flac_fallback_memory_skips_flac_next_select() {
    let bridge = Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None)));
    assert_eq!(
      select_egress(bridge.flac_fallback.lock().contains("nest")),
      EgressKind::FlacLive
    );
    bridge.remember_flac_fallback("nest");
    assert_eq!(
      select_egress(bridge.flac_fallback.lock().contains("nest")),
      EgressKind::WavBuffered
    );
    // Idempotent remember.
    bridge.remember_flac_fallback("nest");
    assert_eq!(bridge.flac_fallback.lock().len(), 1);
  }

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

  /// `handle_pause` is sync and returns after local bookkeeping; Cast PAUSE is detached.
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
      assert!(session.paused, "paused flag set before Cast PAUSE completes");
      drop(devices);
    }
    bridge.handle_session_end("dev-pause-fast").await;
  }

  #[tokio::test]
  async fn stale_ended_dropped_when_ring_still_current() {
    let registry = Arc::new(DeviceRegistry::new());
    let pool = Arc::new(CastPool::new(None));
    let bridge = Bridge::new(registry, Arc::clone(&pool));
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
    let pool = Arc::new(CastPool::new(None));
    let bridge = Bridge::new(registry, Arc::clone(&pool));
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

  #[tokio::test]
  async fn handle_session_end_shuts_media_before_cast_stop_and_does_not_hang() {
    let registry = Arc::new(DeviceRegistry::new());
    let _appeared = registry.appear(Device {
      id: "dev-1".to_owned(),
      name: "Test".to_owned(),
      host: "127.0.0.1".to_owned(),
      hostname: "test.local".to_owned(),
      port: 9,
      last_seen: Instant::now(),
      instance: "dev-1".to_owned(),
      pending_leave_deadline: None,
      pending_leave_since: None,
    });
    let pool = Arc::new(CastPool::new(None));
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
    let pool = Arc::new(CastPool::new(None));
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
    let pool = Arc::new(CastPool::new(None));
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
    let pool = Arc::new(CastPool::new(None));
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
          MediaLoadRequest::wav("http://127.0.0.1:9/stream".to_owned(), CastStreamKind::Buffered).with_title("test"),
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

  async fn http_get_status_ok(url: &str) -> bool {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let Some(without_scheme) = url.strip_prefix("http://") else {
      return false;
    };
    let Some((host_port, path)) = without_scheme.split_once('/') else {
      return false;
    };
    let Ok(mut stream) = TcpStream::connect(host_port).await else {
      return false;
    };
    let req = format!("GET /{path} HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");
    if stream.write_all(req.as_bytes()).await.is_err() {
      return false;
    }
    let mut buf = Vec::new();
    if stream.read_to_end(&mut buf).await.is_err() {
      return false;
    }
    let text = String::from_utf8_lossy(&buf);
    text.contains("200")
  }

  struct FixedRingLookup {
    current: Option<Arc<PcmRing>>,
  }

  impl RingLookup for FixedRingLookup {
    fn ring_for(&self, _device_id: &str) -> Option<Arc<PcmRing>> {
      self.current.clone()
    }
  }

  struct MapRingLookup {
    rings: HashMap<String, Arc<PcmRing>>,
  }

  impl RingLookup for MapRingLookup {
    fn ring_for(&self, device_id: &str) -> Option<Arc<PcmRing>> {
      self.rings.get(device_id).map(Arc::clone)
    }
  }

  #[tokio::test]
  async fn stale_session_start_skips_without_prebuffer_or_session() {
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None))));
    let event_ring = Arc::new(PcmRing::new(2, 64));
    let rebuilt_ring = Arc::new(PcmRing::new(2, 64));
    let rings: Arc<dyn RingLookup> = Arc::new(FixedRingLookup { current: Some(rebuilt_ring) });

    let start = Instant::now();
    bridge
      .handle_session_start("dev-1", 48_000, event_ring, rings)
      .await
      .expect("stale start skips cleanly");
    assert!(
      start.elapsed() < Duration::from_secs(1),
      "stale start must not prebuffer or Cast-load"
    );
    assert!(!bridge.has_session("dev-1"));
  }

  #[tokio::test]
  async fn session_start_with_receiver_gone_skips() {
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None))));
    let event_ring = Arc::new(PcmRing::new(2, 64));
    let rings: Arc<dyn RingLookup> = Arc::new(FixedRingLookup { current: None });

    bridge
      .handle_session_start("dev-1", 48_000, event_ring, rings)
      .await
      .expect("withdrawn receiver skips cleanly");
    assert!(!bridge.has_session("dev-1"));
  }

  #[tokio::test]
  async fn multi_device_session_starts_do_not_serialize() {
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let registry = Arc::new(DeviceRegistry::new());
    for id in ["dev-a", "dev-b"] {
      let _appeared = registry.appear(Device {
        id: id.to_owned(),
        name: id.to_owned(),
        host: "127.0.0.1".to_owned(),
        hostname: format!("{id}.local"),
        port: 9,
        last_seen: Instant::now(),
        instance: id.to_owned(),
        pending_leave_deadline: None,
        pending_leave_since: None,
      });
    }

    let bridge = Arc::new(Bridge::new(registry, Arc::new(CastPool::new(None))));
    bridge.set_start_barrier(Arc::clone(&barrier));

    let mut ring_map = HashMap::new();
    let mut samples = Vec::with_capacity(PREBUFFER_FRAMES * 2);
    for _ in 0..PREBUFFER_FRAMES {
      samples.push(0.01);
      samples.push(0.01);
    }
    for id in ["dev-a", "dev-b"] {
      let ring = Arc::new(PcmRing::new(2, PREBUFFER_FRAMES * 2));
      ring.push_f32(&samples);
      drop(ring_map.insert(id.to_owned(), ring));
    }
    let rings: Arc<dyn RingLookup> = Arc::new(MapRingLookup { rings: ring_map.clone() });

    let (tx, rx) = mpsc::unbounded_channel();
    let run = tokio::spawn({
      let bridge_for_run = Arc::clone(&bridge);
      async move {
        bridge_for_run.run(rx, rings).await;
      }
    });

    let ring_a = Arc::clone(ring_map.get("dev-a").expect("dev-a"));
    let ring_b = Arc::clone(ring_map.get("dev-b").expect("dev-b"));
    tx.send(AirPlaySessionEvent::Started {
      device_id: "dev-a".to_owned(),
      sample_rate: 48_000,
      ring: ring_a,
    })
    .expect("send a");
    tx.send(AirPlaySessionEvent::Started {
      device_id: "dev-b".to_owned(),
      sample_rate: 48_000,
      ring: ring_b,
    })
    .expect("send b");

    let concurrent = tokio::time::timeout(Duration::from_secs(3), barrier.wait()).await;
    assert!(
      concurrent.is_ok(),
      "both device session starts must enter handle_session_start concurrently (no cross-device HOL)"
    );

    drop(tx);
    let joined = tokio::time::timeout(Duration::from_secs(10), run).await;
    assert!(joined.is_ok(), "bridge run must exit after event channel close");
  }

  #[tokio::test]
  async fn aborting_bridge_run_aborts_device_workers() {
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None))));
    let rings: Arc<dyn RingLookup> = Arc::new(FixedRingLookup { current: None });
    let (tx, rx) = mpsc::unbounded_channel();
    let run = tokio::spawn({
      let bridge_for_run = Arc::clone(&bridge);
      async move {
        bridge_for_run.run(rx, rings).await;
      }
    });

    tx.send(AirPlaySessionEvent::Ended { device_id: "dev-zombie".to_owned() })
      .expect("send");
    sleep(Duration::from_millis(50)).await;

    run.abort();
    let result = tokio::time::timeout(Duration::from_secs(2), run).await;
    assert!(result.is_ok(), "aborted bridge run must finish promptly");
    drop(tx);
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
    // Immediate second flush is debounced (still Playing; no panic without worker).
    bridge.handle_flush("dev-flush");
    {
      let devices = bridge.devices.lock();
      let DeviceState::Playing { session, .. } = &devices.get("dev-flush").expect("slot").state else {
        panic!("expected Playing");
      };
      assert!(session.last_flush_reload_at.is_some());
      drop(devices);
    }
    bridge.handle_session_end("dev-flush").await;
  }

  #[test]
  fn media_load_request_default_flac_is_live() {
    let req = MediaLoadRequest::flac("http://127.0.0.1/stream", CastStreamKind::Live);
    assert_eq!(req.content_type, "audio/flac");
    assert_eq!(req.stream_kind, CastStreamKind::Live);
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
    let bridge = Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None)));
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

  #[tokio::test]
  async fn session_start_unknown_device_clears_starting() {
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None))));
    let ring = Arc::new(PcmRing::new(2, 64));
    let rings: Arc<dyn RingLookup> = Arc::new(FixedRingLookup { current: Some(Arc::clone(&ring)) });
    let err = bridge
      .handle_session_start("missing-dev", 48_000, ring, rings)
      .await
      .expect_err("unknown device must fail");
    assert!(err.to_string().contains("unknown device"), "unexpected error: {err}");
    assert!(!bridge.has_session("missing-dev"), "failed start path must clear Starting");
  }

  #[tokio::test(start_paused = true)]
  async fn session_start_prebuffer_timeout_clears_starting() {
    let registry = Arc::new(DeviceRegistry::new());
    let _appeared = registry.appear(Device {
      id: "dev-prebuf".to_owned(),
      name: "Prebuf".to_owned(),
      host: "127.0.0.1".to_owned(),
      hostname: "prebuf.local".to_owned(),
      port: 9,
      last_seen: Instant::now(),
      instance: "dev-prebuf".to_owned(),
      pending_leave_deadline: None,
      pending_leave_since: None,
    });
    let bridge = Arc::new(Bridge::new(registry, Arc::new(CastPool::new(None))));
    // Current ring but empty → prebuffer polls until timeout (virtual time advances).
    let ring = Arc::new(PcmRing::new(2, PREBUFFER_FRAMES * 2));
    let rings: Arc<dyn RingLookup> = Arc::new(FixedRingLookup { current: Some(Arc::clone(&ring)) });
    let err = bridge
      .handle_session_start("dev-prebuf", 48_000, ring, rings)
      .await
      .expect_err("empty ring must prebuffer-timeout");
    assert!(err.to_string().contains("prebuffer timeout"), "unexpected error: {err}");
    assert!(!bridge.has_session("dev-prebuf"), "prebuffer timeout must clear Starting");
  }

  #[tokio::test]
  async fn on_media_recovered_early_flac_switches_to_wav() {
    let pool = Arc::new(CastPool::new(None));
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let mut playing = test_playing_session(media, "dev-recov", Arc::clone(&pool), Arc::new(PcmRing::new(2, 64)));
    playing.egress = EgressKind::FlacLive;
    playing.started_at = Instant::now();
    insert_playing(&bridge, "dev-recov", 1, playing);

    bridge.on_media_recovered("dev-recov").await;

    assert!(
      bridge.flac_fallback.lock().contains("dev-recov"),
      "early media_recovered must remember WAV fallback"
    );
    {
      let devices = bridge.devices.lock();
      let DeviceState::Playing { session: live, .. } = &devices.get("dev-recov").expect("slot").state else {
        panic!("expected Playing after media_recovered");
      };
      assert_eq!(live.egress, EgressKind::WavBuffered);
      drop(devices);
    }
    bridge.handle_session_end("dev-recov").await;
  }

  #[tokio::test]
  async fn on_media_recovered_outside_window_keeps_flac() {
    let pool = Arc::new(CastPool::new(None));
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let mut playing = test_playing_session(media, "dev-late", Arc::clone(&pool), Arc::new(PcmRing::new(2, 64)));
    playing.egress = EgressKind::FlacLive;
    // Started well before the early-fallback window.
    playing.started_at = Instant::now()
      .checked_sub(FLAC_FALLBACK_WINDOW + Duration::from_secs(1))
      .unwrap_or_else(Instant::now);
    insert_playing(&bridge, "dev-late", 1, playing);

    bridge.on_media_recovered("dev-late").await;

    assert!(!bridge.flac_fallback.lock().contains("dev-late"));
    {
      let devices = bridge.devices.lock();
      let DeviceState::Playing { session: live, .. } = &devices.get("dev-late").expect("slot").state else {
        panic!("expected Playing");
      };
      assert_eq!(live.egress, EgressKind::FlacLive);
      drop(devices);
    }
    bridge.handle_session_end("dev-late").await;
  }

  // --- B1: late STOP after replace ---

  #[test]
  fn late_load_should_stop_only_when_dead_and_allowed() {
    assert!(!late_load_should_stop(true, true));
    assert!(!late_load_should_stop(true, false));
    assert!(late_load_should_stop(false, true));
    assert!(!late_load_should_stop(false, false));
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

    // Replace teardown order: session_alive=false, then late_stop_allowed=false, then join LOAD.
    session_alive.store(false, Ordering::Release);
    assert!(
      replace_skips_cast_stop(TeardownCastPolicy::SkipStopForReplace),
      "replace path is the one that must suppress late STOP"
    );
    late_stop_allowed.store(false, Ordering::Release);
    let _party = gate.wait();
    load.join().expect("load thread");

    assert_eq!(
      stop_count.load(Ordering::SeqCst),
      0,
      "late STOP must be suppressed when replace disallows it"
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
    let pool = Arc::new(CastPool::new(None));
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

  // --- Q1: transport-shaped FLAC errors ---

  #[test]
  fn transport_shaped_cast_errors_do_not_remember_fallback() {
    let transport = [
      "warm Cast load timed out for nest",
      "warm Cast worker for nest disconnected",
      "no active session",
      "no warm Cast worker for nest",
      "warm Cast device not connected",
      "reconnect backoff active for nest",
      "warm connection channel: broken pipe",
      "connect 192.0.2.1:8009: connection refused",
      "warm Cast load reply dropped for nest",
    ];
    for msg in transport {
      assert!(
        is_transport_shaped_cast_error(&Error::Cast(msg.to_owned())),
        "expected transport: {msg}"
      );
    }
  }

  #[test]
  fn decisive_cast_errors_are_not_transport_shaped() {
    let decisive = [
      "warm media load: unsupported content type",
      "LOAD status had no media session entries",
      "warm media load: invalid request",
    ];
    for msg in decisive {
      assert!(
        !is_transport_shaped_cast_error(&Error::Cast(msg.to_owned())),
        "expected decisive (not transport): {msg}"
      );
    }
  }

  // --- Q2: StartingGuard ---

  #[test]
  fn starting_guard_clears_starting_on_drop() {
    let bridge = Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None)));
    {
      let mut devices = bridge.devices.lock();
      drop(devices.insert(
        "dev-guard".to_owned(),
        DeviceSlot {
          generation: 3,
          state: DeviceState::Starting { generation: 3 },
        },
      ));
    }
    assert!(bridge.has_session("dev-guard"));
    {
      let _guard = StartingGuard::arm(&bridge, "dev-guard", 3);
      // drop armed → clear Starting
    }
    assert!(!bridge.has_session("dev-guard"), "armed StartingGuard drop must clear Starting");
    assert!(matches!(
      bridge.devices.lock().get("dev-guard").map(|s| &s.state),
      Some(DeviceState::Idle)
    ));
  }

  #[test]
  fn starting_guard_disarm_preserves_starting() {
    let bridge = Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None)));
    {
      let mut devices = bridge.devices.lock();
      drop(devices.insert(
        "dev-guard-disarm".to_owned(),
        DeviceSlot {
          generation: 1,
          state: DeviceState::Starting { generation: 1 },
        },
      ));
    }
    {
      let mut guard = StartingGuard::arm(&bridge, "dev-guard-disarm", 1);
      guard.disarm();
    }
    assert!(
      bridge.has_session("dev-guard-disarm"),
      "disarmed StartingGuard must not clear Starting"
    );
    assert!(matches!(
      bridge.devices.lock().get("dev-guard-disarm").map(|s| &s.state),
      Some(DeviceState::Starting { generation: 1 })
    ));
  }

  // --- I1: stall watchdog pure decisions ---

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
