//! Session bridge: AirPlay PCM → continuous lossless WAV HTTP → Cast LIVE load.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::sleep;

use crate::airplay::{AirPlayManager, AirPlaySessionEvent, airplay_db_to_cast_linear, cast_linear_to_airplay_db};
use crate::audio::{PcmRing, encode_pcm_i16_to_flac};
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
/// Frames copied for the FLAC quality-path snapshot at session start.
const SNAPSHOT_FRAMES: usize = 2048;
/// How long the main `run` loop waits for per-device workers after the event channel closes.
const WORKER_JOIN_TIMEOUT: Duration = Duration::from_secs(30);
/// How long teardown waits for the rollover async task after cancel/abort.
const ROLLOVER_TASK_JOIN_TIMEOUT: Duration = Duration::from_secs(2);
/// How long teardown waits for an in-flight blocking Cast LOAD.
///
/// Cast pool `COMMAND_TIMEOUT` is 20s; keep margin so a slow LOAD is joined instead of detached.
const INFLIGHT_LOAD_JOIN_TIMEOUT: Duration = Duration::from_secs(30);
/// Ignore queued AirPlay pause events for this long after Cast LOAD succeeds.
///
/// Pause-watch can enqueue `Paused` while `handle_session_start` blocks on prebuffer + LOAD
/// (1–3+ s). That stale event would Cast-PAUSE a brand-new session and leave Nest silent
/// while HTTP still pulls. ~2 s is above typical post-load settle and ~2× pause-idle (750 ms).
const PAUSE_GRACE: Duration = Duration::from_secs(2);
/// Ring frames above this ⇒ AirPlay still has PCM buffered; treat pause as false idle.
const PAUSE_RING_PCM_THRESHOLD: usize = 256;

/// How Cast volume is applied after a progressive WAV LOAD.
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

/// One live bridge session for a device.
struct ActiveSession {
  media: MediaServerHandle,
  device_id: String,
  pool: Arc<CastPool>,
  /// PCM ring feeding this session's `LiveWav` body (for pause re-validation).
  ring: Arc<PcmRing>,
  /// Drop / send to stop the `LiveWav` Content-Length rollover re-LOAD loop.
  rollover_cancel: Option<oneshot::Sender<()>>,
  rollover_task: Option<tokio::task::JoinHandle<()>>,
  /// Cleared first on teardown so late LOAD paths STOP instead of reviving playback.
  session_alive: Arc<AtomicBool>,
  /// Last AirPlay volume as Cast linear level (updated by volume events).
  last_volume_linear: Arc<Mutex<Option<f32>>>,
  /// In-flight blocking Cast LOAD (rollover); awaited on teardown before media STOP.
  inflight_load: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
  /// Earliest instant at which AirPlay pause/flush may Cast-PAUSE this session.
  ///
  /// Set to `now + PAUSE_GRACE` when the session becomes active after a successful LOAD so
  /// stale idle events queued during the blocking start path cannot pause a fresh load.
  pause_eligible_at: Instant,
}

/// Why a queued AirPlay pause must not drive Cast PAUSE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PauseSkipReason {
  /// Session just became active; idle events from the load window are still in flight.
  WithinGrace,
  /// Ring still holds substantial PCM (playback / prebuffer, not a true idle).
  RingHasPcm,
}

impl PauseSkipReason {
  const fn as_str(self) -> &'static str {
    match self {
      Self::WithinGrace => "grace",
      Self::RingHasPcm => "ring_has_pcm",
    }
  }
}

/// Decide whether an AirPlay pause/flush event should pause Cast media.
///
/// Returns `Ok(())` to pause, or `Err(reason)` when the event is stale or false idle.
///
/// Uses `std::result::Result` so the crate [`Result`] alias (`Error`) is not involved.
fn should_pause_cast(
  now: Instant,
  pause_eligible_at: Instant,
  ring_frames: usize,
  ring_pcm_threshold: usize,
) -> std::result::Result<(), PauseSkipReason> {
  // Active/buffered PCM wins over grace: if audio is still in the ring, do not pause.
  if ring_frames > ring_pcm_threshold {
    return Err(PauseSkipReason::RingHasPcm);
  }
  if now < pause_eligible_at {
    return Err(PauseSkipReason::WithinGrace);
  }
  Ok(())
}

/// Ordered teardown steps for an active bridge session.
///
/// [`Bridge::handle_session_end`] always runs these in order: media HTTP first,
/// then timed best-effort Cast STOP. Media must not wait on STOP success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEndStep {
  /// Shut down the local `LiveWav` HTTP server (stop underrun immediately).
  MediaShutdown,
  /// Best-effort Cast STOP with timeout (may fail or time out).
  CastStopBestEffort,
}

/// Shipped session-end order (media first, then Cast STOP).
pub const fn session_end_steps() -> [SessionEndStep; 2] {
  [SessionEndStep::MediaShutdown, SessionEndStep::CastStopBestEffort]
}

/// Orchestrates media HTTP + Cast load for AirPlay lifecycle events.
pub struct Bridge {
  registry: Arc<DeviceRegistry>,
  cast_pool: Arc<CastPool>,
  /// Optional AirPlay manager for seeding `GET_PARAMETER volume` from Cast after LOAD.
  airplay: Option<Arc<AirPlayManager>>,
  sessions: Mutex<HashMap<String, ActiveSession>>,
  /// Optional barrier waited once at the start of each `handle_session_start`.
  ///
  /// Tests use this to prove multi-device starts run concurrently (no HOL blocking).
  start_barrier: Mutex<Option<Arc<tokio::sync::Barrier>>>,
}

impl std::fmt::Debug for Bridge {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Bridge")
      .field("registry", &self.registry)
      .field("cast_pool", &self.cast_pool)
      .field("airplay", &self.airplay.is_some())
      .field("active_sessions", &self.sessions.lock().len())
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
      sessions: Mutex::new(HashMap::new()),
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
  #[must_use]
  pub fn has_session(&self, device_id: &str) -> bool {
    self.sessions.lock().contains_key(device_id)
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
    &self,
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

    // Tear down any previous session for this device first.
    self.handle_session_end(device_id).await;

    let device = self
      .registry
      .get(device_id)
      .ok_or_else(|| Error::Bridge(format!("unknown device {device_id}")))?;

    // The ring comes from the same `audio_init` that emitted this event; its
    // layout is what the WAV header advertises.
    let stream_channels = ring.channels().max(1);
    let stream_rate = sample_rate.max(1);

    if !wait_for_prebuffer(device_id, &ring, rings.as_ref()).await? {
      return Ok(());
    }

    verify_flac_snapshot(&ring, stream_channels, stream_rate);

    if !is_current(&ring) {
      tracing::info!(%device_id, "session restarted before Cast load; skipping stale start");
      return Ok(());
    }

    self
      .start_cast_session(device_id, &device, ring, stream_channels, stream_rate)
      .await
  }

  async fn start_cast_session(
    &self,
    device_id: &str,
    device: &crate::registry::Device,
    ring: Arc<PcmRing>,
    stream_channels: u16,
    stream_rate: u32,
  ) -> Result<()> {
    // Route media URL via the interface that can reach this Cast device.
    let host = advertise_host_for_peer(&device.host);
    let media = MediaServer::start(&host).await?;
    let stream_url = media.stream_url();
    tracing::info!(
      %device_id,
      cast = %device.host,
      %stream_url,
      frames = ring.available_frames(),
      "starting Cast progressive WAV bridge"
    );

    // Keep a ring handle for pause re-validation after LOAD; LiveWav also holds Arc.
    let session_ring = Arc::clone(&ring);
    media.set_content(MediaContent::LiveWav {
      ring,
      channels: stream_channels,
      sample_rate: stream_rate,
    });

    // BUFFERED progressive file works on Nest/Home; LIVE often sits silent.
    // LOAD on the warm Cast control plane (no new TCP unless worker reconnects).
    let pool = Arc::clone(&self.cast_pool);
    let load_device_id = device_id.to_owned();
    let cast_name = device.name.clone();
    let load_url = stream_url.clone();
    let load_result = tokio::task::spawn_blocking(move || {
      cast_load_buffered_wav(&pool, &load_device_id, load_url, cast_name, LoadVolumePolicy::PreserveDevice)
    })
    .await
    .map_err(|err| Error::Bridge(format!("Cast load task join: {err}")))?;

    match load_result {
      Ok(session) => {
        tracing::info!(
          %device_id,
          cast = %device.host,
          transport_id = %session.transport_id,
          media_session_id = session.media_session_id,
          %stream_url,
          "bridge session Cast BUFFERED WAV load ok"
        );
        // Nest remains source of truth on LOAD (PreserveDevice). Seed AirPlay reported
        // volume + rollover last_volume from the device so the iOS slider matches.
        let cast_linear = self.sync_reported_volume_after_load(device_id).await;
        let session_alive = Arc::new(AtomicBool::new(true));
        let last_volume_linear = Arc::new(Mutex::new(cast_linear));
        let inflight_load = Arc::new(Mutex::new(None));
        let (rollover_cancel, rollover_task) = spawn_rollover_reload_loop(
          device_id.to_owned(),
          stream_url,
          device.name.clone(),
          Arc::clone(&self.cast_pool),
          media.rollover_signal(),
          Arc::clone(&session_alive),
          Arc::clone(&last_volume_linear),
          Arc::clone(&inflight_load),
        );
        let pause_eligible_at = Instant::now() + PAUSE_GRACE;
        let ring_frames = session_ring.available_frames();
        {
          let mut guard = self.sessions.lock();
          drop(guard.insert(
            device_id.to_owned(),
            ActiveSession {
              media,
              device_id: device_id.to_owned(),
              pool: Arc::clone(&self.cast_pool),
              ring: session_ring,
              rollover_cancel: Some(rollover_cancel),
              rollover_task: Some(rollover_task),
              session_alive,
              last_volume_linear,
              inflight_load,
              pause_eligible_at,
            },
          ));
        }
        // LOAD starts PLAYING; if the ring still has PCM, a stale pause queued during the
        // blocking start path must not win. Defensive PLAY is unnecessary when the ring is
        // empty (true underrun / client already idle).
        if ring_frames > PAUSE_RING_PCM_THRESHOLD {
          tracing::debug!(
            %device_id,
            ring_frames,
            grace_ms = PAUSE_GRACE.as_millis(),
            "Cast session active; pause grace armed (ring has PCM)"
          );
        }
        Ok(())
      },
      Err(err) => {
        media.shutdown();
        tracing::warn!(%device_id, error = %err, "Cast load failed (device may be offline)");
        Err(err)
      },
    }
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

  async fn handle_session_end(&self, device_id: &str) {
    let removed = {
      let mut guard = self.sessions.lock();
      guard.remove(device_id)
    };
    let Some(active) = removed else {
      return;
    };

    // Mark dead first so an in-flight or just-finishing LOAD will STOP.
    active.session_alive.store(false, Ordering::Release);

    // Stop scheduling new re-LOADs. Join the rollover task *before* taking
    // `inflight_load` so a spawn that was about to publish either published or
    // the task died before publish — never leave a detached blocking LOAD.
    if let Some(tx) = active.rollover_cancel {
      let _cancelled = tx.send(());
    }
    if let Some(task) = active.rollover_task {
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
    let inflight = active.inflight_load.lock().take();
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

    // Cast STOP can block up to its timeout; keep it off the runtime thread.
    let media = active.media;
    let pool = active.pool;
    let end_device_id = active.device_id;
    if tokio::task::spawn_blocking(move || end_media_and_cast_stop(media, &pool, &end_device_id))
      .await
      .is_err()
    {
      tracing::warn!(%device_id, "session teardown task panicked");
    }
    tracing::info!(%device_id, "bridge session ended (media dropped; Cast STOP best-effort)");
  }

  async fn handle_volume(&self, device_id: &str, volume_db: f32) {
    let level = airplay_db_to_cast_linear(volume_db);
    // Volume only applies while a bridge session is active for this device.
    let has_session = {
      let guard = self.sessions.lock();
      guard.get(device_id).is_some_and(|session| {
        *session.last_volume_linear.lock() = Some(level);
        true
      })
    };
    if !has_session {
      return;
    }
    // Await so Volume stays ordered with Started/Ended on this device worker.
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

  /// Pause Cast media immediately (AirPlay rate=0 / flush). Keeps HTTP + session warm.
  ///
  /// Stale idle `Paused` events queued while start blocked on Cast LOAD are dropped when
  /// still inside [`PAUSE_GRACE`] or when the session ring still holds PCM.
  async fn handle_pause(&self, device_id: &str) {
    let decision = {
      let guard = self.sessions.lock();
      let Some(session) = guard.get(device_id) else {
        return;
      };
      let pause_eligible_at = session.pause_eligible_at;
      let ring_frames = session.ring.available_frames();
      drop(guard);
      should_pause_cast(Instant::now(), pause_eligible_at, ring_frames, PAUSE_RING_PCM_THRESHOLD)
    };
    if let Err(reason) = decision {
      tracing::info!(
        %device_id,
        reason = reason.as_str(),
        "skipping Cast pause (stale idle or false idle)"
      );
      return;
    }
    tracing::info!(%device_id, "AirPlay paused; pausing Cast media");
    let pool = Arc::clone(&self.cast_pool);
    let id = device_id.to_owned();
    match tokio::task::spawn_blocking(move || pool.pause(&id)).await {
      Ok(Ok(())) => {},
      Ok(Err(err)) => {
        tracing::debug!(device_id = %device_id, error = %err, "Cast pause failed");
      },
      Err(err) => {
        tracing::debug!(device_id = %device_id, error = %err, "Cast pause task join failed");
      },
    }
  }

  /// Resume Cast media after AirPlay playout restarts.
  async fn handle_resume(&self, device_id: &str) {
    if !self.sessions.lock().contains_key(device_id) {
      return;
    }
    tracing::info!(%device_id, "AirPlay resumed; resuming Cast media");
    let pool = Arc::clone(&self.cast_pool);
    let id = device_id.to_owned();
    match tokio::task::spawn_blocking(move || pool.play(&id)).await {
      Ok(Ok(())) => {},
      Ok(Err(err)) => {
        tracing::debug!(device_id = %device_id, error = %err, "Cast play failed");
      },
      Err(err) => {
        tracing::debug!(device_id = %device_id, error = %err, "Cast play task join failed");
      },
    }
  }
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
        bridge.handle_session_end(&event_device).await;
      },
      AirPlaySessionEvent::Paused { device_id: event_device } => {
        bridge.handle_pause(&event_device).await;
      },
      AirPlaySessionEvent::Resumed { device_id: event_device } => {
        bridge.handle_resume(&event_device).await;
      },
      AirPlaySessionEvent::Flushed { device_id: event_device } => {
        // Ring already cleared; pause Cast so Nest does not play stale buffer.
        bridge.handle_pause(&event_device).await;
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

/// Cast LOAD of buffered progressive WAV (shared by initial start and rollover re-LOAD).
fn cast_load_buffered_wav(
  pool: &CastPool,
  device_id: &str,
  stream_url: String,
  title: String,
  volume: LoadVolumePolicy,
) -> Result<crate::cast::MediaSessionRef> {
  let request = MediaLoadRequest::wav(stream_url, CastStreamKind::Buffered).with_title(title);
  let session = pool.load(device_id, request)?;
  if let Some(level) = volume_after_load(volume)
    && let Err(err) = pool.set_volume(device_id, level)
  {
    tracing::debug!(error = %err, "post-load Cast volume set failed");
  }
  Ok(session)
}

/// Spawn a task that re-LOADs the same stream URL each time `LiveWav` hits its body cap.
#[expect(
  clippy::too_many_arguments,
  reason = "rollover loop needs session liveness, volume, and inflight load slots"
)]
fn spawn_rollover_reload_loop(
  device_id: String,
  stream_url: String,
  cast_name: String,
  pool: Arc<CastPool>,
  rollover: Arc<RolloverSignal>,
  session_alive: Arc<AtomicBool>,
  last_volume_linear: Arc<Mutex<Option<f32>>>,
  inflight_load: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
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
              let result = cast_load_buffered_wav(
                &load_pool,
                &id,
                url,
                title,
                LoadVolumePolicy::Rollover { last_volume },
              );
              // Late LOAD after teardown: stop so playback cannot revive against a dead HTTP server.
              if !alive.load(Ordering::Acquire) {
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

/// Run media shutdown then Cast STOP (blocking; call from `spawn_blocking`).
fn end_media_and_cast_stop(media: MediaServerHandle, pool: &CastPool, device_id: &str) {
  let mut media_handle = Some(media);
  for step in session_end_steps() {
    match step {
      SessionEndStep::MediaShutdown => {
        // Always stop LiveWav HTTP first so underrun ends even if Cast STOP hangs.
        if let Some(handle) = media_handle.take() {
          handle.shutdown();
        }
      },
      SessionEndStep::CastStopBestEffort => {
        // Best-effort Cast STOP with timeout; keep warm TCP for the next play.
        pool.stop_best_effort(device_id, Duration::from_secs(2));
      },
    }
  }
}

/// Non-destructive FLAC snapshot encode to keep the lossless quality path warm.
fn verify_flac_snapshot(ring: &PcmRing, channels: u16, sample_rate: u32) {
  let mut snap = Vec::new();
  let frames = ring.copy_i16(SNAPSHOT_FRAMES, &mut snap);
  if frames == 0 {
    tracing::debug!("FLAC snapshot skipped: empty ring");
    return;
  }
  match encode_pcm_i16_to_flac(&snap, channels, sample_rate) {
    Ok(flac) => {
      tracing::info!(bytes = flac.len(), frames, "session FLAC snapshot ok");
    },
    Err(err) => {
      tracing::warn!(error = %err, "session FLAC snapshot encode failed");
    },
  }
}

/// Pop up to `max_frames` from `ring` and encode a finite FLAC body.
pub fn encode_session_snapshot_flac(
  ring: &PcmRing,
  channels: u16,
  sample_rate: u32,
  max_frames: usize,
) -> Result<Vec<u8>> {
  let mut i16_buf = Vec::new();
  let frames = ring.pop_i16(max_frames, &mut i16_buf);
  if frames == 0 {
    return Err(Error::Bridge("no PCM for FLAC snapshot".to_owned()));
  }
  encode_pcm_i16_to_flac(&i16_buf, channels.max(1), sample_rate.max(1))
}

/// Build a static FLAC [`MediaContent`] from raw FLAC bytes (test / secondary helper).
pub fn static_flac_content(flac: Vec<u8>) -> MediaContent {
  MediaContent::Static {
    content_type: "audio/flac".to_owned(),
    body: Bytes::from(flac),
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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::registry::Device;
  use std::time::{Duration, Instant};

  /// Minimal active session for teardown / pause tests (no live Cast worker).
  fn test_active_session(
    media: MediaServerHandle,
    device_id: &str,
    pool: Arc<CastPool>,
    ring: Arc<PcmRing>,
    pause_eligible_at: Instant,
  ) -> ActiveSession {
    ActiveSession {
      media,
      device_id: device_id.to_owned(),
      pool,
      ring,
      rollover_cancel: None,
      rollover_task: None,
      session_alive: Arc::new(AtomicBool::new(true)),
      last_volume_linear: Arc::new(Mutex::new(None)),
      inflight_load: Arc::new(Mutex::new(None)),
      pause_eligible_at,
    }
  }

  #[test]
  fn session_end_steps_media_before_cast_stop() {
    let steps = session_end_steps();
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0], SessionEndStep::MediaShutdown);
    assert_eq!(steps[1], SessionEndStep::CastStopBestEffort);
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
  fn should_pause_cast_skips_within_grace_when_ring_empty() {
    let now = Instant::now();
    let eligible = now + Duration::from_secs(2);
    assert_eq!(
      should_pause_cast(now, eligible, 0, PAUSE_RING_PCM_THRESHOLD),
      Err(PauseSkipReason::WithinGrace)
    );
  }

  #[test]
  fn should_pause_cast_skips_when_ring_has_pcm_even_after_grace() {
    let now = Instant::now();
    let eligible = now; // already eligible
    assert_eq!(
      should_pause_cast(now, eligible, PAUSE_RING_PCM_THRESHOLD + 1, PAUSE_RING_PCM_THRESHOLD),
      Err(PauseSkipReason::RingHasPcm)
    );
    // Ring check wins over grace (stale pause after load with prebuffer still present).
    let future_eligible = now + Duration::from_secs(2);
    assert_eq!(
      should_pause_cast(now, future_eligible, PAUSE_RING_PCM_THRESHOLD + 1, PAUSE_RING_PCM_THRESHOLD),
      Err(PauseSkipReason::RingHasPcm)
    );
  }

  #[test]
  fn should_pause_cast_allows_when_eligible_and_ring_drained() {
    let now = Instant::now();
    let eligible = now;
    assert_eq!(should_pause_cast(now, eligible, 0, PAUSE_RING_PCM_THRESHOLD), Ok(()));
    assert_eq!(
      should_pause_cast(now, eligible, PAUSE_RING_PCM_THRESHOLD, PAUSE_RING_PCM_THRESHOLD),
      Ok(())
    );
    // Exactly at eligible boundary is allowed (`now < eligible` is the skip).
    assert_eq!(should_pause_cast(eligible, eligible, 0, PAUSE_RING_PCM_THRESHOLD), Ok(()));
  }

  #[test]
  fn should_pause_cast_table() {
    let t0 = Instant::now();
    let cases = [
      // (now_offset_from_t0, eligible_offset, ring_frames, threshold, expected)
      (Duration::ZERO, PAUSE_GRACE, 0, 256, Err(PauseSkipReason::WithinGrace)),
      (PAUSE_GRACE, PAUSE_GRACE, 0, 256, Ok(())),
      (PAUSE_GRACE + Duration::from_millis(1), PAUSE_GRACE, 0, 256, Ok(())),
      (Duration::ZERO, PAUSE_GRACE, 257, 256, Err(PauseSkipReason::RingHasPcm)),
      (PAUSE_GRACE, PAUSE_GRACE, 257, 256, Err(PauseSkipReason::RingHasPcm)),
      (PAUSE_GRACE, PAUSE_GRACE, 256, 256, Ok(())),
      (PAUSE_GRACE, PAUSE_GRACE, 1, 0, Err(PauseSkipReason::RingHasPcm)),
      (PAUSE_GRACE, PAUSE_GRACE, 0, 0, Ok(())),
    ];
    for (i, (now_off, elig_off, frames, threshold, expected)) in cases.iter().enumerate() {
      let now = t0 + *now_off;
      let eligible = t0 + *elig_off;
      assert_eq!(
        should_pause_cast(now, eligible, *frames, *threshold),
        *expected,
        "case {i}: now_off={now_off:?} elig_off={elig_off:?} frames={frames} thr={threshold}"
      );
    }
    // Reason strings are stable for HA log grepping.
    assert_eq!(PauseSkipReason::WithinGrace.as_str(), "grace");
    assert_eq!(PauseSkipReason::RingHasPcm.as_str(), "ring_has_pcm");
  }

  #[tokio::test]
  async fn handle_pause_skips_within_grace_without_cast_call() {
    let registry = Arc::new(DeviceRegistry::new());
    let pool = Arc::new(CastPool::new(None));
    let bridge = Bridge::new(registry, Arc::clone(&pool));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let ring = Arc::new(PcmRing::new(2, 64));

    {
      let mut guard = bridge.sessions.lock();
      drop(guard.insert(
        "dev-grace".to_owned(),
        test_active_session(media, "dev-grace", Arc::clone(&pool), ring, Instant::now() + PAUSE_GRACE),
      ));
    }

    // Empty ring + within grace → skip (no panic; no warm worker so pause would no-op anyway).
    bridge.handle_pause("dev-grace").await;
    assert!(bridge.sessions.lock().contains_key("dev-grace"));
    bridge.handle_session_end("dev-grace").await;
  }

  #[tokio::test]
  async fn handle_pause_skips_when_ring_has_pcm() {
    let registry = Arc::new(DeviceRegistry::new());
    let pool = Arc::new(CastPool::new(None));
    let bridge = Bridge::new(registry, Arc::clone(&pool));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let ring = Arc::new(PcmRing::new(2, 1024));
    // More than PAUSE_RING_PCM_THRESHOLD complete stereo frames.
    let samples = vec![0.01_f32; (PAUSE_RING_PCM_THRESHOLD + 10) * 2];
    ring.push_f32(&samples);
    assert!(ring.available_frames() > PAUSE_RING_PCM_THRESHOLD);

    {
      let mut guard = bridge.sessions.lock();
      drop(guard.insert(
        "dev-pcm".to_owned(),
        test_active_session(
          media,
          "dev-pcm",
          Arc::clone(&pool),
          Arc::clone(&ring),
          Instant::now(), // already eligible; ring must still block pause
        ),
      ));
    }

    bridge.handle_pause("dev-pcm").await;
    assert!(bridge.sessions.lock().contains_key("dev-pcm"));
    bridge.handle_session_end("dev-pcm").await;
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

    {
      let mut guard = bridge.sessions.lock();
      drop(guard.insert(
        "dev-1".to_owned(),
        test_active_session(media, "dev-1", Arc::clone(&pool), Arc::new(PcmRing::new(2, 64)), Instant::now()),
      ));
    }

    let start = Instant::now();
    // Shipped path: session_end_steps() → MediaShutdown then timed CastStopBestEffort.
    // No warm worker → STOP is an immediate no-op (must not hang).
    bridge.handle_session_end("dev-1").await;
    let elapsed = start.elapsed();
    assert!(
      elapsed < Duration::from_secs(4),
      "session end must not hang on Cast STOP; elapsed={elapsed:?}"
    );
    assert!(bridge.sessions.lock().is_empty(), "session removed from map");
    // Media HTTP must already be down (MediaShutdown ran first / independently).
    assert!(
      !http_get_status_ok(&health_url).await,
      "media.shutdown must run on session end so LiveWav stops"
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
    // Task that only exits via cancel — proves session end stops the loop.
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

    {
      let mut guard = bridge.sessions.lock();
      drop(guard.insert(
        "dev-roll".to_owned(),
        ActiveSession {
          media,
          device_id: "dev-roll".to_owned(),
          pool,
          ring: Arc::new(PcmRing::new(2, 64)),
          rollover_cancel: Some(cancel_tx),
          rollover_task: Some(task),
          session_alive: Arc::new(AtomicBool::new(true)),
          last_volume_linear: Arc::new(Mutex::new(None)),
          inflight_load: Arc::new(Mutex::new(None)),
          pause_eligible_at: Instant::now(),
        },
      ));
    }

    bridge.handle_session_end("dev-roll").await;
    assert!(bridge.sessions.lock().is_empty());
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

    // Simulate a slow blocking LOAD that observes session_alive after "LOAD".
    let handle = tokio::task::spawn_blocking(move || {
      started_flag.store(true, Ordering::Release);
      std::thread::sleep(Duration::from_millis(200));
      let still_alive = alive_for_load.load(Ordering::Acquire);
      // Teardown should have cleared the flag before we finish.
      assert!(!still_alive, "session must be marked dead before inflight load finishes join");
    });
    *inflight_load.lock() = Some(handle);

    {
      let mut guard = bridge.sessions.lock();
      drop(guard.insert(
        "dev-inflight".to_owned(),
        ActiveSession {
          media,
          device_id: "dev-inflight".to_owned(),
          pool,
          ring: Arc::new(PcmRing::new(2, 64)),
          rollover_cancel: None,
          rollover_task: None,
          session_alive: Arc::clone(&session_alive),
          last_volume_linear: Arc::new(Mutex::new(None)),
          inflight_load: Arc::clone(&inflight_load),
          pause_eligible_at: Instant::now(),
        },
      ));
    }

    // Wait until the blocking load has started so teardown has something to join.
    let wait_start = Instant::now();
    while !started.load(Ordering::Acquire) && wait_start.elapsed() < Duration::from_secs(2) {
      sleep(Duration::from_millis(5)).await;
    }
    assert!(started.load(Ordering::Acquire), "inflight load must start");

    bridge.handle_session_end("dev-inflight").await;
    assert!(!session_alive.load(Ordering::Acquire));
    assert!(inflight_load.lock().is_none(), "inflight load handle must be taken and joined");
    assert!(bridge.sessions.lock().is_empty());
  }

  /// Rollover parks on cancel while a blocking LOAD sits in `inflight_load`.
  /// Teardown must abort+join the async task, then take and join the LOAD handle
  /// (not drop it on a short timeout).
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
      // Mirror production: publish blocking LOAD, then await (result / cancel).
      let done = Arc::clone(&finished_flag);
      let handle = tokio::task::spawn_blocking(move || {
        std::thread::sleep(Duration::from_millis(200));
        done.store(true, Ordering::Release);
      });
      *inflight_for_task.lock() = Some(handle);
      // Park until cancel or abort (like `result_rx.await` mid-LOAD).
      match cancel_rx.await {
        Ok(()) | Err(_) => {},
      }
    });

    {
      let mut guard = bridge.sessions.lock();
      drop(guard.insert(
        "dev-join-order".to_owned(),
        ActiveSession {
          media,
          device_id: "dev-join-order".to_owned(),
          pool,
          ring: Arc::new(PcmRing::new(2, 64)),
          rollover_cancel: Some(cancel_tx),
          rollover_task: Some(task),
          session_alive: Arc::clone(&session_alive),
          last_volume_linear: Arc::new(Mutex::new(None)),
          inflight_load: Arc::clone(&inflight_load),
          pause_eligible_at: Instant::now(),
        },
      ));
    }

    // Ensure the rollover task has published before teardown.
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
    assert!(bridge.sessions.lock().is_empty());
  }

  #[test]
  fn inflight_load_join_timeout_covers_cast_command_timeout() {
    // Cast pool COMMAND_TIMEOUT is 20s; teardown must not drop the JoinHandle earlier.
    assert!(
      INFLIGHT_LOAD_JOIN_TIMEOUT >= Duration::from_secs(25),
      "INFLIGHT_LOAD_JOIN_TIMEOUT={INFLIGHT_LOAD_JOIN_TIMEOUT:?} must be >= 25s (Cast cmd 20s + margin)"
    );
  }

  #[tokio::test]
  async fn rollover_signal_invokes_reload_path_without_panic() {
    // Exercise the shared LOAD helper + rollover wait wiring without a Cast device.
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
        cast_load_buffered_wav(
          &load_pool,
          "missing-device",
          "http://127.0.0.1:9/stream".to_owned(),
          "test".to_owned(),
          LoadVolumePolicy::Rollover { last_volume: None },
        )
      })
      .await;
      // No worker → load errors; must not panic.
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

    {
      let mut guard = bridge.sessions.lock();
      drop(guard.insert(
        "dev-vol".to_owned(),
        ActiveSession {
          media,
          device_id: "dev-vol".to_owned(),
          pool,
          ring: Arc::new(PcmRing::new(2, 64)),
          rollover_cancel: None,
          rollover_task: None,
          session_alive: Arc::new(AtomicBool::new(true)),
          last_volume_linear: Arc::clone(&last_volume_linear),
          inflight_load: Arc::new(Mutex::new(None)),
          pause_eligible_at: Instant::now(),
        },
      ));
    }

    // AirPlay 0 dB → Cast linear 1.0 (see airplay_db_to_cast_linear).
    bridge.handle_volume("dev-vol", 0.0).await;
    let stored = *last_volume_linear.lock();
    assert_eq!(stored, Some(1.0));
    // Initial load must not force full volume; rollover re-applies last AirPlay level.
    assert_eq!(volume_after_load(LoadVolumePolicy::PreserveDevice), None);
    assert_eq!(volume_after_load(LoadVolumePolicy::Rollover { last_volume: stored }), Some(1.0));
    assert_eq!(
      volume_after_load(LoadVolumePolicy::Rollover { last_volume: Some(0.25) }),
      Some(0.25)
    );

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
    let bridge = Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None)));
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
    assert!(bridge.sessions.lock().is_empty());
  }

  #[tokio::test]
  async fn session_start_with_receiver_gone_skips() {
    let bridge = Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None)));
    let event_ring = Arc::new(PcmRing::new(2, 64));
    let rings: Arc<dyn RingLookup> = Arc::new(FixedRingLookup { current: None });

    bridge
      .handle_session_start("dev-1", 48_000, event_ring, rings)
      .await
      .expect("withdrawn receiver skips cleanly");
    assert!(bridge.sessions.lock().is_empty());
  }

  #[tokio::test]
  async fn multi_device_session_starts_do_not_serialize() {
    // Barrier parties: two device workers + this test. If starts serialize, the
    // second worker never reaches the barrier and this wait times out.
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
    // Keep the channel open so run stays in recv; abort must still tear down workers.
    let run = tokio::spawn({
      let bridge_for_run = Arc::clone(&bridge);
      async move {
        bridge_for_run.run(rx, rings).await;
      }
    });

    // Force a worker to exist by sending a no-op end (no session).
    tx.send(AirPlaySessionEvent::Ended { device_id: "dev-zombie".to_owned() })
      .expect("send");
    sleep(Duration::from_millis(50)).await;

    run.abort();
    let result = tokio::time::timeout(Duration::from_secs(2), run).await;
    assert!(result.is_ok(), "aborted bridge run must finish promptly");
    // Drop sender after abort so we do not keep the test process holding the channel.
    drop(tx);
  }

  #[test]
  fn encode_session_snapshot_flac_roundtrip() {
    let ring = PcmRing::new(2, 8192);
    let mut samples = Vec::with_capacity(4096);
    for n in 0..2048 {
      let t = n as f32 / 48_000.0;
      let s = (2.0 * std::f32::consts::PI * 440.0 * t).sin() * 0.4;
      samples.push(s);
      samples.push(s);
    }
    ring.push_f32(&samples);

    let flac = encode_session_snapshot_flac(&ring, 2, 48_000, 2048).expect("snapshot");
    assert!(flac.len() > 42);
    assert_eq!(&flac[0..4], b"fLaC");

    let content = static_flac_content(flac);
    match content {
      MediaContent::Static { content_type, body } => {
        assert_eq!(content_type, "audio/flac");
        assert!(body.len() > 42);
      },
      MediaContent::LiveWav { .. } | MediaContent::LiveFlac { .. } | MediaContent::Empty => {
        panic!("expected Static media content");
      },
    }
  }

  #[test]
  fn encode_session_snapshot_empty_errors() {
    let ring = PcmRing::new(2, 64);
    let err = encode_session_snapshot_flac(&ring, 2, 48_000, 128).unwrap_err();
    assert!(matches!(err, Error::Bridge(_)));
  }
}
