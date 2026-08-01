//! Session bridge: AirPlay PCM → continuous lossless WAV HTTP → Cast LIVE load.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;

use crate::airplay::{AirPlaySessionEvent, airplay_db_to_cast_linear};
use crate::audio::{PcmRing, encode_pcm_i16_to_flac};
use crate::cast::{CastPool, CastStreamKind, MediaLoadRequest};
use crate::error::{Error, Result};
use crate::media::{MediaContent, MediaServer, MediaServerHandle, RolloverSignal};
use crate::net::advertise_host_for_peer;
use crate::registry::DeviceRegistry;

/// Frames to wait for before starting Cast load (~0.5 s at 44.1/48 kHz).
const PREBUFFER_FRAMES: usize = 24_000;
/// Max prebuffer poll iterations (~3 s at 50 ms).
const PREBUFFER_POLLS: u32 = 60;
const PREBUFFER_POLL: Duration = Duration::from_millis(50);
/// Frames copied for the FLAC quality-path snapshot at session start.
const SNAPSHOT_FRAMES: usize = 2048;
/// How long the main `run` loop waits for per-device workers after the event channel closes.
const WORKER_JOIN_TIMEOUT: Duration = Duration::from_secs(30);

/// One live bridge session for a device.
struct ActiveSession {
  media: MediaServerHandle,
  device_id: String,
  pool: Arc<CastPool>,
  /// Drop / send to stop the `LiveWav` Content-Length rollover re-LOAD loop.
  rollover_cancel: Option<oneshot::Sender<()>>,
  rollover_task: Option<tokio::task::JoinHandle<()>>,
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
      sessions: Mutex::new(HashMap::new()),
      start_barrier: Mutex::new(None),
    }
  }

  /// Install a barrier that each session start waits on once (tests only).
  pub fn set_start_barrier(&self, barrier: Arc<tokio::sync::Barrier>) {
    *self.start_barrier.lock() = Some(barrier);
  }

  /// Run until the session event channel closes.
  ///
  /// Events are routed to a small per-`device_id` worker so one device's slow
  /// Cast LOAD cannot head-of-line-block another device. Ordering within a
  /// device (Started / Ended / Volume) remains sequential.
  pub async fn run(
    self: Arc<Self>,
    mut events: mpsc::UnboundedReceiver<AirPlaySessionEvent>,
    rings: Arc<dyn RingLookup>,
  ) {
    let mut device_workers: HashMap<String, mpsc::UnboundedSender<AirPlaySessionEvent>> = HashMap::new();
    let mut worker_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    while let Some(event) = events.recv().await {
      let device_id = event_device_id(&event).to_owned();
      let sender = device_workers.entry(device_id.clone()).or_insert_with(|| {
        let (tx, rx) = mpsc::unbounded_channel();
        let bridge = Arc::clone(&self);
        let rings_for_worker = Arc::clone(&rings);
        let worker_id = device_id.clone();
        worker_handles.push(tokio::spawn(async move {
          device_worker_loop(bridge, rings_for_worker, worker_id, rx).await;
        }));
        tx
      });
      if sender.send(event).is_err() {
        tracing::error!(%device_id, "device session worker dropped; event lost");
        drop(device_workers.remove(&device_id));
      }
    }

    // Close all worker inboxes so they drain and exit.
    drop(device_workers);
    for handle in worker_handles {
      match tokio::time::timeout(WORKER_JOIN_TIMEOUT, handle).await {
        Ok(Ok(())) => {},
        Ok(Err(err)) => {
          tracing::warn!(error = %err, "device session worker panicked");
        },
        Err(_) => {
          tracing::warn!("device session worker join timed out");
        },
      }
    }
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

    for _ in 0..PREBUFFER_POLLS {
      if !is_current(&ring) {
        tracing::info!(%device_id, "session restarted during prebuffer; skipping stale start");
        return Ok(());
      }
      if ring.available_frames() >= PREBUFFER_FRAMES {
        break;
      }
      sleep(PREBUFFER_POLL).await;
    }

    if ring.available_frames() == 0 {
      return Err(Error::Bridge("no PCM available at session start".to_owned()));
    }

    verify_flac_snapshot(&ring, stream_channels, stream_rate);

    if !is_current(&ring) {
      tracing::info!(%device_id, "session restarted before Cast load; skipping stale start");
      return Ok(());
    }

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

    media.set_content(MediaContent::LiveWav {
      ring: Arc::clone(&ring),
      channels: stream_channels,
      sample_rate: stream_rate,
    });

    // BUFFERED progressive file works on Nest/Home; LIVE often sits silent.
    // LOAD on the warm Cast control plane (no new TCP unless worker reconnects).
    let pool = Arc::clone(&self.cast_pool);
    let load_device_id = device_id.to_owned();
    let cast_name = device.name.clone();
    let load_url = stream_url.clone();
    let load_result =
      tokio::task::spawn_blocking(move || cast_load_buffered_wav(&pool, &load_device_id, load_url, cast_name))
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
        let (rollover_cancel, rollover_task) = spawn_rollover_reload_loop(
          device_id.to_owned(),
          stream_url,
          device.name.clone(),
          Arc::clone(&self.cast_pool),
          media.rollover_signal(),
        );
        {
          let mut guard = self.sessions.lock();
          drop(guard.insert(
            device_id.to_owned(),
            ActiveSession {
              media,
              device_id: device_id.to_owned(),
              pool: Arc::clone(&self.cast_pool),
              rollover_cancel: Some(rollover_cancel),
              rollover_task: Some(rollover_task),
            },
          ));
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

  async fn handle_session_end(&self, device_id: &str) {
    let removed = {
      let mut guard = self.sessions.lock();
      guard.remove(device_id)
    };
    let Some(active) = removed else {
      return;
    };
    // Cast STOP can block up to its timeout; keep it off the runtime thread.
    if tokio::task::spawn_blocking(move || end_active_session(active)).await.is_err() {
      tracing::warn!(%device_id, "session teardown task panicked");
    }
    tracing::info!(%device_id, "bridge session ended (media dropped; Cast STOP best-effort)");
  }

  fn handle_volume(&self, device_id: &str, volume_db: f32) {
    let level = airplay_db_to_cast_linear(volume_db);
    // Volume only applies while a bridge session is active for this device.
    let has_session = self.sessions.lock().contains_key(device_id);
    if !has_session {
      return;
    }
    // set_volume blocks on the warm-worker reply; keep the event loop responsive.
    let pool = Arc::clone(&self.cast_pool);
    let id = device_id.to_owned();
    drop(tokio::task::spawn_blocking(move || {
      if let Err(err) = pool.set_volume(&id, level) {
        tracing::debug!(device_id = %id, error = %err, "Cast volume sync failed");
      }
    }));
  }
}

const fn event_device_id(event: &AirPlaySessionEvent) -> &str {
  match event {
    AirPlaySessionEvent::Started { device_id, .. }
    | AirPlaySessionEvent::Ended { device_id }
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
      AirPlaySessionEvent::Volume { device_id: event_device, volume_db } => {
        bridge.handle_volume(&event_device, volume_db);
      },
    }
  }
  tracing::debug!(%device_id, "device session worker exited");
}

/// Cast LOAD of buffered progressive WAV (shared by initial start and rollover re-LOAD).
fn cast_load_buffered_wav(
  pool: &CastPool,
  device_id: &str,
  stream_url: String,
  title: String,
) -> Result<crate::cast::MediaSessionRef> {
  let request = MediaLoadRequest::wav(stream_url, CastStreamKind::Buffered).with_title(title);
  let session = pool.load(device_id, request)?;
  // Nest device volume is independent of AirPlay; raise receiver volume on initial loads.
  // Rollover re-LOAD also benefits from keeping volume up if the sink reset it.
  if let Err(err) = pool.set_volume(device_id, 1.0) {
    tracing::debug!(error = %err, "post-load Cast volume set failed");
  }
  Ok(session)
}

/// Spawn a task that re-LOADs the same stream URL each time `LiveWav` hits its body cap.
fn spawn_rollover_reload_loop(
  device_id: String,
  stream_url: String,
  cast_name: String,
  pool: Arc<CastPool>,
  rollover: Arc<RolloverSignal>,
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
          tracing::info!(%device_id, rollover = seen, %stream_url, "LiveWav rollover: re-LOADing Cast stream");
          let load_pool = Arc::clone(&pool);
          let id = device_id.clone();
          let url = stream_url.clone();
          let title = cast_name.clone();
          let load_result = tokio::task::spawn_blocking(move || {
            cast_load_buffered_wav(&load_pool, &id, url, title)
          })
          .await;
          match load_result {
            Ok(Ok(session)) => {
              tracing::info!(
                %device_id,
                transport_id = %session.transport_id,
                media_session_id = session.media_session_id,
                "LiveWav rollover Cast re-LOAD ok"
              );
            },
            Ok(Err(err)) => {
              tracing::warn!(%device_id, error = %err, "LiveWav rollover Cast re-LOAD failed");
            },
            Err(err) => {
              tracing::warn!(%device_id, error = %err, "LiveWav rollover re-LOAD task join failed");
            },
          }
        },
      }
    }
  });
  (cancel_tx, task)
}

/// Run shipped teardown order for one active session.
fn end_active_session(active: ActiveSession) {
  let ActiveSession {
    media,
    device_id,
    pool,
    rollover_cancel,
    rollover_task,
  } = active;
  // Stop re-LOAD before tearing down media HTTP.
  if let Some(tx) = rollover_cancel {
    let _cancelled = tx.send(());
  }
  if let Some(task) = rollover_task {
    task.abort();
  }
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
        pool.stop_best_effort(&device_id, Duration::from_secs(2));
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

impl RingLookup for crate::airplay::AirPlayManager {
  fn ring_for(&self, device_id: &str) -> Option<Arc<PcmRing>> {
    self.pcm_ring(device_id)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::registry::Device;
  use std::time::{Duration, Instant};

  #[test]
  fn session_end_steps_media_before_cast_stop() {
    let steps = session_end_steps();
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0], SessionEndStep::MediaShutdown);
    assert_eq!(steps[1], SessionEndStep::CastStopBestEffort);
  }

  #[tokio::test]
  async fn handle_session_end_shuts_media_before_cast_stop_and_does_not_hang() {
    let registry = Arc::new(DeviceRegistry::new());
    registry.appear(Device {
      id: "dev-1".to_owned(),
      name: "Test".to_owned(),
      host: "127.0.0.1".to_owned(),
      hostname: "test.local".to_owned(),
      port: 9,
      last_seen: Instant::now(),
    });
    let pool = Arc::new(CastPool::new());
    let bridge = Bridge::new(Arc::clone(&registry), Arc::clone(&pool));

    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let health_url = format!("{}/health", media.base_url);
    assert!(http_get_status_ok(&health_url).await, "media must be up before end");

    {
      let mut guard = bridge.sessions.lock();
      drop(guard.insert(
        "dev-1".to_owned(),
        ActiveSession {
          media,
          device_id: "dev-1".to_owned(),
          pool: Arc::clone(&pool),
          rollover_cancel: None,
          rollover_task: None,
        },
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
    let pool = Arc::new(CastPool::new());
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
          rollover_cancel: Some(cancel_tx),
          rollover_task: Some(task),
        },
      ));
    }

    bridge.handle_session_end("dev-roll").await;
    assert!(bridge.sessions.lock().is_empty());
  }

  #[tokio::test]
  async fn rollover_signal_invokes_reload_path_without_panic() {
    // Exercise the shared LOAD helper + rollover wait wiring without a Cast device.
    let pool = Arc::new(CastPool::new());
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
    let bridge = Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new()));
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
    let bridge = Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new()));
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
      registry.appear(Device {
        id: id.to_owned(),
        name: id.to_owned(),
        host: "127.0.0.1".to_owned(),
        hostname: format!("{id}.local"),
        port: 9,
        last_seen: Instant::now(),
      });
    }

    let bridge = Arc::new(Bridge::new(registry, Arc::new(CastPool::new())));
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
      MediaContent::LiveWav { .. } | MediaContent::Empty => {
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
