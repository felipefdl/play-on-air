//! Session bridge: AirPlay PCM → continuous lossless WAV HTTP → Cast LIVE load.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::airplay::{AirPlaySessionEvent, airplay_db_to_cast_linear};
use crate::audio::{PcmRing, encode_pcm_i16_to_flac};
use crate::cast::{CastPool, CastStreamKind, MediaLoadRequest};
use crate::error::{Error, Result};
use crate::media::{MediaContent, MediaServer, MediaServerHandle};
use crate::net::advertise_host_for_peer;
use crate::registry::DeviceRegistry;

/// Frames to wait for before starting Cast load (~0.5 s at 44.1/48 kHz).
const PREBUFFER_FRAMES: usize = 24_000;
/// Max prebuffer poll iterations (~3 s at 50 ms).
const PREBUFFER_POLLS: u32 = 60;
const PREBUFFER_POLL: Duration = Duration::from_millis(50);
/// Frames copied for the FLAC quality-path snapshot at session start.
const SNAPSHOT_FRAMES: usize = 2048;

/// One live bridge session for a device.
struct ActiveSession {
  media: MediaServerHandle,
  device_id: String,
  pool: Arc<CastPool>,
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
}

impl std::fmt::Debug for Bridge {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Bridge")
      .field("registry", &self.registry)
      .field("cast_pool", &self.cast_pool)
      .field("active_sessions", &self.sessions.lock().len())
      .finish()
  }
}

impl Bridge {
  /// Create a bridge over the shared device registry and warm Cast pool.
  pub fn new(registry: Arc<DeviceRegistry>, cast_pool: Arc<CastPool>) -> Self {
    Self {
      registry,
      cast_pool,
      sessions: Mutex::new(HashMap::new()),
    }
  }

  /// Run until the session event channel closes.
  pub async fn run(
    self: Arc<Self>,
    mut events: mpsc::UnboundedReceiver<AirPlaySessionEvent>,
    rings: Arc<dyn RingLookup>,
  ) {
    while let Some(event) = events.recv().await {
      match event {
        AirPlaySessionEvent::Started { device_id, sample_rate, channels } => {
          if let Err(err) = self
            .handle_session_start(&device_id, sample_rate, channels, Arc::clone(&rings))
            .await
          {
            tracing::error!(%device_id, error = %err, "failed to start Cast bridge session");
          }
        },
        AirPlaySessionEvent::Ended { device_id } => {
          self.handle_session_end(&device_id).await;
        },
        AirPlaySessionEvent::Volume { device_id, volume_db } => {
          self.handle_volume(&device_id, volume_db);
        },
      }
    }
  }

  async fn handle_session_start(
    &self,
    device_id: &str,
    sample_rate: u32,
    channels: u16,
    rings: Arc<dyn RingLookup>,
  ) -> Result<()> {
    // Tear down any previous session for this device first.
    self.handle_session_end(device_id).await;

    let device = self
      .registry
      .get(device_id)
      .ok_or_else(|| Error::Bridge(format!("unknown device {device_id}")))?;

    let ring = rings
      .ring_for(device_id)
      .ok_or_else(|| Error::Bridge(format!("no PCM ring for {device_id}")))?;

    // Prefer the ring's actual channel layout (rebuilt in `audio_init`).
    let stream_channels = ring.channels().max(1).min(channels.max(1));
    let stream_rate = sample_rate.max(1);

    if ring.channels() != stream_channels {
      return Err(Error::Bridge(format!(
        "channel mismatch: ring={} event={stream_channels}",
        ring.channels()
      )));
    }

    for _ in 0..PREBUFFER_POLLS {
      if ring.available_frames() >= PREBUFFER_FRAMES {
        break;
      }
      sleep(PREBUFFER_POLL).await;
    }

    if ring.available_frames() == 0 {
      return Err(Error::Bridge("no PCM available at session start".to_owned()));
    }

    verify_flac_snapshot(&ring, stream_channels, stream_rate);

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
    let load_result = tokio::task::spawn_blocking(move || {
      let request = MediaLoadRequest::wav(load_url, CastStreamKind::Buffered).with_title(cast_name);
      let session = pool.load(&load_device_id, request)?;
      // Nest device volume is independent of AirPlay; raise receiver volume.
      if let Err(err) = pool.set_volume(&load_device_id, 1.0) {
        tracing::debug!(error = %err, "post-load Cast volume set failed");
      }
      Ok::<_, Error>(session)
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
        {
          let mut guard = self.sessions.lock();
          drop(guard.insert(
            device_id.to_owned(),
            ActiveSession {
              media,
              device_id: device_id.to_owned(),
              pool: Arc::clone(&self.cast_pool),
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

/// Run shipped teardown order for one active session.
fn end_active_session(active: ActiveSession) {
  let ActiveSession { media, device_id, pool } = active;
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
