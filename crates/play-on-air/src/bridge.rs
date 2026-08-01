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
use crate::cast::{CastController, CastStreamKind, MediaLoadRequest};
use crate::error::{Error, Result};
use crate::media::{MediaContent, MediaServer, MediaServerHandle};
use crate::net::advertise_host_ip;
use crate::registry::DeviceRegistry;

/// Frames to wait for before starting Cast load.
const PREBUFFER_FRAMES: usize = 1024;
/// Max prebuffer poll iterations (~1 s at 20 ms).
const PREBUFFER_POLLS: u32 = 50;
const PREBUFFER_POLL: Duration = Duration::from_millis(20);
/// Frames copied for the FLAC quality-path snapshot at session start.
const SNAPSHOT_FRAMES: usize = 2048;

/// One live bridge session for a device.
struct ActiveSession {
  media: MediaServerHandle,
  cast: CastController,
}

/// Orchestrates media HTTP + Cast load for AirPlay lifecycle events.
pub struct Bridge {
  registry: Arc<DeviceRegistry>,
  sessions: Mutex<HashMap<String, ActiveSession>>,
}

impl std::fmt::Debug for Bridge {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Bridge")
      .field("registry", &self.registry)
      .field("active_sessions", &self.sessions.lock().len())
      .finish()
  }
}

impl Bridge {
  /// Create a bridge over the shared device registry.
  pub fn new(registry: Arc<DeviceRegistry>) -> Self {
    Self {
      registry,
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
          self.handle_session_end(&device_id);
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
    self.handle_session_end(device_id);

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

    let host = advertise_host_ip();
    let media = MediaServer::start(&host).await?;
    let stream_url = media.stream_url();

    media.set_content(MediaContent::LiveWav {
      ring: Arc::clone(&ring),
      channels: stream_channels,
      sample_rate: stream_rate,
    });

    let mut cast = CastController::new(device.host.clone(), device.port);
    let request = MediaLoadRequest::wav(stream_url, CastStreamKind::Live).with_title(device.name.clone());

    match cast.connect_and_load(request) {
      Ok(session) => {
        tracing::info!(
          %device_id,
          cast = %device.host,
          transport_id = %session.transport_id,
          media_session_id = session.media_session_id,
          "bridge session Cast LIVE WAV load ok"
        );
        {
          let mut guard = self.sessions.lock();
          drop(guard.insert(device_id.to_owned(), ActiveSession { media, cast }));
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

  fn handle_session_end(&self, device_id: &str) {
    let removed = {
      let mut guard = self.sessions.lock();
      guard.remove(device_id)
    };
    let Some(mut active) = removed else {
      return;
    };
    if let Err(err) = active.cast.stop_active() {
      tracing::warn!(%device_id, error = %err, "Cast STOP on session end failed");
    }
    active.media.shutdown();
    tracing::info!(%device_id, "bridge session ended (Cast STOP + media dropped)");
  }

  fn handle_volume(&self, device_id: &str, volume_db: f32) {
    let level = airplay_db_to_cast_linear(volume_db);
    let result = {
      let mut guard = self.sessions.lock();
      guard.get_mut(device_id).map(|session| session.cast.set_volume(level))
    };
    if let Some(Err(err)) = result {
      tracing::debug!(%device_id, error = %err, "Cast volume sync failed");
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
