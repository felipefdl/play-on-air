//! Session bridge: AirPlay PCM → continuous lossless WAV HTTP → Cast LIVE load.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::airplay::AirPlaySessionEvent;
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

/// Orchestrates media HTTP + Cast load when an AirPlay session starts.
#[derive(Debug)]
pub struct Bridge {
  registry: Arc<DeviceRegistry>,
  /// Active media servers keyed by device id (held for lifetime of session).
  media: parking_lot::Mutex<std::collections::HashMap<String, MediaServerHandle>>,
}

impl Bridge {
  /// Create a bridge over the shared device registry.
  pub fn new(registry: Arc<DeviceRegistry>) -> Self {
    Self {
      registry,
      media: parking_lot::Mutex::new(std::collections::HashMap::new()),
    }
  }

  /// Run until the session event channel closes.
  pub async fn run(
    self: Arc<Self>,
    mut events: mpsc::UnboundedReceiver<AirPlaySessionEvent>,
    rings: Arc<dyn RingLookup>,
  ) {
    while let Some(event) = events.recv().await {
      if let Err(err) = self.handle_session_start(&event, Arc::clone(&rings)).await {
        tracing::error!(
          device_id = %event.device_id,
          error = %err,
          "failed to start Cast bridge session"
        );
      }
    }
  }

  async fn handle_session_start(&self, event: &AirPlaySessionEvent, rings: Arc<dyn RingLookup>) -> Result<()> {
    let device = self
      .registry
      .get(&event.device_id)
      .ok_or_else(|| Error::Bridge(format!("unknown device {}", event.device_id)))?;

    let ring = rings
      .ring_for(&event.device_id)
      .ok_or_else(|| Error::Bridge(format!("no PCM ring for {}", event.device_id)))?;

    let channels = event.channels.max(1);
    let sample_rate = event.sample_rate.max(1);

    // 1. Prebuffer wait so Cast LIVE pull does not start on silence only.
    for _ in 0..PREBUFFER_POLLS {
      if ring.available_frames() >= PREBUFFER_FRAMES {
        break;
      }
      sleep(PREBUFFER_POLL).await;
    }

    if ring.available_frames() == 0 {
      return Err(Error::Bridge("no PCM available at session start".to_owned()));
    }

    // 2. Exercise FLAC quality path on a non-destructive prebuffer snapshot.
    verify_flac_snapshot(&ring, channels, sample_rate);

    // 3. Start media server with LAN-reachable advertise host.
    let host = advertise_host_ip();
    let media = MediaServer::start(&host).await?;
    let stream_url = media.stream_url();

    // 4. Prefer continuous LiveWav (lossless) as the Cast LIVE path.
    media.set_content(MediaContent::LiveWav {
      ring: Arc::clone(&ring),
      channels,
      sample_rate,
    });

    {
      let mut guard = self.media.lock();
      // Drop any previous session media for this device.
      drop(guard.insert(event.device_id.clone(), media));
    }

    // 5. Cast load LIVE WAV; media handle kept for session lifetime in `self.media`.
    let mut cast = CastController::new(device.host.clone(), device.port);
    let request = MediaLoadRequest::wav(stream_url, CastStreamKind::Live).with_title(device.name.clone());

    match cast.connect_and_load(request) {
      Ok(()) => {
        tracing::info!(
          device_id = %event.device_id,
          cast = %device.host,
          "bridge session Cast LIVE WAV load ok"
        );
        Ok(())
      },
      Err(err) => {
        tracing::warn!(
          device_id = %event.device_id,
          error = %err,
          "Cast load failed (device may be offline)"
        );
        Err(err)
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
///
/// Unit-test helper and secondary static-FLAC path without network I/O.
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
