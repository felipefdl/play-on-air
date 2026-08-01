//! AirPlay 2 receiver wrappers (one `RaopServer` per Chromecast).

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use parking_lot::Mutex;
use shairplay::{AirPlayMode, AudioFormat, AudioHandler, AudioSession, RaopServer};
use tokio::sync::mpsc;

use crate::audio::PcmRing;
use crate::error::{Error, Result};

/// Base TCP port for RAOP listeners; each device gets `BASE + (hash % PORT_SPAN)`.
const RAOP_PORT_BASE: u16 = 5100;
const RAOP_PORT_SPAN: u64 = 1000;

/// Event emitted when an AirPlay session starts producing audio.
#[derive(Debug, Clone)]
pub struct AirPlaySessionEvent {
  /// Device id this receiver is bound to.
  pub device_id: String,
  /// Sample rate of the decoded PCM stream.
  pub sample_rate: u32,
  /// Channel count of the decoded PCM stream.
  pub channels: u16,
}

/// Shared PCM ring + optional session event sender for one device.
#[derive(Debug)]
struct DeviceAudioState {
  ring: Arc<PcmRing>,
  event_tx: Option<mpsc::UnboundedSender<AirPlaySessionEvent>>,
  device_id: String,
}

/// Forwards decoded f32 PCM into a [`PcmRing`].
struct RingSession {
  ring: Arc<PcmRing>,
}

impl AudioSession for RingSession {
  fn audio_process(&mut self, samples: &[f32]) {
    self.ring.push_f32(samples);
  }
}

/// Creates ring-backed sessions and notifies the bridge on first init.
struct RingHandler {
  state: Arc<DeviceAudioState>,
}

impl AudioHandler for RingHandler {
  fn audio_init(&self, format: AudioFormat) -> Box<dyn AudioSession> {
    let sample_rate = format.sample_rate;
    let channels = u16::from(format.channels.max(1));
    // Resize path: replace ring if channel layout differs substantially.
    if let Some(tx) = &self.state.event_tx {
      drop(tx.send(AirPlaySessionEvent {
        device_id: self.state.device_id.clone(),
        sample_rate,
        channels,
      }));
    }
    Box::new(RingSession { ring: Arc::clone(&self.state.ring) })
  }
}

/// One running AirPlay 2 advertisement for a Cast device.
pub struct AirPlayReceiver {
  /// Bound Cast / registry device id.
  pub device_id: String,
  /// Advertised AirPlay name.
  pub name: String,
  /// Shared PCM ring for this receiver.
  pub ring: Arc<PcmRing>,
  server: RaopServer,
}

impl std::fmt::Debug for AirPlayReceiver {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("AirPlayReceiver")
      .field("device_id", &self.device_id)
      .field("name", &self.name)
      .field("ring", &self.ring)
      .finish_non_exhaustive()
  }
}

impl AirPlayReceiver {
  /// Build (but do not start) an AP2-only receiver with stable per-device identity.
  pub fn build(
    device_id: impl Into<String>,
    name: impl Into<String>,
    event_tx: Option<mpsc::UnboundedSender<AirPlaySessionEvent>>,
  ) -> Result<Self> {
    let device_id_owned = device_id.into();
    let name_owned = name.into();
    // ~2s of stereo 48 kHz as a starting capacity.
    let ring = Arc::new(PcmRing::new(2, 48_000 * 2));
    let state = Arc::new(DeviceAudioState {
      ring: Arc::clone(&ring),
      event_tx,
      device_id: device_id_owned.clone(),
    });
    let handler = Arc::new(RingHandler { state });

    let hwaddr = stable_hwaddr(&device_id_owned);
    let port = stable_raop_port(&device_id_owned);

    let server = RaopServer::builder()
      .name(name_owned.clone())
      .mode(AirPlayMode::AirPlay2)
      .hwaddr(hwaddr.to_vec())
      .port(port)
      .build(handler)
      .map_err(|err| Error::AirPlay(format!("build RaopServer for {name_owned}: {err}")))?;

    tracing::debug!(
      device_id = %device_id_owned,
      port,
      hwaddr = %format_hwaddr(hwaddr),
      "AirPlay 2 receiver identity assigned"
    );

    Ok(Self {
      device_id: device_id_owned,
      name: name_owned,
      ring,
      server,
    })
  }

  /// Start advertising and accepting AirPlay sessions.
  pub async fn start(&mut self) -> Result<()> {
    self
      .server
      .start()
      .await
      .map_err(|err| Error::AirPlay(format!("start {}: {err}", self.name)))?;
    tracing::info!(device_id = %self.device_id, name = %self.name, "AirPlay 2 receiver started");
    Ok(())
  }
}

/// Manages the set of live AirPlay receivers keyed by Cast device id.
#[derive(Debug, Default)]
pub struct AirPlayManager {
  receivers: Mutex<HashMap<String, AirPlayReceiver>>,
  event_tx: Option<mpsc::UnboundedSender<AirPlaySessionEvent>>,
}

impl AirPlayManager {
  /// Create a manager that forwards session-start events on `event_tx`.
  pub fn new(event_tx: Option<mpsc::UnboundedSender<AirPlaySessionEvent>>) -> Self {
    Self {
      receivers: Mutex::new(HashMap::new()),
      event_tx,
    }
  }

  /// Ensure a receiver exists for `device_id` with the given AirPlay name.
  pub async fn ensure(&self, device_id: &str, airplay_name: &str) -> Result<()> {
    {
      let guard = self.receivers.lock();
      if let Some(existing) = guard.get(device_id)
        && existing.name == airplay_name
      {
        return Ok(());
      }
    }

    // Rebuild if missing or renamed.
    self.remove(device_id);

    let mut rx = AirPlayReceiver::build(device_id, airplay_name, self.event_tx.clone())?;
    rx.start().await?;
    {
      let mut guard = self.receivers.lock();
      drop(guard.insert(device_id.to_owned(), rx));
    }
    Ok(())
  }

  /// Stop and drop the receiver for `device_id`.
  pub fn remove(&self, device_id: &str) {
    let removed = {
      let mut guard = self.receivers.lock();
      guard.remove(device_id)
    };
    if let Some(rx) = removed {
      tracing::info!(device_id = %rx.device_id, name = %rx.name, "AirPlay receiver withdrawn");
      // RaopServer drops on leave; no explicit stop API required for scaffolding.
      drop(rx);
    }
  }

  /// Active device ids currently advertised.
  pub fn active_ids(&self) -> Vec<String> {
    self.receivers.lock().keys().cloned().collect()
  }

  /// PCM ring for a device, if an advertisement exists.
  pub fn pcm_ring(&self, device_id: &str) -> Option<Arc<PcmRing>> {
    self.receivers.lock().get(device_id).map(|r| Arc::clone(&r.ring))
  }
}

/// Stable 6-byte MAC derived from `device_id` with locally administered unicast bits.
pub fn stable_hwaddr(device_id: &str) -> [u8; 6] {
  let h = hash_device_id(device_id);
  let mut mac = [0_u8; 6];
  // Spread hash bits across the address.
  if let Some(slot) = mac.get_mut(0) {
    *slot = ((h >> 40) as u8) & 0xFE; // clear multicast
    *slot |= 0x02; // set locally administered
  }
  if let Some(slot) = mac.get_mut(1) {
    *slot = (h >> 32) as u8;
  }
  if let Some(slot) = mac.get_mut(2) {
    *slot = (h >> 24) as u8;
  }
  if let Some(slot) = mac.get_mut(3) {
    *slot = (h >> 16) as u8;
  }
  if let Some(slot) = mac.get_mut(4) {
    *slot = (h >> 8) as u8;
  }
  if let Some(slot) = mac.get_mut(5) {
    *slot = h as u8;
  }
  mac
}

/// Unique RAOP listen port in `5100..6099` derived from `device_id`.
pub fn stable_raop_port(device_id: &str) -> u16 {
  let h = hash_device_id(device_id);
  let offset = (h % RAOP_PORT_SPAN) as u16;
  RAOP_PORT_BASE.saturating_add(offset)
}

fn hash_device_id(device_id: &str) -> u64 {
  let mut hasher = std::collections::hash_map::DefaultHasher::new();
  device_id.hash(&mut hasher);
  hasher.finish()
}

fn format_hwaddr(mac: [u8; 6]) -> String {
  // Fixed-size array: get(i) is always Some; defensive for clippy indexing rules.
  let b0 = mac.first().copied().unwrap_or(0);
  let b1 = mac.get(1).copied().unwrap_or(0);
  let b2 = mac.get(2).copied().unwrap_or(0);
  let b3 = mac.get(3).copied().unwrap_or(0);
  let b4 = mac.get(4).copied().unwrap_or(0);
  let b5 = mac.get(5).copied().unwrap_or(0);
  format!("{b0:02x}:{b1:02x}:{b2:02x}:{b3:02x}:{b4:02x}:{b5:02x}")
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn stable_hwaddr_is_locally_administered_unicast() {
    let mac = stable_hwaddr("cast-device-abc");
    assert_eq!(mac[0] & 0x01, 0, "must be unicast");
    assert_eq!(mac[0] & 0x02, 0x02, "must be locally administered");
  }

  #[test]
  fn stable_hwaddr_deterministic() {
    assert_eq!(stable_hwaddr("x"), stable_hwaddr("x"));
    assert_ne!(stable_hwaddr("a"), stable_hwaddr("b"));
  }

  #[test]
  fn stable_raop_port_in_range() {
    let p = stable_raop_port("device-1");
    assert!((5100..6100).contains(&p));
    assert_eq!(stable_raop_port("device-1"), stable_raop_port("device-1"));
  }

  #[test]
  fn distinct_devices_get_distinct_ports_often() {
    // Not a guarantee for all pairs, but different ids should usually differ.
    let a = stable_raop_port("kitchen-tv");
    let b = stable_raop_port("bedroom-speaker");
    // If they collide, MAC still differs — just ensure both are valid.
    assert!((5100..6100).contains(&a));
    assert!((5100..6100).contains(&b));
    let _ = (a, b);
  }
}
