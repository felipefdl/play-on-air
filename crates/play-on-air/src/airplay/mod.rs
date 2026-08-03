//! AirPlay 2 receiver wrappers (one `RaopServer` per Chromecast).

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use shairplay::{AirPlayMode, AudioFormat, AudioHandler, AudioSession, RaopServer};
use tokio::sync::mpsc;

use crate::audio::PcmRing;
use crate::error::{Error, Result};

/// Base TCP port for RAOP listeners; each device gets `BASE + (hash % PORT_SPAN)`.
const RAOP_PORT_BASE: u16 = 5100;
const RAOP_PORT_SPAN: u64 = 1000;
/// Default stereo capacity (~2 s at 48 kHz) until `audio_init` sets the real layout.
const DEFAULT_RING_FRAMES: usize = 48_000 * 2;
/// Product max channels for Cast stereo path.
const OUTPUT_MAX_CHANNELS: u8 = 2;
/// RTSP `Audio-Latency` advertised on RECORD (samples at stream rate).
///
/// Stream rate is not known at receiver build (passthrough). Use 2 s at 48 kHz
/// so the constant is visible product-side and matches shairplay's default.
const AUDIO_LATENCY_SAMPLES: u32 = 48_000 * 2;
/// Lifecycle events from an AirPlay receiver toward the bridge.
#[derive(Debug, Clone)]
pub enum AirPlaySessionEvent {
  /// Client started an audio stream (PCM format known).
  Started {
    /// Device id this receiver is bound to.
    device_id: String,
    /// Sample rate of the decoded PCM stream.
    sample_rate: u32,
    /// Ring receiving this stream's decoded PCM (rebuilt per `audio_init`).
    ///
    /// Carrying the ring pins the event to its stream: if the client restarts
    /// quickly, the bridge can detect a stale `Started` by comparing this ring
    /// against the receiver's current one instead of wiring the new ring (and
    /// possibly a different sample rate) to the old event.
    ring: Arc<PcmRing>,
  },
  /// Client disconnected; bridge should Cast-STOP and drop media.
  Ended {
    /// Device id this receiver is bound to.
    device_id: String,
  },
  /// AirPlay playout rate went to 0 (or PCM idle); Cast should PAUSE promptly.
  Paused {
    /// Device id this receiver is bound to.
    device_id: String,
  },
  /// PCM flowing again after pause; Cast should PLAY.
  Resumed {
    /// Device id this receiver is bound to.
    device_id: String,
  },
  /// AirPlay FLUSH / buffer clear; ring already cleared, Cast should PAUSE.
  Flushed {
    /// Device id this receiver is bound to.
    device_id: String,
  },
  /// AirPlay volume change in dB (0.0 = max, -144.0 = mute).
  Volume {
    /// Device id this receiver is bound to.
    device_id: String,
    /// Volume in dB.
    volume_db: f32,
  },
}

/// Shared mutable ring slot so `audio_init` can rebuild the ring for the stream format.
#[derive(Debug)]
struct DeviceAudioState {
  ring_slot: Mutex<Arc<PcmRing>>,
  event_tx: Option<mpsc::UnboundedSender<AirPlaySessionEvent>>,
  device_id: String,
}

impl DeviceAudioState {
  fn current_ring(&self) -> Arc<PcmRing> {
    Arc::clone(&self.ring_slot.lock())
  }

  fn replace_ring(&self, ring: Arc<PcmRing>) {
    *self.ring_slot.lock() = ring;
  }
}

/// Forwards decoded f32 PCM into the current [`PcmRing`].
///
/// `Ended` is emitted from [`Drop`]: that is the end of the **audio** stream.
/// RTSP Remote-Control connections also call [`AudioHandler::on_client_disconnected`]
/// when they close; those must not stop Cast (multi-speaker iOS opens/closes RC
/// links while audio keeps running on another connection).
///
/// Cast PAUSE sources (AP2 buffered):
/// - [`AudioSession::on_rate`] with `rate == 0` (iPhone pause)
/// - [`AudioSession::on_flush`] / [`AudioSession::audio_flush`] (seek / buffer clear)
///
/// We do **not** Cast-PAUSE on PCM idle: buffered AP2 gaps and underruns left Nest
/// paused while iPhone still showed Streaming and advanced the scrubber.
///
/// Resume: [`AudioSession::on_rate`] nonzero if previously paused, and/or the first
/// PCM after flush via [`Self::note_pcm`] (single source of truth via `cast_paused`).
struct RingSession {
  state: Arc<DeviceAudioState>,
  ring: Arc<PcmRing>,
  cast_paused: Arc<AtomicBool>,
}

impl RingSession {
  fn new(state: Arc<DeviceAudioState>, ring: Arc<PcmRing>) -> Self {
    Self {
      state,
      ring,
      cast_paused: Arc::new(AtomicBool::new(false)),
    }
  }

  fn note_pcm(&self) {
    if self.cast_paused.swap(false, Ordering::AcqRel)
      && let Some(tx) = &self.state.event_tx
    {
      drop(tx.send(AirPlaySessionEvent::Resumed { device_id: self.state.device_id.clone() }));
    }
  }

  /// Clear ring + `Flushed`; set paused and emit `Paused` if not already paused.
  fn emit_flush_and_pause(&self) {
    self.ring.clear();
    if let Some(tx) = &self.state.event_tx {
      drop(tx.send(AirPlaySessionEvent::Flushed { device_id: self.state.device_id.clone() }));
    }
    if !self.cast_paused.swap(true, Ordering::AcqRel)
      && let Some(tx) = &self.state.event_tx
    {
      drop(tx.send(AirPlaySessionEvent::Paused { device_id: self.state.device_id.clone() }));
    }
  }

  /// Whether this session still owns the live ring (not superseded by a later `audio_init`).
  fn owns_current_ring(&self) -> bool {
    Arc::ptr_eq(&self.ring, &self.state.current_ring())
  }
}

impl AudioSession for RingSession {
  fn audio_process(&mut self, samples: &[f32]) {
    self.note_pcm();
    self.ring.push_f32(samples);
  }

  fn audio_flush(&mut self) {
    self.emit_flush_and_pause();
  }

  fn on_rate(&mut self, rate: u32) {
    if rate == 0 {
      if !self.cast_paused.swap(true, Ordering::AcqRel)
        && let Some(tx) = &self.state.event_tx
      {
        drop(tx.send(AirPlaySessionEvent::Paused { device_id: self.state.device_id.clone() }));
      }
    } else if self.cast_paused.swap(false, Ordering::AcqRel)
      && let Some(tx) = &self.state.event_tx
    {
      // Dedupe with `note_pcm`: only emit Resumed when we were actually paused.
      drop(tx.send(AirPlaySessionEvent::Resumed { device_id: self.state.device_id.clone() }));
    }
  }

  fn on_flush(&mut self) {
    self.emit_flush_and_pause();
  }
}

impl Drop for RingSession {
  fn drop(&mut self) {
    // Format rebuild replaces the ring then drops the old session. Only the
    // current session owns the live ring — skip clear + Ended when stale so we
    // do not kill the session that `audio_init` just started.
    if !self.owns_current_ring() {
      tracing::debug!(
        device_id = %self.state.device_id,
        "AirPlay audio session dropped (stale ring; suppress Ended)"
      );
      return;
    }
    self.ring.clear();
    if let Some(tx) = &self.state.event_tx {
      drop(tx.send(AirPlaySessionEvent::Ended { device_id: self.state.device_id.clone() }));
    }
    tracing::debug!(
      device_id = %self.state.device_id,
      "AirPlay audio session dropped (bridge should Cast-STOP)"
    );
  }
}

/// Creates ring-backed sessions and notifies the bridge on lifecycle events.
struct RingHandler {
  state: Arc<DeviceAudioState>,
}

impl AudioHandler for RingHandler {
  fn audio_init(&self, format: AudioFormat) -> Box<dyn AudioSession> {
    let sample_rate = format.sample_rate.max(1);
    // Cap at product stereo; shairplay also mixdowns via output_max_channels(2).
    let channels = u16::from(format.channels.clamp(1, OUTPUT_MAX_CHANNELS));
    let capacity_frames = usize::try_from(sample_rate)
      .unwrap_or(48_000)
      .saturating_mul(2)
      .max(DEFAULT_RING_FRAMES);
    let ring = Arc::new(PcmRing::new(channels, capacity_frames));
    // shairplay aborts any prior audio session before this callback (exclusive
    // active audio). We always replace the ring; the stack drops the old session.
    self.state.replace_ring(Arc::clone(&ring));
    tracing::info!(
      device_id = %self.state.device_id,
      sample_rate,
      channels,
      "AirPlay audio session started (exclusive; prior stream aborted by stack if any)"
    );

    if let Some(tx) = &self.state.event_tx {
      drop(tx.send(AirPlaySessionEvent::Started {
        device_id: self.state.device_id.clone(),
        sample_rate,
        ring: Arc::clone(&ring),
      }));
    }
    Box::new(RingSession::new(Arc::clone(&self.state), ring))
  }

  fn on_volume(&self, volume: f32) {
    if let Some(tx) = &self.state.event_tx {
      drop(tx.send(AirPlaySessionEvent::Volume {
        device_id: self.state.device_id.clone(),
        volume_db: volume,
      }));
    }
  }

  fn on_client_connected(&self, addr: &str) {
    tracing::debug!(device_id = %self.state.device_id, %addr, "AirPlay RTSP client connected");
  }

  fn on_client_disconnected(&self, addr: &str) {
    // Do **not** send `Ended` or clear the ring here. shairplay calls this for
    // every RTSP TCP close, including short-lived Remote Control (type 130)
    // probes. Ending the bridge here is what killed multi-speaker after a few
    // seconds while the first speaker kept playing.
    tracing::debug!(
      device_id = %self.state.device_id,
      %addr,
      "AirPlay RTSP client disconnected (audio session may still be active)"
    );
  }
}

/// One running AirPlay 2 advertisement for a Cast device.
pub struct AirPlayReceiver {
  /// Bound Cast / registry device id.
  pub device_id: String,
  /// Advertised AirPlay name.
  pub name: String,
  /// TCP port the RAOP listener uses (stable per device id).
  pub port: u16,
  state: Arc<DeviceAudioState>,
  server: RaopServer,
  /// True after reported volume was seeded from Cast (host path).
  volume_seeded_from_cast: AtomicBool,
}

impl std::fmt::Debug for AirPlayReceiver {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("AirPlayReceiver")
      .field("device_id", &self.device_id)
      .field("name", &self.name)
      .field("port", &self.port)
      .field("ring_channels", &self.state.current_ring().channels())
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
    // Placeholder stereo ring until audio_init rebuilds from AudioFormat.
    let ring = Arc::new(PcmRing::new(2, DEFAULT_RING_FRAMES));
    let state = Arc::new(DeviceAudioState {
      ring_slot: Mutex::new(ring),
      event_tx,
      device_id: device_id_owned.clone(),
    });
    let handler = Arc::new(RingHandler { state: Arc::clone(&state) });

    let hwaddr = stable_hwaddr(&device_id_owned);
    let port = stable_raop_port(&device_id_owned);

    let server = RaopServer::builder()
      .name(name_owned.clone())
      .mode(AirPlayMode::AirPlay2)
      .hwaddr(hwaddr.to_vec())
      .port(port)
      .output_max_channels(OUTPUT_MAX_CHANNELS)
      .audio_latency_samples(AUDIO_LATENCY_SAMPLES)
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
      port,
      state,
      server,
      volume_seeded_from_cast: AtomicBool::new(false),
    })
  }

  /// Current PCM ring (may be rebuilt on each `audio_init`).
  pub fn ring(&self) -> Arc<PcmRing> {
    self.state.current_ring()
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

  /// Seed the dB value returned by AirPlay `GET_PARAMETER volume` (clamped by shairplay).
  ///
  /// Marks the receiver as seeded so maintain stops re-querying Cast.
  pub fn set_reported_volume_db(&self, volume_db: f32) {
    self.server.set_reported_volume_db(volume_db);
    self.volume_seeded_from_cast.store(true, Ordering::Relaxed);
  }

  /// Current reported AirPlay volume in dB (`0.0` = max until seeded from Cast).
  pub fn reported_volume_db(&self) -> f32 {
    self.server.reported_volume_db()
  }

  /// Whether reported volume has been seeded from Cast (or host) at least once.
  pub fn volume_seeded_from_cast(&self) -> bool {
    self.volume_seeded_from_cast.load(Ordering::Relaxed)
  }

  /// Stop mDNS + accept loop, then force-close live RTSP sockets on this port.
  ///
  /// Required for kick: dropping `RaopServer` alone leaves accepted TCP streams open.
  pub async fn shutdown_hard(&mut self) {
    self.server.stop().await;
    crate::net::force_close_tcp_on_local_port(self.port);
  }
}

/// How long a speaker stays withdrawn after a hard kick before re-advertise.
const KICK_WITHDRAW_HOLD: Duration = Duration::from_millis(1_500);
/// Cap on hard-stop wait during [`AirPlayManager::remove`] so re-advertise is not blocked forever.
const REMOVE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

/// Manages the set of live AirPlay receivers keyed by Cast device id.
#[derive(Debug, Default)]
pub struct AirPlayManager {
  receivers: Mutex<HashMap<String, AirPlayReceiver>>,
  /// Device ids mid-kick: [`Self::ensure`] must not recreate them (maintain race).
  kicking: Mutex<HashSet<String>>,
  event_tx: Option<mpsc::UnboundedSender<AirPlaySessionEvent>>,
}

impl AirPlayManager {
  /// Create a manager that forwards session lifecycle events on `event_tx`.
  pub fn new(event_tx: Option<mpsc::UnboundedSender<AirPlaySessionEvent>>) -> Self {
    Self {
      receivers: Mutex::new(HashMap::new()),
      kicking: Mutex::new(HashSet::new()),
      event_tx,
    }
  }

  /// Ensure a receiver exists for `device_id` with the given AirPlay name.
  pub async fn ensure(&self, device_id: &str, airplay_name: &str) -> Result<()> {
    if self.kicking.lock().contains(device_id) {
      tracing::debug!(%device_id, "ensure skipped: device is being kicked");
      return Ok(());
    }
    {
      let guard = self.receivers.lock();
      if let Some(existing) = guard.get(device_id)
        && existing.name == airplay_name
      {
        return Ok(());
      }
    }

    self.remove(device_id);

    let mut rx = AirPlayReceiver::build(device_id, airplay_name, self.event_tx.clone())?;
    rx.start().await?;
    {
      let mut guard = self.receivers.lock();
      drop(guard.insert(device_id.to_owned(), rx));
    }
    Ok(())
  }

  /// Stop and drop the receiver for `device_id` (best-effort hard stop when in a runtime).
  ///
  /// When a tokio multi-thread runtime is current, hard-stop is **awaited** (with
  /// [`REMOVE_SHUTDOWN_TIMEOUT`]) so a following [`Self::ensure`] does not race the
  /// RAOP port. Detached spawn used to leave the old accept loop alive long enough
  /// for re-bind / iOS session races.
  pub fn remove(&self, device_id: &str) {
    let removed = {
      let mut guard = self.receivers.lock();
      guard.remove(device_id)
    };
    if let Some(mut rx) = removed {
      let withdrawn_id = rx.device_id.clone();
      let name = rx.name.clone();
      let port = rx.port;
      tracing::info!(device_id = %withdrawn_id, %name, "AirPlay receiver withdrawn");
      if let Ok(handle) = tokio::runtime::Handle::try_current() {
        // Callers include async maintain/ensure; block_in_place is safe on the
        // multi-thread runtime used by `#[tokio::main]`.
        tokio::task::block_in_place(|| {
          handle.block_on(async {
            if tokio::time::timeout(REMOVE_SHUTDOWN_TIMEOUT, rx.shutdown_hard()).await.is_err() {
              tracing::warn!(
                device_id = %withdrawn_id,
                port,
                "AirPlay hard-stop timed out during remove; force-closing port"
              );
              crate::net::force_close_tcp_on_local_port(port);
            }
          });
        });
      } else {
        crate::net::force_close_tcp_on_local_port(port);
        drop(rx);
      }
    }
  }

  /// Force-drop all AP2 sessions for `device_id` after Cast ownership loss, then re-advertise.
  ///
  /// Marks the device as **kicking** so [`Self::ensure`] (maintain loop) cannot recreate
  /// the receiver during the withdrawal hold. Uses vendored shairplay hard-stop (abort
  /// RTSP + session tasks) plus best-effort `ss -K` on the RAOP port.
  pub async fn kick_clients(&self, device_id: &str) -> Result<()> {
    {
      let mut kicking = self.kicking.lock();
      let _inserted = kicking.insert(device_id.to_owned());
    }

    let result = self.kick_clients_inner(device_id).await;

    {
      let mut kicking = self.kicking.lock();
      let _removed = kicking.remove(device_id);
    }
    result
  }

  async fn kick_clients_inner(&self, device_id: &str) -> Result<()> {
    let removed = {
      let mut guard = self.receivers.lock();
      guard.remove(device_id)
    };
    let Some(mut rx) = removed else {
      tracing::debug!(%device_id, "kick_clients: no AirPlay receiver for device");
      return Ok(());
    };
    let airplay_name = rx.name.clone();
    let port = rx.port;
    tracing::info!(
      %device_id,
      airplay_name = %airplay_name,
      port,
      "kicking AirPlay clients (hard-stop RTSP + session tasks)"
    );
    rx.shutdown_hard().await;
    drop(rx);

    // Hold advertisement withdrawn so iOS observes disconnect before re-appear.
    tokio::time::sleep(KICK_WITHDRAW_HOLD).await;

    // Re-advertise under same name (kicking flag still set — use direct insert path).
    let mut replacement = AirPlayReceiver::build(device_id, &airplay_name, self.event_tx.clone())?;
    replacement.start().await?;
    {
      let mut guard = self.receivers.lock();
      drop(guard.insert(device_id.to_owned(), replacement));
    }
    tracing::info!(
      %device_id,
      airplay_name = %airplay_name,
      "kicked AirPlay clients after Cast ownership loss; re-advertised"
    );
    Ok(())
  }

  /// Active device ids currently advertised.
  pub fn active_ids(&self) -> Vec<String> {
    self.receivers.lock().keys().cloned().collect()
  }

  /// PCM ring for a device, if an advertisement exists.
  pub fn pcm_ring(&self, device_id: &str) -> Option<Arc<PcmRing>> {
    self.receivers.lock().get(device_id).map(AirPlayReceiver::ring)
  }

  /// Set the AirPlay `GET_PARAMETER volume` value for an advertised device (no-op if unknown).
  pub fn set_reported_volume_db(&self, device_id: &str, volume_db: f32) {
    if let Some(rx) = self.receivers.lock().get(device_id) {
      rx.set_reported_volume_db(volume_db);
    }
  }

  /// Reported AirPlay volume for `device_id`, if advertised.
  pub fn reported_volume_db(&self, device_id: &str) -> Option<f32> {
    self.receivers.lock().get(device_id).map(AirPlayReceiver::reported_volume_db)
  }

  /// Whether `device_id` still needs a Cast volume seed for `GET_PARAMETER`.
  pub fn needs_volume_seed(&self, device_id: &str) -> bool {
    self
      .receivers
      .lock()
      .get(device_id)
      .is_some_and(|rx| !rx.volume_seeded_from_cast())
  }
}

/// Stable 6-byte MAC derived from `device_id` with locally administered unicast bits.
pub fn stable_hwaddr(device_id: &str) -> [u8; 6] {
  let h = hash_device_id(device_id);
  let mut mac = [0_u8; 6];
  if let Some(slot) = mac.get_mut(0) {
    *slot = ((h >> 40) as u8) & 0xFE;
    *slot |= 0x02;
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

/// Stable RAOP listen port in `5100..=6099` derived from `device_id`.
///
/// shairplay auto-senses upward (and advertises the actual port) if the
/// derived port is already bound, so collisions only shift the port.
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
  let b0 = mac.first().copied().unwrap_or(0);
  let b1 = mac.get(1).copied().unwrap_or(0);
  let b2 = mac.get(2).copied().unwrap_or(0);
  let b3 = mac.get(3).copied().unwrap_or(0);
  let b4 = mac.get(4).copied().unwrap_or(0);
  let b5 = mac.get(5).copied().unwrap_or(0);
  format!("{b0:02x}:{b1:02x}:{b2:02x}:{b3:02x}:{b4:02x}:{b5:02x}")
}

/// AirPlay mute floor in dB (`GET_PARAMETER` / `SET_PARAMETER`).
const AIRPLAY_VOLUME_DB_MIN: f32 = -144.0;
/// Floor for `log10` when mapping near-silent Cast levels to AirPlay dB.
const CAST_LINEAR_LOG_EPS: f32 = 1.0e-7;

/// Map AirPlay volume (dB, 0 = max, -144 = mute) to Cast linear `0.0..=1.0`.
pub fn airplay_db_to_cast_linear(volume_db: f32) -> f32 {
  if volume_db <= AIRPLAY_VOLUME_DB_MIN {
    return 0.0;
  }
  if volume_db >= 0.0 {
    return 1.0;
  }
  // Approximate amplitude: 10^(dB/20).
  let linear = 10_f32.powf(volume_db / 20.0);
  linear.clamp(0.0, 1.0)
}

/// Map Cast linear volume (`0.0..=1.0`) to AirPlay dB (`0.0` = max, `-144.0` = mute).
///
/// Inverse of [`airplay_db_to_cast_linear`] for the amplitude path (`20 * log10(level)`).
pub fn cast_linear_to_airplay_db(level: f32) -> f32 {
  if !level.is_finite() || level <= 0.0 {
    return AIRPLAY_VOLUME_DB_MIN;
  }
  if level >= 1.0 {
    return 0.0;
  }
  let clamped = level.clamp(CAST_LINEAR_LOG_EPS, 1.0);
  let db = 20.0 * clamped.log10();
  db.clamp(AIRPLAY_VOLUME_DB_MIN, 0.0)
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
  fn airplay_db_to_cast_linear_bounds() {
    assert!((airplay_db_to_cast_linear(0.0) - 1.0).abs() < f32::EPSILON);
    assert!((airplay_db_to_cast_linear(-144.0) - 0.0).abs() < f32::EPSILON);
    let mid = airplay_db_to_cast_linear(-6.0);
    assert!(mid > 0.4 && mid < 0.6);
  }

  #[test]
  fn cast_linear_to_airplay_db_bounds() {
    assert!((cast_linear_to_airplay_db(0.0) - AIRPLAY_VOLUME_DB_MIN).abs() < f32::EPSILON);
    assert!((cast_linear_to_airplay_db(1.0) - 0.0).abs() < f32::EPSILON);
    let mid = cast_linear_to_airplay_db(0.5);
    assert!((mid - (-6.020_6)).abs() < 0.01);
  }

  #[test]
  fn cast_linear_airplay_db_roundtrip_mid() {
    for level in [0.1_f32, 0.25, 0.5, 0.75, 0.9] {
      let db = cast_linear_to_airplay_db(level);
      let back = airplay_db_to_cast_linear(db);
      assert!((back - level).abs() < 1.0e-5, "level={level} db={db} back={back}");
    }
    assert!((airplay_db_to_cast_linear(cast_linear_to_airplay_db(0.0)) - 0.0).abs() < f32::EPSILON);
    assert!((airplay_db_to_cast_linear(cast_linear_to_airplay_db(1.0)) - 1.0).abs() < f32::EPSILON);
  }

  #[test]
  fn ring_handler_rebuilds_ring_for_format_channels() {
    let state = Arc::new(DeviceAudioState {
      ring_slot: Mutex::new(Arc::new(PcmRing::new(2, 64))),
      event_tx: None,
      device_id: "dev".to_owned(),
    });
    let handler = RingHandler { state: Arc::clone(&state) };
    let format = AudioFormat {
      codec: shairplay::AudioCodec::Pcm,
      bits: 32,
      channels: 1,
      sample_rate: 44_100,
    };
    let mut session = handler.audio_init(format);
    assert_eq!(state.current_ring().channels(), 1);
    session.audio_process(&[0.1_f32, 0.2, 0.3]);
    assert_eq!(state.current_ring().available_frames(), 3);
    session.audio_flush();
    assert_eq!(state.current_ring().available_frames(), 0);
  }

  #[test]
  fn format_change_second_init_suppresses_stale_ended() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let state = Arc::new(DeviceAudioState {
      ring_slot: Mutex::new(Arc::new(PcmRing::new(2, 64))),
      event_tx: Some(tx),
      device_id: "dev-fmt".to_owned(),
    });
    let handler = RingHandler { state: Arc::clone(&state) };
    let fmt = |channels: u8| AudioFormat {
      codec: shairplay::AudioCodec::Pcm,
      bits: 32,
      channels,
      sample_rate: 44_100,
    };

    let session1 = handler.audio_init(fmt(1));
    match rx.try_recv() {
      Ok(AirPlaySessionEvent::Started { device_id, sample_rate, ring }) => {
        assert_eq!(device_id, "dev-fmt");
        assert_eq!(sample_rate, 44_100);
        assert_eq!(ring.channels(), 1);
      },
      other => panic!("expected Started, got {other:?}"),
    }

    let session2 = handler.audio_init(fmt(2));
    match rx.try_recv() {
      Ok(AirPlaySessionEvent::Started { ring, .. }) => {
        assert_eq!(ring.channels(), 2);
        assert!(Arc::ptr_eq(&ring, &state.current_ring()));
      },
      other => panic!("expected second Started, got {other:?}"),
    }

    // Old session drop must not emit Ended (would kill the new stream).
    drop(session1);
    match rx.try_recv() {
      Err(_) => {},
      Ok(ev) => panic!("stale RingSession Drop must not emit Ended, got {ev:?}"),
    }

    // Genuine end of the current session still ends the stream.
    drop(session2);
    match rx.try_recv() {
      Ok(AirPlaySessionEvent::Ended { device_id }) => assert_eq!(device_id, "dev-fmt"),
      other => panic!("expected Ended from current session, got {other:?}"),
    }
  }

  #[test]
  fn drop_after_replace_ring_skips_ended() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let ring_a = Arc::new(PcmRing::new(2, 64));
    let state = Arc::new(DeviceAudioState {
      ring_slot: Mutex::new(Arc::clone(&ring_a)),
      event_tx: Some(tx),
      device_id: "dev-stale".to_owned(),
    });
    let session = RingSession::new(Arc::clone(&state), Arc::clone(&ring_a));
    let ring_b = Arc::new(PcmRing::new(1, 64));
    state.replace_ring(ring_b);
    drop(session);
    match rx.try_recv() {
      Err(_) => {},
      Ok(ev) => panic!("stale drop must not emit Ended, got {ev:?}"),
    }
  }

  #[test]
  fn flush_emits_pause_and_pcm_resumes() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let state = Arc::new(DeviceAudioState {
      ring_slot: Mutex::new(Arc::new(PcmRing::new(2, 64))),
      event_tx: Some(tx),
      device_id: "dev-flush".to_owned(),
    });
    let ring = state.current_ring();
    let mut session = RingSession::new(state, Arc::clone(&ring));
    session.audio_process(&[0.1_f32, 0.2]);
    // Drain the Started event is not from RingSession; only flush/pause/resume here.
    while rx.try_recv().is_ok() {}
    session.audio_flush();
    match rx.try_recv() {
      Ok(AirPlaySessionEvent::Flushed { device_id }) => assert_eq!(device_id, "dev-flush"),
      other => panic!("expected Flushed, got {other:?}"),
    }
    match rx.try_recv() {
      Ok(AirPlaySessionEvent::Paused { device_id }) => assert_eq!(device_id, "dev-flush"),
      other => panic!("expected Paused after flush, got {other:?}"),
    }
    session.audio_process(&[0.3_f32, 0.4]);
    match rx.try_recv() {
      Ok(AirPlaySessionEvent::Resumed { device_id }) => assert_eq!(device_id, "dev-flush"),
      other => panic!("expected Resumed after PCM, got {other:?}"),
    }
  }

  #[test]
  fn on_rate_pause_resume_and_on_flush() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let state = Arc::new(DeviceAudioState {
      ring_slot: Mutex::new(Arc::new(PcmRing::new(2, 64))),
      event_tx: Some(tx),
      device_id: "dev-rate".to_owned(),
    });
    let ring = state.current_ring();
    let mut session = RingSession::new(Arc::clone(&state), Arc::clone(&ring));
    session.audio_process(&[0.1_f32, 0.2]);
    while rx.try_recv().is_ok() {}

    session.on_rate(0);
    match rx.try_recv() {
      Ok(AirPlaySessionEvent::Paused { device_id }) => assert_eq!(device_id, "dev-rate"),
      other => panic!("expected Paused on rate 0, got {other:?}"),
    }
    // Dedupe: second pause is silent.
    session.on_rate(0);
    match rx.try_recv() {
      Err(_) => {},
      Ok(ev) => panic!("duplicate on_rate(0) must not emit, got {ev:?}"),
    }

    session.on_rate(1);
    match rx.try_recv() {
      Ok(AirPlaySessionEvent::Resumed { device_id }) => assert_eq!(device_id, "dev-rate"),
      other => panic!("expected Resumed on nonzero rate, got {other:?}"),
    }
    // Dedupe with note_pcm: already resumed, PCM must not double-send Resumed.
    session.audio_process(&[0.3_f32, 0.4]);
    match rx.try_recv() {
      Err(_) => {},
      Ok(ev) => panic!("note_pcm must not double Resumed after on_rate, got {ev:?}"),
    }

    session.on_flush();
    match rx.try_recv() {
      Ok(AirPlaySessionEvent::Flushed { device_id }) => assert_eq!(device_id, "dev-rate"),
      other => panic!("expected Flushed, got {other:?}"),
    }
    match rx.try_recv() {
      Ok(AirPlaySessionEvent::Paused { device_id }) => assert_eq!(device_id, "dev-rate"),
      other => panic!("expected Paused after on_flush, got {other:?}"),
    }
    assert_eq!(state.current_ring().available_frames(), 0);
  }
}
