//! AirPlay 2 receiver wrappers (one `RaopServer` per Chromecast).

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

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
/// No PCM for this long **and** an empty ring ⇒ treat AirPlay as paused (Cast PAUSE).
///
/// Buffered AP2 often gaps 1–2s between chunks while the track is still playing.
/// A short idle alone was thrashing Nest PAUSE/PLAY mid-song. Explicit AP2 rate=0
/// and FLUSH still pause immediately via [`AudioSession::audio_flush`].
const PAUSE_IDLE: Duration = Duration::from_millis(2_500);
/// Pause-watch poll cadence.
const PAUSE_POLL: Duration = Duration::from_millis(50);

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
/// Idle pause is inferred only when PCM stops for [`PAUSE_IDLE`] **and** the ring is
/// empty (AP2 rate=0 often stops delivery without dropping the session). Flush is
/// explicit via [`AudioSession::audio_flush`] (immediate Cast PAUSE path).
struct RingSession {
  state: Arc<DeviceAudioState>,
  ring: Arc<PcmRing>,
  last_process_ms: Arc<AtomicU64>,
  /// At least one PCM buffer has been delivered (avoids false pause before playout).
  had_pcm: Arc<AtomicBool>,
  cast_paused: Arc<AtomicBool>,
  watch_cancel: Arc<AtomicBool>,
  watch: Option<std::thread::JoinHandle<()>>,
}

impl RingSession {
  fn new(state: Arc<DeviceAudioState>, ring: Arc<PcmRing>) -> Self {
    let last_process_ms = Arc::new(AtomicU64::new(millis_since_start()));
    let had_pcm = Arc::new(AtomicBool::new(false));
    let cast_paused = Arc::new(AtomicBool::new(false));
    let watch_cancel = Arc::new(AtomicBool::new(false));
    let watch = spawn_pause_watch(
      Arc::clone(&state),
      Arc::clone(&last_process_ms),
      Arc::clone(&had_pcm),
      Arc::clone(&cast_paused),
      Arc::clone(&watch_cancel),
    );
    Self {
      state,
      ring,
      last_process_ms,
      had_pcm,
      cast_paused,
      watch_cancel,
      watch: Some(watch),
    }
  }

  fn note_pcm(&self) {
    self.had_pcm.store(true, Ordering::Release);
    self.last_process_ms.store(millis_since_start(), Ordering::Release);
    if self.cast_paused.swap(false, Ordering::AcqRel)
      && let Some(tx) = &self.state.event_tx
    {
      drop(tx.send(AirPlaySessionEvent::Resumed { device_id: self.state.device_id.clone() }));
    }
  }
}

impl AudioSession for RingSession {
  fn audio_process(&mut self, samples: &[f32]) {
    self.note_pcm();
    self.ring.push_f32(samples);
  }

  fn audio_flush(&mut self) {
    self.ring.clear();
    self.last_process_ms.store(millis_since_start(), Ordering::Release);
    if let Some(tx) = &self.state.event_tx {
      drop(tx.send(AirPlaySessionEvent::Flushed { device_id: self.state.device_id.clone() }));
    }
    if !self.cast_paused.swap(true, Ordering::AcqRel)
      && let Some(tx) = &self.state.event_tx
    {
      drop(tx.send(AirPlaySessionEvent::Paused { device_id: self.state.device_id.clone() }));
    }
  }
}

impl Drop for RingSession {
  fn drop(&mut self) {
    self.watch_cancel.store(true, Ordering::Release);
    if let Some(handle) = self.watch.take() {
      drop(handle.join());
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

fn millis_since_start() -> u64 {
  static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
  let start = START.get_or_init(Instant::now);
  u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Whether the idle watcher should emit Cast PAUSE (pure; unit-tested).
///
/// Requires long enough silence **and** no residual PCM in the ring so buffered
/// AP2 chunk gaps do not thrash Nest while audio is still queued for `LiveWav`.
#[must_use]
const fn should_emit_idle_pause(idle_ms: u64, idle_threshold_ms: u64, ring_frames: usize) -> bool {
  idle_ms >= idle_threshold_ms && ring_frames == 0
}

fn spawn_pause_watch(
  state: Arc<DeviceAudioState>,
  last_process_ms: Arc<AtomicU64>,
  had_pcm: Arc<AtomicBool>,
  cast_paused: Arc<AtomicBool>,
  cancel: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
  let idle_threshold_ms = u64::try_from(PAUSE_IDLE.as_millis()).unwrap_or(2_500);
  std::thread::Builder::new()
    .name(format!("ap-pause-{}", short_device_id(&state.device_id)))
    .spawn(move || {
      while !cancel.load(Ordering::Acquire) {
        std::thread::sleep(PAUSE_POLL);
        if cancel.load(Ordering::Acquire) {
          break;
        }
        if !had_pcm.load(Ordering::Acquire) {
          continue;
        }
        let last = last_process_ms.load(Ordering::Acquire);
        let now = millis_since_start();
        let idle_ms = now.saturating_sub(last);
        let ring_frames = state.current_ring().available_frames();
        if !should_emit_idle_pause(idle_ms, idle_threshold_ms, ring_frames) {
          continue;
        }
        if !cast_paused.swap(true, Ordering::AcqRel)
          && let Some(tx) = &state.event_tx
        {
          drop(tx.send(AirPlaySessionEvent::Paused { device_id: state.device_id.clone() }));
        }
      }
    })
    .unwrap_or_else(|_| {
      // Spawn failed: fall back to a no-op join handle via a finished thread.
      std::thread::spawn(|| {})
    })
}

fn short_device_id(id: &str) -> String {
  id.chars().take(8).collect()
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
  pub fn remove(&self, device_id: &str) {
    let removed = {
      let mut guard = self.receivers.lock();
      guard.remove(device_id)
    };
    if let Some(mut rx) = removed {
      tracing::info!(device_id = %rx.device_id, name = %rx.name, "AirPlay receiver withdrawn");
      // Prefer async hard stop when a tokio runtime is available.
      if let Ok(handle) = tokio::runtime::Handle::try_current() {
        drop(handle.spawn(async move {
          rx.shutdown_hard().await;
        }));
      } else {
        crate::net::force_close_tcp_on_local_port(rx.port);
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
  fn should_emit_idle_pause_requires_idle_and_empty_ring() {
    let threshold = 2_500_u64;
    // Buffered AP2 chunk gap with residual PCM: do not pause Cast.
    assert!(!should_emit_idle_pause(3_000, threshold, 1));
    assert!(!should_emit_idle_pause(threshold, threshold, 100));
    // Short gap under threshold even with empty ring: still playing.
    assert!(!should_emit_idle_pause(749, threshold, 0));
    assert!(!should_emit_idle_pause(2_499, threshold, 0));
    // Long silence and drained ring: real idle / rate=0 with no tail.
    assert!(should_emit_idle_pause(threshold, threshold, 0));
    assert!(should_emit_idle_pause(threshold + 1, threshold, 0));
  }

  #[test]
  fn pause_idle_is_above_typical_buffered_gaps() {
    // Documented product bar: longer than the old 750ms thrash threshold.
    assert!(PAUSE_IDLE.as_millis() >= 2_000);
  }
}
