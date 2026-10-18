//! AirPlay 2 receiver wrappers (one `RaopServer` per Chromecast).

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use shairplay::{AudioFormat, AudioHandler, AudioSession, MediaRemoteCommand, RaopServer, SessionGroup, TrackMetadata};
use tokio::sync::mpsc;

use crate::audio::PcmRing;
use crate::error::{Error, Result};
use crate::media::{Artwork, CAST_READ_AHEAD, MAX_ARTWORK_BYTES};

/// Base TCP port for RAOP listeners; each device gets `BASE + (hash % PORT_SPAN)`.
const RAOP_PORT_BASE: u16 = 5100;
const RAOP_PORT_SPAN: u64 = 1000;
/// Placeholder stereo capacity (~2 s at 48 kHz) until `audio_init` sets the real layout.
const DEFAULT_RING_FRAMES: usize = 48_000 * 2;
/// Headroom in the session PCM ring beyond [`CAST_READ_AHEAD`], in seconds of output audio.
///
/// shairplay releases buffered audio on the sender's anchor timeline at presentation time
/// minus [`CAST_READ_AHEAD`] and back-pressures the sender over TCP, so the producer is
/// real-time paced. The ring only ever holds:
/// - up to [`CAST_READ_AHEAD`] released at once when an anchor arrives (the burst a fresh
///   Cast LOAD or re-LOAD pulls immediately);
/// - real PCM produced while Cast is not pulling yet (initial LOAD, re-LOAD, FLAC to WAV
///   fallback), typically 1–4 s.
///
/// The read-ahead plus 8 s covers both with headroom. Overflow means Cast stopped pulling for
/// longer than that; dropping the oldest audio is then the correct live-stream behavior.
const SESSION_RING_HEADROOM_SECONDS: u64 = 8;
/// Product max channels for Cast stereo path.
const OUTPUT_MAX_CHANNELS: u8 = 2;
/// RTSP `Audio-Latency` advertised on RECORD (samples at stream rate).
///
/// The output delay the sender is told about. Buffered audio is released earlier than this,
/// [`CAST_READ_AHEAD`] before presentation, so a Cast (re)start can burst it; the speaker then
/// plays up to that much ahead of the sender's timeline (no A/V sync promise). Stream rate is not
/// known at receiver build (passthrough): 2 s at 48 kHz, ~2.18 s at 44.1 kHz.
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
    /// Sender's `groupUUID` when it selected several receivers together
    /// (`isMultiSelectAirPlay`); `None` for a single-receiver stream.
    ///
    /// Every PlayOnAir receiver of one multi-select carries the same id, so the bridge can
    /// play them as one Cast multizone group.
    group: Option<String>,
  },
  /// Client disconnected; bridge should Cast-STOP and drop media.
  Ended {
    /// Device id this receiver is bound to.
    device_id: String,
  },
  /// AirPlay playout rate went to 0 (or a flush); Cast should PAUSE promptly.
  Paused {
    /// Device id this receiver is bound to.
    device_id: String,
    /// When the receiver got the rate 0 (or flush), for pause latency diagnostics.
    at: Instant,
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
  /// The sender's now-playing title / artist / album or artwork changed.
  NowPlaying {
    /// Device id this receiver is bound to.
    device_id: String,
    /// Full now-playing state after the change.
    now_playing: NowPlaying,
  },
}

/// Now-playing state the sender last reported for a receiver.
#[derive(Debug, Clone, Default)]
pub struct NowPlaying {
  /// Track title.
  pub title: Option<String>,
  /// Track artist.
  pub artist: Option<String>,
  /// Album name.
  pub album: Option<String>,
  /// Album artwork (a new image gets a new id).
  pub artwork: Option<Arc<Artwork>>,
}

/// Process-unique artwork ids, so each image has its own `/artwork/<id>` URL.
static NEXT_ARTWORK_ID: AtomicU64 = AtomicU64::new(1);

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

  /// If `ring` is still the live slot, clear it, install a placeholder, and emit
  /// [`AirPlaySessionEvent::Ended`] under the same `ring_slot` lock that
  /// [`Self::replace_ring`] uses.
  ///
  /// Sending `Ended` inside the critical section is load-bearing: a concurrent
  /// `audio_init` cannot publish `Started` until this lock is released, so the
  /// bridge never sees `Started` then a stale `Ended` for the same device.
  /// Returns whether this session owned the slot and emitted `Ended`.
  fn end_if_current(&self, ring: &Arc<PcmRing>) -> bool {
    let mut slot = self.ring_slot.lock();
    if !Arc::ptr_eq(&*slot, ring) {
      return false;
    }
    ring.clear();
    // Release ownership while still holding the lock. Placeholder keeps
    // `current_ring()` valid for idle readers until the next `audio_init`.
    *slot = Arc::new(PcmRing::new(ring.channels(), DEFAULT_RING_FRAMES));
    if let Some(tx) = &self.event_tx {
      // Send while `slot` is still held so concurrent `audio_init` cannot
      // publish `Started` before this `Ended` (TOCTOU if send is after unlock).
      drop(tx.send(AirPlaySessionEvent::Ended { device_id: self.device_id.clone() }));
    }
    drop(slot);
    true
  }

  /// Test/helpers: claim end without emitting (ownership release only).
  #[cfg(test)]
  fn claim_end_if_current(&self, ring: &Arc<PcmRing>) -> bool {
    let mut slot = self.ring_slot.lock();
    if !Arc::ptr_eq(&*slot, ring) {
      return false;
    }
    *slot = Arc::new(PcmRing::new(ring.channels(), DEFAULT_RING_FRAMES));
    true
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
      drop(tx.send(AirPlaySessionEvent::Paused {
        device_id: self.state.device_id.clone(),
        at: Instant::now(),
      }));
    }
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
        drop(tx.send(AirPlaySessionEvent::Paused {
          device_id: self.state.device_id.clone(),
          at: Instant::now(),
        }));
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
    // Delivery does drop-then-replace on format change (Ended then Started).
    // Cross-connection SETUP may replace-then-drop. Decide and send Ended under
    // the same `ring_slot` lock as `replace_ring` so a superseded session can
    // never emit Ended after a later Started (claim-then-send outside the lock
    // still races).
    if !self.state.end_if_current(&self.ring) {
      tracing::debug!(
        device_id = %self.state.device_id,
        "AirPlay audio session dropped (stale ring; suppress Ended)"
      );
      return;
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
  now_playing: Arc<Mutex<NowPlaying>>,
  /// Multi-select group of the sender's latest initial SETUP (attached to the next `Started`).
  group: Mutex<Option<String>>,
}

impl RingHandler {
  fn new(state: Arc<DeviceAudioState>) -> Self {
    Self {
      state,
      now_playing: Arc::new(Mutex::new(NowPlaying::default())),
      group: Mutex::new(None),
    }
  }

  fn emit_now_playing(&self, now_playing: NowPlaying) {
    if let Some(tx) = &self.state.event_tx {
      drop(tx.send(AirPlaySessionEvent::NowPlaying {
        device_id: self.state.device_id.clone(),
        now_playing,
      }));
    }
  }
}

impl AudioHandler for RingHandler {
  fn audio_init(&self, format: AudioFormat) -> Box<dyn AudioSession> {
    let sample_rate = format.sample_rate.max(1);
    // Cap at product stereo; shairplay also mixdowns via output_max_channels(2).
    let channels = u16::from(format.channels.clamp(1, OUTPUT_MAX_CHANNELS));
    let capacity_frames = session_ring_frames(sample_rate);
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
        group: self.group.lock().clone(),
      }));
    }
    Box::new(RingSession::new(Arc::clone(&self.state), ring))
  }

  fn on_session_group(&self, group: &SessionGroup) {
    let multi_select = group.group_uuid.clone().filter(|_| group.is_multi_select);
    tracing::debug!(
      device_id = %self.state.device_id,
      group_uuid = ?group.group_uuid,
      multi_select = group.is_multi_select,
      "AirPlay session group"
    );
    *self.group.lock() = multi_select;
  }

  fn on_volume(&self, volume: f32) {
    if let Some(tx) = &self.state.event_tx {
      drop(tx.send(AirPlaySessionEvent::Volume {
        device_id: self.state.device_id.clone(),
        volume_db: volume,
      }));
    }
  }

  fn on_metadata(&self, metadata: &TrackMetadata) {
    let snapshot = {
      let mut now_playing = self.now_playing.lock();
      if now_playing.title == metadata.title
        && now_playing.artist == metadata.artist
        && now_playing.album == metadata.album
      {
        return;
      }
      now_playing.title.clone_from(&metadata.title);
      now_playing.artist.clone_from(&metadata.artist);
      now_playing.album.clone_from(&metadata.album);
      now_playing.clone()
    };
    tracing::info!(
      device_id = %self.state.device_id,
      title = ?snapshot.title,
      artist = ?snapshot.artist,
      album = ?snapshot.album,
      "AirPlay now playing"
    );
    self.emit_now_playing(snapshot);
  }

  fn on_coverart(&self, mime_type: &str, coverart: &[u8]) {
    if coverart.len() > MAX_ARTWORK_BYTES {
      tracing::warn!(
        device_id = %self.state.device_id,
        bytes = coverart.len(),
        max = MAX_ARTWORK_BYTES,
        "AirPlay artwork too large; ignoring"
      );
      return;
    }
    let snapshot = {
      let mut now_playing = self.now_playing.lock();
      if now_playing
        .artwork
        .as_ref()
        .is_some_and(|art| art.content_type == mime_type && art.body.as_ref() == coverart)
      {
        return;
      }
      now_playing.artwork = Some(Arc::new(Artwork {
        id: NEXT_ARTWORK_ID.fetch_add(1, Ordering::Relaxed),
        content_type: mime_type.to_owned(),
        body: Bytes::copy_from_slice(coverart),
      }));
      now_playing.clone()
    };
    tracing::debug!(device_id = %self.state.device_id, mime_type, bytes = coverart.len(), "AirPlay artwork");
    self.emit_now_playing(snapshot);
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

/// Network options applied to every [`AirPlayReceiver`] a manager builds.
///
/// The product uses [`ReceiverOptions::default`]. In-process tests disable mDNS and use
/// ephemeral PTP ports so a receiver touches nothing beyond its own listeners.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiverOptions {
  /// Register the `_raop._tcp` / `_airplay._tcp` mDNS services (product: `true`).
  pub advertise: bool,
  /// PTP `(event, general)` UDP ports; `None` keeps the AirPlay standard 319/320, `0` binds
  /// an ephemeral port.
  pub ptp_ports: Option<(u16, u16)>,
}

impl Default for ReceiverOptions {
  fn default() -> Self {
    Self { advertise: true, ptp_ports: None }
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
  /// Shared with the handler; read when the bridge builds Cast metadata.
  now_playing: Arc<Mutex<NowPlaying>>,
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
    options: ReceiverOptions,
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
    let handler = Arc::new(RingHandler::new(Arc::clone(&state)));
    let now_playing = Arc::clone(&handler.now_playing);

    let hwaddr = stable_hwaddr(&device_id_owned);
    let port = stable_raop_port(&device_id_owned);

    let mut builder = RaopServer::builder()
      .name(name_owned.clone())
      .hwaddr(hwaddr.to_vec())
      .port(port)
      .output_max_channels(OUTPUT_MAX_CHANNELS)
      .audio_latency_samples(AUDIO_LATENCY_SAMPLES)
      .release_lead(CAST_READ_AHEAD)
      .advertise(options.advertise);
    if let Some((event, general)) = options.ptp_ports {
      builder = builder.ptp_ports(event, general);
    }
    let server = builder
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
      now_playing,
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

  /// What the sender last reported as now playing.
  pub fn now_playing(&self) -> NowPlaying {
    self.now_playing.lock().clone()
  }

  /// Report a Cast-side volume change to the sender (`dvlc`) and to `GET_PARAMETER volume`.
  ///
  /// `level` is the Cast level, which maps one to one onto the iOS slider.
  pub fn report_volume(&self, level: f32, muted: bool) -> Result<()> {
    self.volume_seeded_from_cast.store(true, Ordering::Relaxed);
    self
      .server
      .report_volume(level, muted)
      .map_err(|err| Error::AirPlay(format!("report volume for {}: {err}", self.name)))
  }

  /// Ask the sender to perform a playback command (`paus`, `play`, …).
  pub fn send_media_remote_command(&self, command: MediaRemoteCommand) -> Result<()> {
    self
      .server
      .send_media_remote_command(command)
      .map_err(|err| Error::AirPlay(format!("send {} to {}: {err}", command.code(), self.name)))
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
  options: ReceiverOptions,
}

impl AirPlayManager {
  /// Create a manager that forwards session lifecycle events on `event_tx`.
  pub fn new(event_tx: Option<mpsc::UnboundedSender<AirPlaySessionEvent>>) -> Self {
    Self {
      receivers: Mutex::new(HashMap::new()),
      kicking: Mutex::new(HashSet::new()),
      event_tx,
      options: ReceiverOptions::default(),
    }
  }

  /// Build every receiver with `options` instead of the product defaults.
  #[must_use]
  pub const fn with_receiver_options(mut self, options: ReceiverOptions) -> Self {
    self.options = options;
    self
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

    let mut rx = AirPlayReceiver::build(device_id, airplay_name, self.event_tx.clone(), self.options)?;
    rx.start().await?;
    {
      let mut guard = self.receivers.lock();
      drop(guard.insert(device_id.to_owned(), rx));
    }
    Ok(())
  }

  /// Stop and drop the receiver for `device_id` (best-effort hard stop when in a runtime).
  ///
  /// Hard-stop is **awaited** (with [`REMOVE_SHUTDOWN_TIMEOUT`]) so a following
  /// [`Self::ensure`] does not race the RAOP port. Safe on multi-thread runtimes
  /// (`block_in_place`) and current-thread / no-runtime callers (dedicated helper
  /// thread — `block_in_place` panics on current-thread).
  pub fn remove(&self, device_id: &str) {
    let removed = {
      let mut guard = self.receivers.lock();
      guard.remove(device_id)
    };
    if let Some(rx) = removed {
      tracing::info!(device_id = %rx.device_id, name = %rx.name, "AirPlay receiver withdrawn");
      Self::await_shutdown_hard(rx);
    }
  }

  /// Await hard-stop without panicking on current-thread runtimes.
  fn await_shutdown_hard(mut rx: AirPlayReceiver) {
    let device_id = rx.device_id.clone();
    let port = rx.port;
    match tokio::runtime::Handle::try_current() {
      Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
        // Product path: `#[tokio::main]` multi-thread. `block_in_place` frees the
        // worker so other tasks (including hard-stop work) can run.
        tokio::task::block_in_place(|| {
          handle.block_on(async {
            if tokio::time::timeout(REMOVE_SHUTDOWN_TIMEOUT, rx.shutdown_hard()).await.is_err() {
              tracing::warn!(
                device_id = %device_id,
                port,
                "AirPlay hard-stop timed out during remove; force-closing port"
              );
              crate::net::force_close_tcp_on_local_port(port);
            }
          });
        });
      },
      Ok(_) | Err(_) => {
        // Current-thread: cannot `block_in_place` or `Handle::spawn` + recv (only
        // worker would deadlock). No runtime: need one. Dedicated OS thread with
        // its own current-thread runtime owns the wait.
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let helper_id = device_id.clone();
        let spawn_result = std::thread::Builder::new()
          .name("airplay-remove-shutdown".to_owned())
          .spawn(move || {
            match tokio::runtime::Builder::new_current_thread().enable_all().build() {
              Ok(rt) => {
                rt.block_on(async {
                  if tokio::time::timeout(REMOVE_SHUTDOWN_TIMEOUT, rx.shutdown_hard()).await.is_err() {
                    tracing::warn!(
                      device_id = %helper_id,
                      port,
                      "AirPlay hard-stop timed out during remove; force-closing port"
                    );
                    crate::net::force_close_tcp_on_local_port(port);
                  }
                });
              },
              Err(err) => {
                tracing::warn!(
                  device_id = %helper_id,
                  port,
                  error = %err,
                  "failed to build runtime for AirPlay remove; force-closing port"
                );
                crate::net::force_close_tcp_on_local_port(port);
                drop(rx);
              },
            }
            // Result is `Copy` for `()` payload; consume via `is_err` (not `drop`/`let _`).
            if done_tx.send(()).is_err() {
              tracing::debug!(
                device_id = %helper_id,
                "AirPlay remove helper: done channel closed before signal"
              );
            }
          });
        match spawn_result {
          Ok(join_handle) => {
            let wait = REMOVE_SHUTDOWN_TIMEOUT.saturating_add(Duration::from_secs(1));
            if done_rx.recv_timeout(wait).is_ok() {
              if let Err(err) = join_handle.join() {
                tracing::warn!(
                  device_id = %device_id,
                  ?err,
                  "AirPlay remove helper thread panicked"
                );
              }
            } else {
              tracing::warn!(
                device_id = %device_id,
                port,
                "AirPlay hard-stop helper timed out; force-closing port"
              );
              crate::net::force_close_tcp_on_local_port(port);
            }
          },
          Err(err) => {
            tracing::warn!(
              device_id = %device_id,
              port,
              error = %err,
              "failed to spawn AirPlay remove helper; force-closing port"
            );
            crate::net::force_close_tcp_on_local_port(port);
          },
        }
      },
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
    let mut replacement = AirPlayReceiver::build(device_id, &airplay_name, self.event_tx.clone(), self.options)?;
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

  /// What the sender last reported as now playing for `device_id`, if advertised.
  pub fn now_playing(&self, device_id: &str) -> Option<NowPlaying> {
    self.receivers.lock().get(device_id).map(AirPlayReceiver::now_playing)
  }

  /// Report a Cast-side volume / mute change to `device_id`'s sender.
  pub fn report_volume(&self, device_id: &str, level: f32, muted: bool) -> Result<()> {
    self
      .receivers
      .lock()
      .get(device_id)
      .ok_or_else(|| Error::AirPlay(format!("no AirPlay receiver for {device_id}")))?
      .report_volume(level, muted)
  }

  /// Ask `device_id`'s sender to perform a playback command.
  pub fn send_media_remote_command(&self, device_id: &str, command: MediaRemoteCommand) -> Result<()> {
    self
      .receivers
      .lock()
      .get(device_id)
      .ok_or_else(|| Error::AirPlay(format!("no AirPlay receiver for {device_id}")))?
      .send_media_remote_command(command)
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

/// Session PCM ring capacity at `sample_rate`: [`CAST_READ_AHEAD`] plus
/// [`SESSION_RING_HEADROOM_SECONDS`].
fn session_ring_frames(sample_rate: u32) -> usize {
  let seconds = CAST_READ_AHEAD.as_secs().saturating_add(SESSION_RING_HEADROOM_SECONDS);
  usize::try_from(u64::from(sample_rate.max(1)).saturating_mul(seconds)).unwrap_or(usize::MAX)
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

/// AirPlay mute sentinel in dB (`GET_PARAMETER` / `SET_PARAMETER`).
const AIRPLAY_VOLUME_DB_MIN: f32 = -144.0;
/// Bottom of the audible AirPlay range: iOS slider position 0. Apple mutes at or below it.
const AIRPLAY_VOLUME_DB_FLOOR: f32 = -30.0;

/// Map AirPlay volume (dB, `0` = max, `-30` = slider bottom, `-144` = mute) to Cast level.
///
/// Cast level is the iOS slider position: `(dB + 30) / 30`, clamped to `0..=1`, so the
/// slider drives the Cast device's (already perceptual) volume one to one. At or below
/// `-30` dB, including the `-144` mute sentinel, the level is `0` (Apple mutes there).
pub fn airplay_db_to_cast_linear(volume_db: f32) -> f32 {
  if volume_db.is_nan() || volume_db <= AIRPLAY_VOLUME_DB_FLOOR {
    return 0.0;
  }
  ((volume_db - AIRPLAY_VOLUME_DB_FLOOR) / -AIRPLAY_VOLUME_DB_FLOOR).clamp(0.0, 1.0)
}

/// Map Cast level (`0.0..=1.0`) to AirPlay dB: `-30 + 30 * level`, `-144` (mute) at `0`.
///
/// Inverse of [`airplay_db_to_cast_linear`] on `(0, 1]`.
pub fn cast_linear_to_airplay_db(level: f32) -> f32 {
  if !level.is_finite() || level <= 0.0 {
    return AIRPLAY_VOLUME_DB_MIN;
  }
  level.min(1.0).mul_add(-AIRPLAY_VOLUME_DB_FLOOR, AIRPLAY_VOLUME_DB_FLOOR)
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

  /// Spec 6: the iOS slider is linear in dB over `-30..0`; Cast level follows it one to one.
  #[test]
  fn airplay_db_to_cast_level_follows_slider() {
    for (db, level) in [
      (-144.0_f32, 0.0_f32),
      (-60.0, 0.0),
      (-30.0, 0.0),
      (-15.0, 0.5),
      (-7.5, 0.75),
      (0.0, 1.0),
    ] {
      let got = airplay_db_to_cast_linear(db);
      assert!((got - level).abs() < 1.0e-6, "dB {db}: expected {level}, got {got}");
    }
    assert!((airplay_db_to_cast_linear(3.0) - 1.0).abs() < f32::EPSILON, "above 0 dB clamps");
    assert!((airplay_db_to_cast_linear(f32::NAN) - 0.0).abs() < f32::EPSILON);
  }

  #[test]
  fn cast_level_to_airplay_db_inverse() {
    for (level, db) in [(0.0_f32, -144.0_f32), (0.5, -15.0), (1.0, 0.0), (1.4, 0.0)] {
      let got = cast_linear_to_airplay_db(level);
      assert!((got - db).abs() < 1.0e-5, "level {level}: expected {db}, got {got}");
    }
    // A tiny nonzero level is the slider bottom (-30 dB), not the mute sentinel.
    assert!((cast_linear_to_airplay_db(1.0e-6) - -30.0).abs() < 1.0e-3);
    assert!((cast_linear_to_airplay_db(f32::NAN) - AIRPLAY_VOLUME_DB_MIN).abs() < f32::EPSILON);
    for level in [0.1_f32, 0.25, 0.5, 0.75, 0.9] {
      let back = airplay_db_to_cast_linear(cast_linear_to_airplay_db(level));
      assert!((back - level).abs() < 1.0e-5, "roundtrip {level} -> {back}");
    }
  }

  /// Metadata and artwork are surfaced once per change; artwork gets a fresh id per image
  /// and oversized images are dropped.
  #[test]
  fn now_playing_events_dedupe_and_bound_artwork() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let state = Arc::new(DeviceAudioState {
      ring_slot: Mutex::new(Arc::new(PcmRing::new(2, 64))),
      event_tx: Some(tx),
      device_id: "dev-np".to_owned(),
    });
    let handler = RingHandler::new(state);
    let meta = TrackMetadata {
      title: Some("Song".to_owned()),
      artist: Some("Band".to_owned()),
      ..TrackMetadata::default()
    };
    handler.on_metadata(&meta);
    handler.on_metadata(&meta);
    let first = match rx.try_recv() {
      Ok(AirPlaySessionEvent::NowPlaying { device_id, now_playing }) => {
        assert_eq!(device_id, "dev-np");
        now_playing
      },
      other => panic!("expected NowPlaying, got {other:?}"),
    };
    assert_eq!(first.title.as_deref(), Some("Song"));
    assert_eq!(first.artist.as_deref(), Some("Band"));
    assert!(rx.try_recv().is_err(), "identical metadata must not re-emit");

    handler.on_coverart("image/jpeg", b"jpeg-1");
    let art1 = match rx.try_recv() {
      Ok(AirPlaySessionEvent::NowPlaying { now_playing, .. }) => now_playing.artwork.expect("artwork"),
      other => panic!("expected artwork NowPlaying, got {other:?}"),
    };
    assert_eq!(art1.content_type, "image/jpeg");
    handler.on_coverart("image/jpeg", b"jpeg-1");
    assert!(rx.try_recv().is_err(), "same image must not re-emit");
    handler.on_coverart("image/jpeg", b"jpeg-2");
    let art2 = match rx.try_recv() {
      Ok(AirPlaySessionEvent::NowPlaying { now_playing, .. }) => now_playing.artwork.expect("artwork"),
      other => panic!("expected artwork NowPlaying, got {other:?}"),
    };
    assert_ne!(art1.id, art2.id);

    handler.on_coverart("image/jpeg", &vec![0_u8; MAX_ARTWORK_BYTES + 1]);
    assert!(rx.try_recv().is_err(), "oversized artwork is dropped");
    assert_eq!(handler.now_playing.lock().artwork.as_ref().map(|a| a.id), Some(art2.id));
  }

  #[test]
  fn ring_handler_rebuilds_ring_for_format_channels() {
    let state = Arc::new(DeviceAudioState {
      ring_slot: Mutex::new(Arc::new(PcmRing::new(2, 64))),
      event_tx: None,
      device_id: "dev".to_owned(),
    });
    let handler = RingHandler::new(Arc::clone(&state));
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
  fn multi_select_group_rides_on_started() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let state = Arc::new(DeviceAudioState {
      ring_slot: Mutex::new(Arc::new(PcmRing::new(2, 64))),
      event_tx: Some(tx),
      device_id: "dev-group".to_owned(),
    });
    let handler = RingHandler::new(state);
    let format = AudioFormat {
      codec: shairplay::AudioCodec::Pcm,
      bits: 32,
      channels: 2,
      sample_rate: 44_100,
    };
    let started_group = |events: &mut mpsc::UnboundedReceiver<AirPlaySessionEvent>| match events.try_recv() {
      Ok(AirPlaySessionEvent::Started { group, .. }) => group,
      other => panic!("expected Started, got {other:?}"),
    };

    handler.on_session_group(&SessionGroup {
      group_uuid: Some("ABC-123".to_owned()),
      is_multi_select: true,
    });
    let first = handler.audio_init(format);
    assert_eq!(started_group(&mut rx).as_deref(), Some("ABC-123"));
    drop(first);
    drop(rx.try_recv());

    // A group id without multi-select (one receiver) is not a Cast group.
    handler.on_session_group(&SessionGroup {
      group_uuid: Some("ABC-123".to_owned()),
      is_multi_select: false,
    });
    let second = handler.audio_init(format);
    assert_eq!(started_group(&mut rx), None);
    drop(second);
  }

  #[test]
  fn format_change_second_init_suppresses_stale_ended() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let state = Arc::new(DeviceAudioState {
      ring_slot: Mutex::new(Arc::new(PcmRing::new(2, 64))),
      event_tx: Some(tx),
      device_id: "dev-fmt".to_owned(),
    });
    let handler = RingHandler::new(Arc::clone(&state));
    let fmt = |channels: u8| AudioFormat {
      codec: shairplay::AudioCodec::Pcm,
      bits: 32,
      channels,
      sample_rate: 44_100,
    };

    let session1 = handler.audio_init(fmt(1));
    match rx.try_recv() {
      Ok(AirPlaySessionEvent::Started { device_id, sample_rate, ring, .. }) => {
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
  fn claim_end_if_current_releases_ownership_under_lock() {
    let ring_a = Arc::new(PcmRing::new(2, 64));
    let state = Arc::new(DeviceAudioState {
      ring_slot: Mutex::new(Arc::clone(&ring_a)),
      event_tx: None,
      device_id: "dev-claim".to_owned(),
    });
    assert!(state.claim_end_if_current(&ring_a));
    assert!(!Arc::ptr_eq(&state.current_ring(), &ring_a));
    // Second claim on the same ring must fail after ownership was released.
    assert!(!state.claim_end_if_current(&ring_a));
  }

  #[test]
  fn claim_end_after_replace_returns_false() {
    let ring_a = Arc::new(PcmRing::new(2, 64));
    let state = Arc::new(DeviceAudioState {
      ring_slot: Mutex::new(Arc::clone(&ring_a)),
      event_tx: None,
      device_id: "dev-claim-stale".to_owned(),
    });
    state.replace_ring(Arc::new(PcmRing::new(1, 64)));
    assert!(!state.claim_end_if_current(&ring_a));
  }

  /// Two-thread race: old session Drop concurrent with new `audio_init`.
  ///
  /// Invariant: after both complete (and before the new session is dropped),
  /// events must never be `Started` then `Ended` from the superseded session
  /// (that order kills the live Cast session on the bridge).
  #[test]
  fn concurrent_drop_and_audio_init_never_started_then_ended() {
    use std::sync::Barrier;
    use std::thread;

    let fmt = AudioFormat {
      codec: shairplay::AudioCodec::Pcm,
      bits: 32,
      channels: 2,
      sample_rate: 44_100,
    };

    for iteration in 0..200 {
      let (tx, mut rx) = mpsc::unbounded_channel();
      let state = Arc::new(DeviceAudioState {
        ring_slot: Mutex::new(Arc::new(PcmRing::new(2, 64))),
        event_tx: Some(tx),
        device_id: "dev-race".to_owned(),
      });
      let handler = RingHandler::new(Arc::clone(&state));

      let session1 = handler.audio_init(fmt);
      match rx.try_recv() {
        Ok(AirPlaySessionEvent::Started { .. }) => {},
        other => panic!("iter {iteration}: expected first Started, got {other:?}"),
      }

      let barrier = Arc::new(Barrier::new(2));
      let barrier_drop = Arc::clone(&barrier);
      let drop_thread = thread::spawn(move || {
        let _ = barrier_drop.wait();
        drop(session1);
      });

      let _ = barrier.wait();
      let session2 = handler.audio_init(fmt);
      drop_thread
        .join()
        .unwrap_or_else(|_| panic!("iter {iteration}: drop thread panicked"));

      // Drain events before dropping session2 so any stale Ended is visible here.
      let mid: Vec<AirPlaySessionEvent> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
      let mut saw_second_started = false;
      for event in &mid {
        match event {
          AirPlaySessionEvent::Started { .. } => {
            saw_second_started = true;
          },
          AirPlaySessionEvent::Ended { .. } if saw_second_started => {
            panic!("iter {iteration}: Ended after second Started (stale Drop race); events={mid:?}");
          },
          AirPlaySessionEvent::Ended { .. }
          | AirPlaySessionEvent::Paused { .. }
          | AirPlaySessionEvent::Resumed { .. }
          | AirPlaySessionEvent::Flushed { .. }
          | AirPlaySessionEvent::Volume { .. }
          | AirPlaySessionEvent::NowPlaying { .. } => {},
        }
      }
      assert!(
        saw_second_started,
        "iter {iteration}: expected second Started in mid events, got {mid:?}"
      );

      drop(session2);
      match rx.try_recv() {
        Ok(AirPlaySessionEvent::Ended { device_id }) => assert_eq!(device_id, "dev-race"),
        other => panic!("iter {iteration}: expected final Ended, got {other:?}"),
      }
    }
  }

  #[tokio::test(flavor = "current_thread")]
  async fn remove_does_not_panic_on_current_thread_runtime() {
    let mgr = AirPlayManager::new(None);
    // Missing device is a no-op (never hits shutdown).
    mgr.remove("no-such-device");
    mgr
      .ensure("dev-cur-thread", "Current-Thread Test")
      .await
      .expect("ensure should bind a RAOP port in tests");
    // Must not panic: current-thread forbids `block_in_place`.
    mgr.remove("dev-cur-thread");
    assert!(
      !mgr.active_ids().iter().any(|id| id == "dev-cur-thread"),
      "receiver should be withdrawn after remove"
    );
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
      Ok(AirPlaySessionEvent::Paused { device_id, .. }) => assert_eq!(device_id, "dev-flush"),
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
      Ok(AirPlaySessionEvent::Paused { device_id, .. }) => assert_eq!(device_id, "dev-rate"),
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
      Ok(AirPlaySessionEvent::Paused { device_id, .. }) => assert_eq!(device_id, "dev-rate"),
      other => panic!("expected Paused after on_flush, got {other:?}"),
    }
    assert_eq!(state.current_ring().available_frames(), 0);
  }
}
