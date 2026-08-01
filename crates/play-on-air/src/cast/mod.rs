//! Google Cast control plane (media load + transport).
//!
//! Payload construction is unit-tested without a device. Live connect/load uses
//! `rust_cast` and stores the media session so play/pause/stop can target it.
//!
//! Prefer [`CastPool`] for production: it keeps a warm TCP control plane per device
//! so AirPlay sessions avoid dialing Nest during the AP2 black-hole window.

mod pool;

pub use pool::CastPool;

use rust_cast::channels::media::{Media, Metadata, MusicTrackMediaMetadata, Status, StreamType};
use serde_json::{Value, json};

use crate::error::{Error, Result};

/// Content type for lossless FLAC Cast media.
pub const CONTENT_TYPE_FLAC: &str = "audio/flac";

/// Content type for WAV / LPCM Cast media.
pub const CONTENT_TYPE_WAV: &str = "audio/wav";

/// Cast media namespace (wire).
pub const MEDIA_NAMESPACE: &str = "urn:x-cast:com.google.cast.media";

/// Cast receiver namespace (wire).
pub const RECEIVER_NAMESPACE: &str = "urn:x-cast:com.google.cast.receiver";

/// How Cast should stream the media URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastStreamKind {
  /// Progressive / finite file (BUFFERED).
  Buffered,
  /// Continuous live stream (LIVE).
  Live,
}

impl CastStreamKind {
  const fn to_stream_type(self) -> StreamType {
    match self {
      Self::Buffered => StreamType::Buffered,
      Self::Live => StreamType::Live,
    }
  }
}

/// Clamp a Cast volume level into the valid `0.0..=1.0` range.
pub const fn volume_level_clamped(level: f32) -> f32 {
  if level < 0.0 {
    0.0
  } else if level > 1.0 {
    1.0
  } else {
    level
  }
}

/// Pure builder for Cast `LOAD` media payloads (no network).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaLoadRequest {
  /// URL the Chromecast will HTTP-GET (our local media server).
  pub content_url: String,
  /// MIME type (`audio/flac` or `audio/wav`).
  pub content_type: String,
  /// Stream type for the player.
  pub stream_kind: CastStreamKind,
  /// Optional track title shown on the sink UI.
  pub title: Option<String>,
}

impl MediaLoadRequest {
  /// Build a FLAC load request.
  pub fn flac(url: impl Into<String>, kind: CastStreamKind) -> Self {
    Self {
      content_url: url.into(),
      content_type: CONTENT_TYPE_FLAC.to_owned(),
      stream_kind: kind,
      title: None,
    }
  }

  /// Build a WAV load request.
  pub fn wav(url: impl Into<String>, kind: CastStreamKind) -> Self {
    Self {
      content_url: url.into(),
      content_type: CONTENT_TYPE_WAV.to_owned(),
      stream_kind: kind,
      title: None,
    }
  }

  /// Attach a display title.
  pub fn with_title(mut self, title: impl Into<String>) -> Self {
    self.title = Some(title.into());
    self
  }

  /// Convert to `rust_cast` [`Media`] for wire send.
  pub fn to_media(&self) -> Media {
    let metadata = self.title.as_ref().map(|title| {
      Metadata::MusicTrack(MusicTrackMediaMetadata {
        title: Some(title.clone()),
        artist: None,
        album_name: None,
        album_artist: None,
        composer: None,
        track_number: None,
        disc_number: None,
        images: vec![],
        release_date: None,
      })
    });
    Media {
      content_id: self.content_url.clone(),
      stream_type: self.stream_kind.to_stream_type(),
      content_type: self.content_type.clone(),
      metadata,
      duration: None,
    }
  }

  /// Cast media-channel LOAD body shape (request fields only; requestId filled by sender).
  pub fn to_load_message_body(&self, session_id: &str, request_id: u32) -> Value {
    let media = self.to_media();
    json!({
      "type": "LOAD",
      "requestId": request_id,
      "sessionId": session_id,
      "autoplay": true,
      "media": {
        "contentId": media.content_id,
        "streamType": match media.stream_type {
          StreamType::Buffered => "BUFFERED",
          StreamType::Live => "LIVE",
          StreamType::None => "NONE",
        },
        "contentType": media.content_type,
      }
    })
  }
}

/// Identifiers required by Cast media transport commands (play/pause/stop).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaSessionRef {
  /// Cast app transport id.
  pub transport_id: String,
  /// Media session id returned by LOAD/STATUS.
  pub media_session_id: i32,
}

impl MediaSessionRef {
  /// Construct a session reference used by pure transport helpers.
  pub fn new(transport_id: impl Into<String>, media_session_id: i32) -> Self {
    Self {
      transport_id: transport_id.into(),
      media_session_id,
    }
  }
}

/// Active Cast media session after a successful LOAD.
pub type ActiveCastSession = MediaSessionRef;

/// Pure record of a play/pause/stop command (no network).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportCommand {
  /// Pause media.
  Pause(MediaSessionRef),
  /// Resume media.
  Play(MediaSessionRef),
  /// Stop media.
  Stop(MediaSessionRef),
}

impl TransportCommand {
  /// Cast media-channel wire JSON for this command (matches `rust_cast` message type fields).
  pub fn to_media_message_body(&self, request_id: u32) -> Value {
    let (typ, session) = match self {
      Self::Pause(s) => ("PAUSE", s),
      Self::Play(s) => ("PLAY", s),
      Self::Stop(s) => ("STOP", s),
    };
    json!({
      "type": typ,
      "requestId": request_id,
      "mediaSessionId": session.media_session_id,
    })
  }

  /// Transport id the command targets.
  pub const fn transport_id(&self) -> &str {
    match self {
      Self::Pause(s) | Self::Play(s) | Self::Stop(s) => s.transport_id.as_str(),
    }
  }
}

/// Cast receiver `SET_VOLUME` wire body.
pub fn set_volume_message_body(level: f32, request_id: u32) -> Value {
  let clamped = volume_level_clamped(level);
  json!({
    "type": "SET_VOLUME",
    "requestId": request_id,
    "volume": {
      "level": f64::from(clamped),
    }
  })
}

/// Extract media session id from a LOAD/STATUS response (shipped helper used by `connect_and_load`).
pub fn media_session_id_from_status(status: &Status) -> Result<i32> {
  if let Some(entry) = status.entries.first() {
    return Ok(entry.media_session_id);
  }
  Err(Error::Cast("LOAD status had no media session entries".to_owned()))
}

/// Thin wrapper around Cast session operations.
#[derive(Debug)]
pub struct CastController {
  /// Device host or IP.
  pub host: String,
  /// Optional mDNS hostname for re-resolution on connect retry.
  pub hostname: Option<String>,
  /// Cast port (typically 8009).
  pub port: u16,
  /// Last built load request (for introspection / tests).
  last_load: Option<MediaLoadRequest>,
  /// Last volume level prepared via [`Self::prepare_volume`] (tests / dry-run).
  last_volume: Option<f32>,
  /// Last transport command prepared without network.
  last_transport: Option<TransportCommand>,
  /// Active media session after a successful [`Self::connect_and_load`].
  active: Option<ActiveCastSession>,
  /// Optional background heartbeat task (stopped on drop / `stop_active`).
  heartbeat_stop: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
}

impl CastController {
  /// Create a controller targeting `host:port`.
  pub fn new(host: impl Into<String>, port: u16) -> Self {
    Self {
      host: host.into(),
      hostname: None,
      port,
      last_load: None,
      last_volume: None,
      last_transport: None,
      active: None,
      heartbeat_stop: None,
    }
  }

  /// Attach an mDNS hostname used to refresh `host` before connect retries.
  pub fn with_hostname(mut self, hostname: impl Into<String>) -> Self {
    let h = hostname.into();
    if !h.is_empty() {
      self.hostname = Some(h);
    }
    self
  }

  /// Refresh `host` from `hostname` when resolution yields an IPv4 address.
  pub fn refresh_host(&mut self) {
    let Some(hn) = self.hostname.as_ref() else {
      return;
    };
    if let Some(ip) = crate::net::resolve_host_ipv4(hn)
      && ip != self.host
    {
      tracing::info!(old = %self.host, new = %ip, hostname = %hn, "refreshed Cast host IP");
      self.host = ip;
    }
  }

  /// Active Cast media session, if LOAD succeeded.
  pub const fn active_session(&self) -> Option<&ActiveCastSession> {
    self.active.as_ref()
  }

  /// Test-only: install an active session without LOAD (for teardown path tests).
  #[cfg(test)]
  pub fn set_active_for_test(&mut self, session: MediaSessionRef) {
    self.active = Some(session);
  }

  /// Record a media load payload (pure; no network).
  pub fn prepare_load(&mut self, request: MediaLoadRequest) -> Media {
    let media = request.to_media();
    self.last_load = Some(request);
    media
  }

  /// Record a clamped volume level (pure; no network).
  pub const fn prepare_volume(&mut self, level: f32) -> f32 {
    let clamped = volume_level_clamped(level);
    self.last_volume = Some(clamped);
    clamped
  }

  /// Record a pause command (pure; no network).
  pub fn prepare_pause(&mut self, session: MediaSessionRef) -> TransportCommand {
    let cmd = TransportCommand::Pause(session);
    self.last_transport = Some(cmd.clone());
    cmd
  }

  /// Record a play command (pure; no network).
  pub fn prepare_play(&mut self, session: MediaSessionRef) -> TransportCommand {
    let cmd = TransportCommand::Play(session);
    self.last_transport = Some(cmd.clone());
    cmd
  }

  /// Record a stop command (pure; no network).
  pub fn prepare_stop(&mut self, session: MediaSessionRef) -> TransportCommand {
    let cmd = TransportCommand::Stop(session);
    self.last_transport = Some(cmd.clone());
    cmd
  }

  /// Last prepared load, if any.
  pub const fn last_load(&self) -> Option<&MediaLoadRequest> {
    self.last_load.as_ref()
  }

  /// Last prepared volume, if any.
  pub const fn last_volume(&self) -> Option<f32> {
    self.last_volume
  }

  /// Last prepared transport command, if any.
  pub const fn last_transport(&self) -> Option<&TransportCommand> {
    self.last_transport.as_ref()
  }

  /// Connect to the device, launch default media receiver, load media, and store the session.
  ///
  /// Returns the active session (transport id + media session id) needed for play/pause/stop.
  ///
  /// Retries on transient network errors (Nest sleep / intermittent "No route to host").
  pub fn connect_and_load(&mut self, request: MediaLoadRequest) -> Result<ActiveCastSession> {
    let media = self.prepare_load(request);
    let port = self.port;
    let session = self.with_device_retry(|device| {
      device
        .connection
        .connect("receiver-0")
        .map_err(|err| Error::Cast(format!("connection channel: {err}")))?;

      device
        .heartbeat
        .ping()
        .map_err(|err| Error::Cast(format!("heartbeat: {err}")))?;

      let app = device
        .receiver
        .launch_app(&rust_cast::channels::receiver::CastDeviceApp::DefaultMediaReceiver)
        .map_err(|err| Error::Cast(format!("launch app: {err}")))?;

      device
        .connection
        .connect(app.transport_id.as_str())
        .map_err(|err| Error::Cast(format!("app connection: {err}")))?;

      let status = device
        .media
        .load(app.transport_id.as_str(), app.session_id.as_str(), &media)
        .map_err(|err| Error::Cast(format!("media load: {err}")))?;

      let media_session_id = media_session_id_from_status(&status)?;
      Ok(ActiveCastSession::new(app.transport_id, media_session_id))
    })?;

    self.active = Some(session.clone());
    tracing::info!(
      host = %self.host,
      port,
      transport_id = %session.transport_id,
      media_session_id = session.media_session_id,
      url = %media.content_id,
      "Cast media loaded"
    );
    Ok(session)
  }

  /// Set receiver volume level in `0.0..=1.0` (real wire path via `rust_cast`).
  pub fn set_volume(&mut self, level: f32) -> Result<()> {
    let clamped = self.prepare_volume(level);
    // Record the wire body shape for tests / debugging.
    let _wire = set_volume_message_body(clamped, 0);
    self.with_device(|device| {
      device
        .connection
        .connect("receiver-0")
        .map_err(|err| Error::Cast(format!("connection channel: {err}")))?;
      let volume = device
        .receiver
        .set_volume(clamped)
        .map_err(|err| Error::Cast(format!("set volume: {err}")))?;
      tracing::debug!(?volume, "Cast volume set");
      Ok(())
    })
  }

  /// Pause using a known media session.
  pub fn pause(&mut self, transport_id: &str, media_session_id: i32) -> Result<()> {
    let session = MediaSessionRef::new(transport_id, media_session_id);
    let cmd = self.prepare_pause(session);
    let plan = MediaTransportPlan::from_command(&cmd);
    self.execute_media_transport_plan(&plan)
  }

  /// Resume using a known media session.
  pub fn play(&mut self, transport_id: &str, media_session_id: i32) -> Result<()> {
    let session = MediaSessionRef::new(transport_id, media_session_id);
    let cmd = self.prepare_play(session);
    let plan = MediaTransportPlan::from_command(&cmd);
    self.execute_media_transport_plan(&plan)
  }

  /// Stop using a known media session.
  pub fn stop(&mut self, transport_id: &str, media_session_id: i32) -> Result<()> {
    let session = MediaSessionRef::new(transport_id, media_session_id);
    let cmd = self.prepare_stop(session);
    let plan = MediaTransportPlan::from_command(&cmd);
    self.execute_media_transport_plan(&plan)
  }

  /// Execute a media transport plan: CONNECT destinations, then the media command.
  ///
  /// This is the single shipped path for pause/play/stop on a fresh TCP session.
  pub fn execute_media_transport_plan(&self, plan: &MediaTransportPlan) -> Result<()> {
    let _wire = plan.command.to_media_message_body(0);
    self.with_device(|device| {
      // CONNECT order is defined by the plan (receiver-0 then app transport).
      for dest in &plan.connect_destinations {
        device
          .connection
          .connect(dest.as_str())
          .map_err(|err| Error::Cast(format!("connection channel to {dest}: {err}")))?;
      }
      device
        .heartbeat
        .ping()
        .map_err(|err| Error::Cast(format!("heartbeat after media transport connect: {err}")))?;

      let transport_id = plan.command.transport_id();
      let media_session_id = plan.media_session_id();
      match plan.command {
        TransportCommand::Pause(_) => {
          drop(
            device
              .media
              .pause(transport_id, media_session_id)
              .map_err(|err| Error::Cast(format!("pause: {err}")))?,
          );
        },
        TransportCommand::Play(_) => {
          drop(
            device
              .media
              .play(transport_id, media_session_id)
              .map_err(|err| Error::Cast(format!("play: {err}")))?,
          );
        },
        TransportCommand::Stop(_) => {
          drop(
            device
              .media
              .stop(transport_id, media_session_id)
              .map_err(|err| Error::Cast(format!("stop: {err}")))?,
          );
        },
      }
      Ok(())
    })
  }

  /// Stop the active session (if any) and clear it.
  pub fn stop_active(&mut self) -> Result<()> {
    self.stop_heartbeat();
    let Some(session) = self.active.take() else {
      return Ok(());
    };
    self.stop(&session.transport_id, session.media_session_id)
  }

  /// Stop background heartbeats (if any).
  pub fn stop_heartbeat(&mut self) {
    if let Some(flag) = self.heartbeat_stop.take() {
      flag.store(true, std::sync::atomic::Ordering::Relaxed);
    }
  }

  /// Spawn a best-effort heartbeat loop so some receivers do not idle-disconnect.
  ///
  /// Uses a separate TCP session (`receiver-0` PING). Safe to call after LOAD.
  pub fn spawn_heartbeat_keep_alive(&mut self, interval: std::time::Duration) {
    self.stop_heartbeat();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    self.heartbeat_stop = Some(std::sync::Arc::clone(&stop));
    let host = self.host.clone();
    let port = self.port;
    drop(std::thread::spawn(move || {
      while !stop.load(std::sync::atomic::Ordering::Relaxed) {
        let ctl = Self::new(host.clone(), port);
        if let Err(err) = ctl.with_device(|device| {
          device
            .connection
            .connect("receiver-0")
            .map_err(|e| Error::Cast(format!("heartbeat connect: {e}")))?;
          device
            .heartbeat
            .ping()
            .map_err(|e| Error::Cast(format!("heartbeat ping: {e}")))?;
          Ok(())
        }) {
          tracing::debug!(error = %err, "Cast heartbeat ping failed");
        }
        // Sleep in 200 ms slices so stop is responsive.
        let steps = (interval.as_millis() / 200).max(1);
        for _ in 0..steps {
          if stop.load(std::sync::atomic::Ordering::Relaxed) {
            break;
          }
          std::thread::sleep(std::time::Duration::from_millis(200));
        }
      }
    }));
  }

  /// Best-effort stop of the active session with a wall-clock timeout.
  ///
  /// Clears `active` immediately so callers never re-use a stale session. Network
  /// STOP runs on a worker thread so a blocked `rust_cast` receive cannot hang
  /// the bridge (media HTTP is shut down independently by the bridge).
  pub fn stop_active_best_effort(&mut self, timeout: std::time::Duration) {
    self.stop_heartbeat();
    let Some(session) = self.active.take() else {
      return;
    };
    let host = self.host.clone();
    let port = self.port;
    let transport_id = session.transport_id.clone();
    let media_session_id = session.media_session_id;

    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
      let mut ctl = Self::new(host, port);
      let result = ctl.stop(&transport_id, media_session_id);
      drop(tx.send(result));
    });

    match rx.recv_timeout(timeout) {
      Ok(Ok(())) => {
        tracing::debug!(%session.transport_id, media_session_id, "Cast STOP ok");
      },
      Ok(Err(err)) => {
        tracing::warn!(error = %err, "Cast STOP failed");
      },
      Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
        tracing::warn!(
          %session.transport_id,
          media_session_id,
          timeout_ms = timeout.as_millis(),
          "Cast STOP timed out; abandoning worker"
        );
        // Detach worker; do not join (it may still be blocked in rust_cast).
        drop(worker);
      },
      Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
        tracing::warn!("Cast STOP worker disconnected before reply");
      },
    }
  }

  /// Pause the active session when present.
  pub fn pause_active(&mut self) -> Result<()> {
    let Some(session) = self.active.clone() else {
      return Err(Error::Cast("no active Cast media session".to_owned()));
    };
    self.pause(&session.transport_id, session.media_session_id)
  }

  /// Play the active session when present.
  pub fn play_active(&mut self) -> Result<()> {
    let Some(session) = self.active.clone() else {
      return Err(Error::Cast("no active Cast media session".to_owned()));
    };
    self.play(&session.transport_id, session.media_session_id)
  }

  fn with_device<F, T>(&self, f: F) -> Result<T>
  where
    F: FnOnce(&rust_cast::CastDevice<'_>) -> Result<T>,
  {
    // `rust_cast` dials with an unbound `TcpStream::connect`, which on
    // multi-homed Macs can pick the wrong NIC while AirPlay is active
    // (`No route to host`). Pre-connect with source bind + localhost relay so
    // rust_cast only talks to 127.0.0.1; TLS still terminates on the Nest.
    let (relay_host, relay_port) = crate::net::spawn_cast_connect_relay(self.host.as_str(), self.port)
      .map_err(|err| Error::Cast(format!("connect {}:{}: {err}", self.host, self.port)))?;
    let device = rust_cast::CastDevice::connect_without_host_verification(relay_host.as_str(), relay_port)
      .map_err(|err| {
        Error::Cast(format!(
          "connect {}:{} (via local relay {relay_host}:{relay_port}): {err}",
          self.host, self.port
        ))
      })?;
    f(&device)
  }

  /// Like [`Self::with_device`], but refreshes DNS and retries transient link errors.
  fn with_device_retry<F, T>(&mut self, mut f: F) -> Result<T>
  where
    F: FnMut(&rust_cast::CastDevice<'_>) -> Result<T>,
  {
    const ATTEMPTS: u32 = 8;
    let mut last_err: Option<Error> = None;
    let candidates = crate::net::cast_connect_hosts(self.host.as_str(), self.hostname.as_deref());
    // Wake ARP on every candidate once up front.
    for c in &candidates {
      crate::net::wake_cast_host(c);
    }

    for attempt in 1..=ATTEMPTS {
      self.refresh_host();
      let mut hosts = crate::net::cast_connect_hosts(self.host.as_str(), self.hostname.as_deref());
      if hosts.is_empty() {
        hosts.push(self.host.clone());
      }
      for host in hosts {
        self.host = host;
        crate::net::wake_cast_host(&self.host);
        match self.with_device(&mut f) {
          Ok(v) => return Ok(v),
          Err(err) => {
            let retriable = is_retriable_cast_error(&err);
            tracing::warn!(
              attempt,
              max = ATTEMPTS,
              host = %self.host,
              retriable,
              error = %err,
              "Cast connect attempt failed"
            );
            // Per-interface probe so the log shows which source IP still works
            // while AirPlay is active (unbound default route may not).
            crate::net::probe_cast_reachability(self.host.as_str(), self.port);
            last_err = Some(err);
            if !retriable {
              // Hard protocol error: stop trying other hosts this attempt.
              break;
            }
          },
        }
      }
      if attempt < ATTEMPTS {
        std::thread::sleep(std::time::Duration::from_millis(400 * u64::from(attempt)));
      }
    }
    Err(last_err.unwrap_or_else(|| Error::Cast("Cast connect failed with no error detail".to_owned())))
  }
}

/// True when a Cast error is worth retrying (Wi‑Fi / Nest sleep / transient route).
fn is_retriable_cast_error(err: &Error) -> bool {
  let msg = err.to_string().to_ascii_lowercase();
  msg.contains("no route to host")
    || msg.contains("host is down")
    || msg.contains("network is unreachable")
    || msg.contains("timed out")
    || msg.contains("timeout")
    || msg.contains("connection refused")
    || msg.contains("connection reset")
    || msg.contains("broken pipe")
    || msg.contains("temporarily unavailable")
}

/// Destinations that must be CONNECT'd before media play/pause/stop on a fresh TCP session.
///
/// Order matches `connect_and_load`: platform receiver first, then the app transport.
pub const fn media_command_connect_destinations(transport_id: &str) -> [&str; 2] {
  ["receiver-0", transport_id]
}

/// Ordered wire plan for pause/play/stop on a **fresh** TCP Cast session.
///
/// Production `pause`/`play`/`stop` always build this plan via
/// [`MediaTransportPlan::from_command`] and execute it with
/// [`CastController::execute_media_transport_plan`]. CONNECT destinations come
/// first so `rust_cast` media commands receive STATUS replies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaTransportPlan {
  /// CONNECT destinations in order (must include `receiver-0` then app transport).
  pub connect_destinations: Vec<String>,
  /// Media command after CONNECT.
  pub command: TransportCommand,
}

impl MediaTransportPlan {
  /// Build the shipped plan for a transport command (CONNECT ×2 then media op).
  pub fn from_command(command: &TransportCommand) -> Self {
    let transport_id = command.transport_id();
    let connect_destinations = media_command_connect_destinations(transport_id)
      .into_iter()
      .map(str::to_owned)
      .collect();
    Self {
      connect_destinations,
      command: command.clone(),
    }
  }

  /// Media session id targeted by this plan.
  pub const fn media_session_id(&self) -> i32 {
    match &self.command {
      TransportCommand::Pause(s) | TransportCommand::Play(s) | TransportCommand::Stop(s) => s.media_session_id,
    }
  }

  /// True when the plan CONNECTs `receiver-0` before the app transport id.
  pub fn connects_receiver_then_transport(&self) -> bool {
    let Some(first) = self.connect_destinations.first() else {
      return false;
    };
    let Some(second) = self.connect_destinations.get(1) else {
      return false;
    };
    first == "receiver-0" && second == self.command.transport_id() && self.connect_destinations.len() == 2
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn builds_flac_live_payload() {
    let req = MediaLoadRequest::flac("http://10.0.0.2:9/stream", CastStreamKind::Live).with_title("PlayOnAir");
    let media = req.to_media();
    assert_eq!(media.content_id, "http://10.0.0.2:9/stream");
    assert_eq!(media.content_type, CONTENT_TYPE_FLAC);
    assert_eq!(media.stream_type, StreamType::Live);
    assert!(media.metadata.is_some());
  }

  #[test]
  fn builds_wav_live_payload() {
    let req = MediaLoadRequest::wav("http://192.168.1.5:4000/stream", CastStreamKind::Live).with_title("Kitchen");
    let media = req.to_media();
    assert_eq!(media.content_type, CONTENT_TYPE_WAV);
    assert_eq!(media.stream_type, StreamType::Live);
    assert_eq!(media.content_id, "http://192.168.1.5:4000/stream");
  }

  #[test]
  fn load_message_body_uses_live_stream_type() {
    let req = MediaLoadRequest::wav("http://10.0.0.1/stream", CastStreamKind::Live);
    let body = req.to_load_message_body("app-session", 7);
    assert_eq!(body["type"], "LOAD");
    assert_eq!(body["requestId"], 7);
    assert_eq!(body["sessionId"], "app-session");
    assert_eq!(body["media"]["streamType"], "LIVE");
    assert_eq!(body["media"]["contentType"], CONTENT_TYPE_WAV);
    assert_eq!(body["media"]["contentId"], "http://10.0.0.1/stream");
  }

  #[test]
  fn prepare_load_stores_last() {
    let mut ctl = CastController::new("127.0.0.1", 8009);
    let req = MediaLoadRequest::wav("http://127.0.0.1/x.wav", CastStreamKind::Buffered);
    let media = ctl.prepare_load(req.clone());
    assert_eq!(media.content_type, CONTENT_TYPE_WAV);
    assert_eq!(ctl.last_load(), Some(&req));
  }

  #[test]
  fn volume_level_clamped_bounds() {
    assert!((volume_level_clamped(0.0) - 0.0).abs() < f32::EPSILON);
    assert!((volume_level_clamped(1.0) - 1.0).abs() < f32::EPSILON);
    assert!((volume_level_clamped(-0.5) - 0.0).abs() < f32::EPSILON);
    assert!((volume_level_clamped(1.5) - 1.0).abs() < f32::EPSILON);
    assert!((volume_level_clamped(0.42) - 0.42).abs() < f32::EPSILON);
  }

  #[test]
  fn set_volume_message_body_matches_cast_wire() {
    let body = set_volume_message_body(1.5, 3);
    assert_eq!(body["type"], "SET_VOLUME");
    assert_eq!(body["requestId"], 3);
    assert_eq!(body["volume"]["level"], 1.0);
  }

  #[test]
  fn prepare_volume_records_clamped_and_builds_wire() {
    let mut ctl = CastController::new("127.0.0.1", 8009);
    let level = ctl.prepare_volume(2.0);
    assert!((level - 1.0).abs() < f32::EPSILON);
    assert_eq!(ctl.last_volume(), Some(1.0));
    let wire = set_volume_message_body(level, 1);
    assert_eq!(wire["type"], "SET_VOLUME");
    assert_eq!(wire["volume"]["level"], 1.0);
  }

  #[test]
  fn transport_commands_build_rust_cast_media_message_types() {
    let mut ctl = CastController::new("192.168.1.20", 8009);
    let session = MediaSessionRef::new("transport-1", 42);

    let pause = ctl.prepare_pause(session.clone());
    let pause_body = pause.to_media_message_body(11);
    assert_eq!(pause_body["type"], "PAUSE");
    assert_eq!(pause_body["mediaSessionId"], 42);
    assert_eq!(pause_body["requestId"], 11);
    assert_eq!(pause.transport_id(), "transport-1");

    let play = ctl.prepare_play(session.clone());
    let play_body = play.to_media_message_body(12);
    assert_eq!(play_body["type"], "PLAY");
    assert_eq!(play_body["mediaSessionId"], 42);

    let stop = ctl.prepare_stop(session);
    let stop_body = stop.to_media_message_body(13);
    assert_eq!(stop_body["type"], "STOP");
    assert_eq!(stop_body["mediaSessionId"], 42);
  }

  #[test]
  fn media_session_id_from_status_reads_first_entry() {
    let status = Status {
      request_id: 1,
      entries: vec![rust_cast::channels::media::StatusEntry {
        media_session_id: 99,
        media: None,
        playback_rate: 1.0,
        player_state: rust_cast::channels::media::PlayerState::Playing,
        current_item_id: None,
        loading_item_id: None,
        preloaded_item_id: None,
        idle_reason: None,
        extended_status: None,
        current_time: None,
        supported_media_commands: 0,
      }],
    };
    assert_eq!(media_session_id_from_status(&status).expect("id"), 99);
  }

  #[test]
  fn media_session_id_from_empty_status_errors() {
    let status = Status { request_id: 1, entries: vec![] };
    let err = media_session_id_from_status(&status).expect_err("empty status");
    assert!(matches!(err, Error::Cast(_)));
  }

  #[test]
  fn stop_active_without_session_is_ok() {
    let mut ctl = CastController::new("127.0.0.1", 8009);
    ctl.stop_active().expect("noop");
  }

  #[test]
  fn media_command_connect_destinations_order() {
    let dests = media_command_connect_destinations("web-1");
    assert_eq!(dests[0], "receiver-0");
    assert_eq!(dests[1], "web-1");
  }

  #[test]
  fn pause_play_stop_plans_connect_receiver_then_transport_before_media() {
    let session = MediaSessionRef::new("web-42", 99);
    for cmd in [
      TransportCommand::Pause(session.clone()),
      TransportCommand::Play(session.clone()),
      TransportCommand::Stop(session),
    ] {
      let plan = MediaTransportPlan::from_command(&cmd);
      // Shipped path: CONNECT receiver-0, CONNECT transport, then media op.
      assert!(
        plan.connects_receiver_then_transport(),
        "plan must CONNECT receiver-0 then transport before media: {plan:?}"
      );
      assert_eq!(plan.connect_destinations.len(), 2);
      assert_eq!(plan.connect_destinations.first().map(String::as_str), Some("receiver-0"));
      assert_eq!(plan.connect_destinations.get(1).map(String::as_str), Some("web-42"));
      assert_eq!(plan.media_session_id(), 99);
      // Wire body type must match command (media channel after CONNECT).
      let body = plan.command.to_media_message_body(1);
      let expected_type = match plan.command {
        TransportCommand::Pause(_) => "PAUSE",
        TransportCommand::Play(_) => "PLAY",
        TransportCommand::Stop(_) => "STOP",
      };
      assert_eq!(body["type"], expected_type);
      assert_eq!(body["mediaSessionId"], 99);
    }
  }

  #[test]
  fn pause_play_stop_methods_use_same_transport_plan_as_from_command() {
    // prepare_* + from_command is exactly what pause/play/stop build before execute.
    let mut ctl = CastController::new("127.0.0.1", 8009);
    let session = MediaSessionRef::new("transport-1", 5);

    let pause_cmd = ctl.prepare_pause(session.clone());
    let pause_plan = MediaTransportPlan::from_command(&pause_cmd);
    assert!(pause_plan.connects_receiver_then_transport());
    assert_eq!(ctl.last_transport(), Some(&TransportCommand::Pause(session.clone())));

    let play_cmd = ctl.prepare_play(session.clone());
    let play_plan = MediaTransportPlan::from_command(&play_cmd);
    assert!(play_plan.connects_receiver_then_transport());

    let stop_cmd = ctl.prepare_stop(session);
    let stop_plan = MediaTransportPlan::from_command(&stop_cmd);
    assert!(stop_plan.connects_receiver_then_transport());
  }

  #[test]
  fn execute_media_transport_plan_fails_before_media_when_host_unreachable() {
    // Unreachable host: connect fails during CONNECT phase (no hang on media STATUS).
    let ctl = CastController::new("127.0.0.1", 9);
    let plan = MediaTransportPlan::from_command(&TransportCommand::Pause(MediaSessionRef::new("web-1", 1)));
    assert!(plan.connects_receiver_then_transport());
    let start = std::time::Instant::now();
    let err = ctl.execute_media_transport_plan(&plan).expect_err("must fail");
    assert!(matches!(err, Error::Cast(_)));
    assert!(
      start.elapsed() < std::time::Duration::from_secs(3),
      "CONNECT failure must not hang waiting for media STATUS"
    );
  }

  #[test]
  fn stop_active_best_effort_clears_session_without_device() {
    let mut ctl = CastController::new("127.0.0.1", 9);
    // Inject a fake active session; stop will fail to connect (port 9 closed) but must clear.
    ctl.set_active_for_test(MediaSessionRef::new("transport-x", 7));
    ctl.stop_active_best_effort(std::time::Duration::from_millis(500));
    assert!(ctl.active_session().is_none());
  }

  #[test]
  fn retriable_cast_error_detects_no_route() {
    let err = Error::Cast("connect 192.168.1.171:8009: No route to host (os error 65)".to_owned());
    assert!(is_retriable_cast_error(&err));
    let hard = Error::Cast("media load: Invalid request".to_owned());
    assert!(!is_retriable_cast_error(&hard));
  }
}
