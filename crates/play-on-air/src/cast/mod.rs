//! Google Cast control plane (media load + transport).
//!
//! Payload construction is unit-tested without a device. Live connect/load uses
//! `rust_cast` and stores the media session so play/pause/stop can target it.

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
}

impl CastController {
  /// Create a controller targeting `host:port`.
  pub fn new(host: impl Into<String>, port: u16) -> Self {
    Self {
      host: host.into(),
      port,
      last_load: None,
      last_volume: None,
      last_transport: None,
      active: None,
    }
  }

  /// Active Cast media session, if LOAD succeeded.
  pub const fn active_session(&self) -> Option<&ActiveCastSession> {
    self.active.as_ref()
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
  pub fn connect_and_load(&mut self, request: MediaLoadRequest) -> Result<ActiveCastSession> {
    let media = self.prepare_load(request);
    let host = self.host.clone();
    let port = self.port;
    let session = self.with_device(|device| {
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
      host = %host,
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
    let _wire = cmd.to_media_message_body(0);
    self.with_device(|device| {
      drop(
        device
          .media
          .pause(transport_id, media_session_id)
          .map_err(|err| Error::Cast(format!("pause: {err}")))?,
      );
      Ok(())
    })
  }

  /// Resume using a known media session.
  pub fn play(&mut self, transport_id: &str, media_session_id: i32) -> Result<()> {
    let session = MediaSessionRef::new(transport_id, media_session_id);
    let cmd = self.prepare_play(session);
    let _wire = cmd.to_media_message_body(0);
    self.with_device(|device| {
      drop(
        device
          .media
          .play(transport_id, media_session_id)
          .map_err(|err| Error::Cast(format!("play: {err}")))?,
      );
      Ok(())
    })
  }

  /// Stop using a known media session.
  pub fn stop(&mut self, transport_id: &str, media_session_id: i32) -> Result<()> {
    let session = MediaSessionRef::new(transport_id, media_session_id);
    let cmd = self.prepare_stop(session);
    let _wire = cmd.to_media_message_body(0);
    self.with_device(|device| {
      drop(
        device
          .media
          .stop(transport_id, media_session_id)
          .map_err(|err| Error::Cast(format!("stop: {err}")))?,
      );
      Ok(())
    })
  }

  /// Stop the active session (if any) and clear it.
  pub fn stop_active(&mut self) -> Result<()> {
    let Some(session) = self.active.clone() else {
      return Ok(());
    };
    let result = self.stop(&session.transport_id, session.media_session_id);
    self.active = None;
    result
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
    // Chromecasts often present self-signed certs on the LAN.
    let device = rust_cast::CastDevice::connect_without_host_verification(self.host.as_str(), self.port)
      .map_err(|err| Error::Cast(format!("connect {}:{}: {err}", self.host, self.port)))?;
    f(&device)
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
}
