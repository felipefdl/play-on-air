//! Google Cast control plane (media load + transport).
//!
//! Pure payload construction is unit-testable without a device on the wire.
//! Live connect/load uses `rust_cast` and may fail on hosts without a Cast device.

use rust_cast::channels::media::{Media, Metadata, MusicTrackMediaMetadata, StreamType};

use crate::error::{Error, Result};

/// Content type for lossless FLAC Cast media.
pub const CONTENT_TYPE_FLAC: &str = "audio/flac";

/// Content type for WAV / LPCM Cast media.
pub const CONTENT_TYPE_WAV: &str = "audio/wav";

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

/// Thin wrapper around Cast session operations.
///
/// Connect is optional/lazy: scaffolding keeps a host/port and pure helpers so
/// unit tests do not require a physical device.
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
}

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

impl CastController {
  /// Create a controller targeting `host:port`.
  pub fn new(host: impl Into<String>, port: u16) -> Self {
    Self {
      host: host.into(),
      port,
      last_load: None,
      last_volume: None,
      last_transport: None,
    }
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

  /// Connect to the device, launch default media receiver, and load media.
  ///
  /// Network-facing; returns a typed error when the device is unreachable.
  pub fn connect_and_load(&mut self, request: MediaLoadRequest) -> Result<()> {
    let media = self.prepare_load(request);
    self.with_device(|device| {
      // Connect transport channel.
      device
        .connection
        .connect("receiver-0")
        .map_err(|err| Error::Cast(format!("connection channel: {err}")))?;

      // Heartbeat keep-alive.
      device
        .heartbeat
        .ping()
        .map_err(|err| Error::Cast(format!("heartbeat: {err}")))?;

      // Launch Default Media Receiver.
      let app = device
        .receiver
        .launch_app(&rust_cast::channels::receiver::CastDeviceApp::DefaultMediaReceiver)
        .map_err(|err| Error::Cast(format!("launch app: {err}")))?;

      device
        .connection
        .connect(app.transport_id.as_str())
        .map_err(|err| Error::Cast(format!("app connection: {err}")))?;

      drop(
        device
          .media
          .load(app.transport_id.as_str(), app.session_id.as_str(), &media)
          .map_err(|err| Error::Cast(format!("media load: {err}")))?,
      );

      tracing::info!(host = %self.host, port = self.port, url = %media.content_id, "Cast media loaded");
      Ok(())
    })
  }

  /// Set receiver volume level in `0.0..=1.0`.
  pub fn set_volume(&mut self, level: f32) -> Result<()> {
    let clamped = self.prepare_volume(level);
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

  /// Pause the active media session (requires known media session id).
  pub fn pause(&mut self, transport_id: &str, media_session_id: i32) -> Result<()> {
    let session = MediaSessionRef::new(transport_id, media_session_id);
    drop(self.prepare_pause(session));
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

  /// Resume playback.
  pub fn play(&mut self, transport_id: &str, media_session_id: i32) -> Result<()> {
    let session = MediaSessionRef::new(transport_id, media_session_id);
    drop(self.prepare_play(session));
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

  /// Stop playback.
  pub fn stop(&mut self, transport_id: &str, media_session_id: i32) -> Result<()> {
    let session = MediaSessionRef::new(transport_id, media_session_id);
    drop(self.prepare_stop(session));
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
  fn prepare_volume_records_clamped() {
    let mut ctl = CastController::new("127.0.0.1", 8009);
    let level = ctl.prepare_volume(2.0);
    assert!((level - 1.0).abs() < f32::EPSILON);
    assert_eq!(ctl.last_volume(), Some(1.0));
  }

  #[test]
  fn prepare_transport_commands_without_device() {
    let mut ctl = CastController::new("192.168.1.20", 8009);
    let session = MediaSessionRef::new("transport-1", 42);

    let pause = ctl.prepare_pause(session.clone());
    assert_eq!(pause, TransportCommand::Pause(session.clone()));
    assert_eq!(ctl.last_transport(), Some(&TransportCommand::Pause(session.clone())));

    let play = ctl.prepare_play(session.clone());
    assert_eq!(play, TransportCommand::Play(session.clone()));

    let stop = ctl.prepare_stop(session.clone());
    assert_eq!(stop, TransportCommand::Stop(session));
  }

  #[test]
  fn media_load_request_equality_for_live_wav() {
    let a = MediaLoadRequest::wav("http://10.0.0.1/stream", CastStreamKind::Live).with_title("A");
    let b = MediaLoadRequest::wav("http://10.0.0.1/stream", CastStreamKind::Live).with_title("A");
    assert_eq!(a, b);
    let media = a.to_media();
    assert_eq!(media.stream_type, StreamType::Live);
  }
}
