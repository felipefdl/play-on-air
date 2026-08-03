//! Google Cast control plane (media load + transport).
//!
//! Payload construction is unit-tested without a device. Live connect/load uses
//! `rust_cast` via [`CastPool`] (warm per-device worker threads).

mod pool;

pub use pool::CastPool;

use rust_cast::channels::media::{Media, Metadata, MusicTrackMediaMetadata, Status, StreamType};

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

/// Extract media session id from a LOAD/STATUS response.
pub fn media_session_id_from_status(status: &Status) -> Result<i32> {
  if let Some(entry) = status.entries.first() {
    return Ok(entry.media_session_id);
  }
  Err(Error::Cast("LOAD status had no media session entries".to_owned()))
}

#[cfg(test)]
mod tests {
  use super::*;
  use rust_cast::channels::media::{Media, StreamType};

  #[test]
  fn builds_flac_live_payload() {
    let req = MediaLoadRequest::flac("http://192.168.1.2:8090/x.flac", CastStreamKind::Live).with_title("Nest");
    let media: Media = req.to_media();
    assert_eq!(media.content_id, "http://192.168.1.2:8090/x.flac");
    assert_eq!(media.content_type, CONTENT_TYPE_FLAC);
    assert!(matches!(media.stream_type, StreamType::Live));
    assert!(media.metadata.is_some());
  }

  #[test]
  fn builds_wav_live_payload() {
    let req = MediaLoadRequest::wav("http://10.0.0.1/s.wav", CastStreamKind::Live);
    let media = req.to_media();
    assert_eq!(media.content_type, CONTENT_TYPE_WAV);
    assert!(matches!(media.stream_type, StreamType::Live));
  }

  #[test]
  fn volume_level_clamped_bounds() {
    assert!((volume_level_clamped(-0.5) - 0.0).abs() < f32::EPSILON);
    assert!((volume_level_clamped(0.3) - 0.3).abs() < f32::EPSILON);
    assert!((volume_level_clamped(1.5) - 1.0).abs() < f32::EPSILON);
  }

  #[test]
  fn media_session_id_from_status_reads_first_entry() {
    use rust_cast::channels::media::{PlayerState, StatusEntry};
    let status = Status {
      request_id: 1,
      entries: vec![StatusEntry {
        media_session_id: 42,
        playback_rate: 1.0,
        player_state: PlayerState::Playing,
        current_item_id: None,
        loading_item_id: None,
        preloaded_item_id: None,
        idle_reason: None,
        extended_status: None,
        media: None,
        current_time: None,
        supported_media_commands: 0,
      }],
    };
    assert_eq!(media_session_id_from_status(&status).unwrap(), 42);
  }

  #[test]
  fn media_session_id_from_empty_status_errors() {
    let status = Status { request_id: 1, entries: vec![] };
    drop(media_session_id_from_status(&status).unwrap_err());
  }
}
