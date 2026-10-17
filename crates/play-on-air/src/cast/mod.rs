//! Google Cast control plane (media load + transport).
//!
//! Payload construction is unit-tested without a device. Live connect/load uses
//! `rust_cast` channels via [`CastPool`] (warm per-device worker threads) over a TLS
//! client that accepts self-signed and X.509 v1 device certificates (`tls`).

mod pool;
mod tls;

pub use pool::{CastPool, CastRemoteEvent, CastRemoteKind};

use rust_cast::channels::media::{Image, Media, Metadata, MusicTrackMediaMetadata, Status, StreamType};

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

  /// Cast wire name (`BUFFERED` / `LIVE`).
  const fn wire_name(self) -> &'static str {
    match self {
      Self::Buffered => "BUFFERED",
      Self::Live => "LIVE",
    }
  }
}

/// Now-playing track shown on the Cast device (`MusicTrackMediaMetadata`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CastTrack {
  /// Track title.
  pub title: Option<String>,
  /// Track artist.
  pub artist: Option<String>,
  /// Album name.
  pub album: Option<String>,
  /// Artwork URL on our media server (`/artwork/<id>`).
  pub image_url: Option<String>,
}

impl CastTrack {
  const fn is_empty(&self) -> bool {
    self.title.is_none() && self.artist.is_none() && self.album.is_none() && self.image_url.is_none()
  }

  fn to_metadata(&self) -> Option<Metadata> {
    if self.is_empty() {
      return None;
    }
    Some(Metadata::MusicTrack(MusicTrackMediaMetadata {
      title: self.title.clone(),
      artist: self.artist.clone(),
      album_name: self.album.clone(),
      album_artist: None,
      composer: None,
      track_number: None,
      disc_number: None,
      images: self.image_url.iter().map(|url| Image::new(url.clone())).collect(),
      release_date: None,
    }))
  }

  /// Wire `MusicTrackMediaMetadata` (`metadataType` 3) as JSON.
  fn to_json(&self) -> serde_json::Value {
    let mut metadata = serde_json::Map::new();
    drop(metadata.insert("metadataType".to_owned(), serde_json::Value::from(3)));
    for (key, field) in [("title", &self.title), ("artist", &self.artist), ("albumName", &self.album)] {
      if let Some(value) = field {
        drop(metadata.insert(key.to_owned(), serde_json::Value::from(value.as_str())));
      }
    }
    if let Some(url) = &self.image_url {
      drop(metadata.insert("images".to_owned(), serde_json::json!([{ "url": url }])));
    }
    serde_json::Value::Object(metadata)
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
  /// Now-playing metadata shown on the sink UI.
  pub track: CastTrack,
}

impl MediaLoadRequest {
  /// Build a FLAC load request.
  pub fn flac(url: impl Into<String>, kind: CastStreamKind) -> Self {
    Self {
      content_url: url.into(),
      content_type: CONTENT_TYPE_FLAC.to_owned(),
      stream_kind: kind,
      track: CastTrack::default(),
    }
  }

  /// Build a WAV load request.
  pub fn wav(url: impl Into<String>, kind: CastStreamKind) -> Self {
    Self {
      content_url: url.into(),
      content_type: CONTENT_TYPE_WAV.to_owned(),
      stream_kind: kind,
      track: CastTrack::default(),
    }
  }

  /// Attach now-playing metadata.
  #[must_use]
  pub fn with_track(mut self, track: CastTrack) -> Self {
    self.track = track;
    self
  }

  /// Convert to `rust_cast` [`Media`] for wire send.
  pub fn to_media(&self) -> Media {
    Media {
      content_id: self.content_url.clone(),
      stream_type: self.stream_kind.to_stream_type(),
      content_type: self.content_type.clone(),
      metadata: self.track.to_metadata(),
      duration: None,
    }
  }

  /// `QUEUE_UPDATE` that replaces the metadata of the playing queue item in place.
  ///
  /// The Default Media Receiver has no other sender message that changes metadata of
  /// loaded media; updating the current item keeps the same `contentId`, so the live
  /// stream is not re-fetched (a re-LOAD would drop Cast's buffer and gap the audio).
  pub fn queue_update_payload(&self, request_id: u32, media_session_id: i32, item_id: i32) -> serde_json::Value {
    serde_json::json!({
      "type": "QUEUE_UPDATE",
      "requestId": request_id,
      "mediaSessionId": media_session_id,
      "items": [{
        "itemId": item_id,
        "media": {
          "contentId": self.content_url,
          "streamType": self.stream_kind.wire_name(),
          "contentType": self.content_type,
          "metadata": self.track.to_json(),
        },
      }],
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
  /// Queue item id of the loaded media (target of metadata `QUEUE_UPDATE`), if reported.
  pub queue_item_id: Option<i32>,
}

impl MediaSessionRef {
  /// Construct a session reference used by pure transport helpers.
  pub fn new(transport_id: impl Into<String>, media_session_id: i32) -> Self {
    Self {
      transport_id: transport_id.into(),
      media_session_id,
      queue_item_id: None,
    }
  }
}

/// Active Cast media session after a successful LOAD.
pub type ActiveCastSession = MediaSessionRef;

/// Extract the media session (and its current queue item) from a LOAD/STATUS response.
pub fn media_session_from_status(transport_id: impl Into<String>, status: &Status) -> Result<MediaSessionRef> {
  if let Some(entry) = status.entries.first() {
    return Ok(MediaSessionRef {
      transport_id: transport_id.into(),
      media_session_id: entry.media_session_id,
      queue_item_id: entry.current_item_id.map(i32::from),
    });
  }
  Err(Error::Cast("LOAD status had no media session entries".to_owned()))
}

#[cfg(test)]
mod tests {
  use super::*;
  use rust_cast::channels::media::{Media, StreamType};

  fn track() -> CastTrack {
    CastTrack {
      title: Some("Song".to_owned()),
      artist: Some("Band".to_owned()),
      album: Some("Record".to_owned()),
      image_url: Some("http://192.168.1.2:8090/artwork/7".to_owned()),
    }
  }

  #[test]
  fn builds_flac_live_payload_with_music_metadata() {
    let req = MediaLoadRequest::flac("http://192.168.1.2:8090/x.flac", CastStreamKind::Live).with_track(track());
    let media: Media = req.to_media();
    assert_eq!(media.content_id, "http://192.168.1.2:8090/x.flac");
    assert_eq!(media.content_type, CONTENT_TYPE_FLAC);
    assert!(matches!(media.stream_type, StreamType::Live));
    let Some(Metadata::MusicTrack(meta)) = media.metadata else {
      panic!("expected MusicTrack metadata");
    };
    assert_eq!(meta.title.as_deref(), Some("Song"));
    assert_eq!(meta.artist.as_deref(), Some("Band"));
    assert_eq!(meta.album_name.as_deref(), Some("Record"));
    assert_eq!(meta.images.len(), 1);
    assert_eq!(meta.images[0].url, "http://192.168.1.2:8090/artwork/7");
  }

  #[test]
  fn empty_track_sends_no_metadata() {
    let media = MediaLoadRequest::flac("http://h/x", CastStreamKind::Live).to_media();
    assert!(media.metadata.is_none());
  }

  #[test]
  fn queue_update_replaces_current_item_metadata() {
    let req = MediaLoadRequest::flac("http://192.168.1.2:8090/stream", CastStreamKind::Live).with_track(track());
    let payload = req.queue_update_payload(9, 4, 1);
    assert_eq!(
      payload,
      serde_json::json!({
        "type": "QUEUE_UPDATE",
        "requestId": 9,
        "mediaSessionId": 4,
        "items": [{
          "itemId": 1,
          "media": {
            "contentId": "http://192.168.1.2:8090/stream",
            "streamType": "LIVE",
            "contentType": "audio/flac",
            "metadata": {
              "metadataType": 3,
              "title": "Song",
              "artist": "Band",
              "albumName": "Record",
              "images": [{ "url": "http://192.168.1.2:8090/artwork/7" }],
            },
          },
        }],
      })
    );
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
  fn media_session_from_status_reads_first_entry() {
    use rust_cast::channels::media::{PlayerState, StatusEntry};
    let status = Status {
      request_id: 1,
      entries: vec![StatusEntry {
        media_session_id: 42,
        playback_rate: 1.0,
        player_state: PlayerState::Playing,
        current_item_id: Some(3),
        loading_item_id: None,
        preloaded_item_id: None,
        idle_reason: None,
        extended_status: None,
        media: None,
        current_time: None,
        supported_media_commands: 0,
      }],
    };
    let session = media_session_from_status("t-1", &status).unwrap();
    assert_eq!(session.transport_id, "t-1");
    assert_eq!(session.media_session_id, 42);
    assert_eq!(session.queue_item_id, Some(3));
  }

  #[test]
  fn media_session_from_empty_status_errors() {
    let status = Status { request_id: 1, entries: vec![] };
    drop(media_session_from_status("t", &status).unwrap_err());
  }
}
