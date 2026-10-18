//! Local HTTP media server for Cast pull of FLAC/WAV bodies and now-playing artwork.

pub mod http;

pub use http::{
  Artwork, CAST_READ_AHEAD, MAX_ARTWORK_BYTES, MediaContent, MediaServer, MediaServerHandle, RolloverSignal,
};
