//! Local HTTP media server for Cast pull of FLAC/WAV bodies.

pub mod http;

pub use http::{MediaContent, MediaServer, MediaServerHandle};
