//! RAOP/AirPlay 2 server core — connection handling, audio pipeline, and public API.

pub use crate::proto::dmap::TrackMetadata;

pub mod audio_pipeline;
#[cfg(feature = "video")]
pub mod buffer;
pub(crate) mod buffered_audio;
pub(crate) mod event_channel;
pub(crate) mod handlers;
pub(crate) mod handlers_ap2;
#[cfg(feature = "hls")]
pub(crate) mod handlers_hls;
#[cfg(feature = "hls")]
pub mod hls;
#[cfg(feature = "video")]
pub(crate) mod ntp;
pub(crate) mod realtime_audio;
pub(crate) mod remote;
#[cfg(feature = "video")]
pub(crate) mod rtp;
mod rtsp;
#[cfg(feature = "video")]
pub mod video;
#[cfg(feature = "video")]
pub(crate) mod video_stream;
pub(crate) mod volume;

pub(crate) mod config;

/// Maximum hardware address length in bytes.
pub(crate) const MAX_HWADDR_LEN: usize = 6;
/// Maximum password length in bytes.
pub(crate) const MAX_PASSWORD_LEN: usize = 64;
/// Maximum HTTP Digest nonce length in bytes.
pub(crate) const MAX_NONCE_LEN: usize = 32;

mod types;
pub use remote::MediaRemoteCommand;
pub use types::*;

mod connection;
mod server;
pub use server::{RaopServer, RaopServerBuilder};
