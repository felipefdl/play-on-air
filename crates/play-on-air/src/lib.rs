//! PlayOnAir library: Chromecast devices as AirPlay 2 speakers on the LAN.
//!
//! See the repository `VISION.md` for product intent. This crate exposes the
//! binary's modules for unit tests and future embedding.

#![doc(html_no_source)]

pub mod airplay;
pub mod app;
pub mod audio;
pub mod bridge;
pub mod cast;
pub mod config;
pub mod discover;
pub mod error;
pub mod media;
pub mod names;
pub mod net;
pub mod registry;

pub use app::App;
pub use config::Config;
pub use error::{Error, Result};
pub use net::{advertise_host_for_peer, advertise_host_ip};
