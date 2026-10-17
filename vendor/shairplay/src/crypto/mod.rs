//! Cryptographic primitives for AirPlay 2 authentication and encryption.

pub mod chacha_transport;
pub mod fairplay;
#[cfg(feature = "video")]
mod fairplay_garble;
#[cfg(feature = "video")]
mod fairplay_tables;
pub mod pairing_homekit;
#[cfg(feature = "video")]
mod playfair;
pub mod tlv;
#[cfg(feature = "video")]
pub mod video_cipher;
#[cfg(feature = "video")]
pub mod video_key;
