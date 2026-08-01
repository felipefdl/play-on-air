//! Typed errors for PlayOnAir.

use std::path::PathBuf;

/// Library and binary result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Top-level error type for PlayOnAir operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
  /// Failed to read or parse optional TOML configuration.
  #[error("config error at {path}: {source}")]
  Config {
    /// Path that failed to load.
    path: PathBuf,
    /// Underlying I/O or parse error.
    source: ConfigError,
  },

  /// Chromecast mDNS discovery failure (non-fatal at process level).
  #[error("discovery error: {0}")]
  Discovery(String),

  /// Local media HTTP server failure.
  #[error("media server error: {0}")]
  Media(String),

  /// Cast control-plane failure.
  #[error("cast error: {0}")]
  Cast(String),

  /// AirPlay receiver failure.
  #[error("airplay error: {0}")]
  AirPlay(String),

  /// Audio encode or buffer failure.
  #[error("audio error: {0}")]
  Audio(String),

  /// Bridge session orchestration failure.
  #[error("bridge error: {0}")]
  Bridge(String),

  /// I/O error with context.
  #[error("I/O error: {0}")]
  Io(#[from] std::io::Error),
}

/// Configuration-specific errors.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
  /// Filesystem I/O while reading the config file.
  #[error(transparent)]
  Io(#[from] std::io::Error),

  /// TOML parse failure.
  #[error("TOML parse error: {0}")]
  Parse(#[from] toml::de::Error),
}
