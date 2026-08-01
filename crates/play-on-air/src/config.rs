//! Optional TOML configuration for device rename and hide cosmetics.
//!
//! Missing config is not an error: product defaults apply (identity map, no hides).

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{ConfigError, Error, Result};

/// Environment variable that overrides the default config path.
pub const CONFIG_ENV: &str = "PLAY_ON_AIR_CONFIG";

/// Default relative path when neither CLI nor env is set.
pub const DEFAULT_CONFIG_PATH: &str = "play-on-air.toml";

/// Runtime configuration (always present; may be empty defaults).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Config {
  /// Per-device cosmetic overrides.
  pub devices: Vec<DeviceConfig>,
}

/// One optional `[[device]]` table entry.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct DeviceConfig {
  /// Match Chromecast advertised name or UUID substring (case-insensitive).
  #[serde(default)]
  pub id: String,

  /// Optional AirPlay display rename. Empty means keep Cast name.
  #[serde(default)]
  pub name: Option<String>,

  /// When true, do not advertise this device as AirPlay.
  #[serde(default)]
  pub hide: bool,
}

/// Wire format of the optional TOML file.
#[derive(Debug, Clone, Default, Deserialize)]
struct ConfigFile {
  #[serde(default)]
  device: Vec<DeviceConfig>,
}

/// Resolved cosmetic override for a discovered Cast device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceOverride {
  /// Name shown on the AirPlay picker.
  pub display_name: String,
  /// When true, skip AirPlay advertisement.
  pub hidden: bool,
}

impl Config {
  /// Load optional config from an explicit path, env, or default path.
  ///
  /// Resolution order when `path` is `None`:
  /// 1. `$PLAY_ON_AIR_CONFIG` if set and non-empty
  /// 2. `./play-on-air.toml`
  ///
  /// A missing file yields [`Config::default`] (Ok), not an error.
  pub fn load_optional(path: Option<&Path>) -> Result<Self> {
    let resolved = resolve_config_path(path);
    Self::load_path(&resolved)
  }

  /// Load from a concrete path. Missing file → defaults.
  pub fn load_path(path: &Path) -> Result<Self> {
    match fs::read_to_string(path) {
      Ok(contents) => Self::parse_toml(&contents).map_err(|source| Error::Config { path: path.to_path_buf(), source }),
      Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
        tracing::debug!(path = %path.display(), "config file not found; using defaults");
        Ok(Self::default())
      },
      Err(err) => Err(Error::Config {
        path: path.to_path_buf(),
        source: ConfigError::Io(err),
      }),
    }
  }

  /// Parse TOML body into a [`Config`].
  pub fn parse_toml(contents: &str) -> std::result::Result<Self, ConfigError> {
    let file: ConfigFile = toml::from_str(contents)?;
    Ok(Self { devices: file.device })
  }

  /// Resolve rename/hide for a Cast device by advertised name and id.
  ///
  /// Matching: case-insensitive substring of `id` against Cast name or Cast id.
  /// First matching entry wins. No match → identity map, not hidden.
  pub fn device_override(&self, cast_name: &str, cast_id: &str) -> DeviceOverride {
    for entry in &self.devices {
      if entry.id.is_empty() {
        continue;
      }
      if matches_device(&entry.id, cast_name, cast_id) {
        let display_name = entry
          .name
          .as_deref()
          .filter(|n| !n.is_empty())
          .map_or_else(|| cast_name.to_owned(), str::to_owned);
        return DeviceOverride { display_name, hidden: entry.hide };
      }
    }
    DeviceOverride {
      display_name: cast_name.to_owned(),
      hidden: false,
    }
  }
}

/// Resolve which path to attempt loading.
fn resolve_config_path(cli_path: Option<&Path>) -> PathBuf {
  if let Some(p) = cli_path {
    return p.to_path_buf();
  }
  if let Ok(env_path) = env::var(CONFIG_ENV) {
    let trimmed = env_path.trim();
    if !trimmed.is_empty() {
      return PathBuf::from(trimmed);
    }
  }
  PathBuf::from(DEFAULT_CONFIG_PATH)
}

fn matches_device(pattern: &str, cast_name: &str, cast_id: &str) -> bool {
  let pat = pattern.to_ascii_lowercase();
  let name = cast_name.to_ascii_lowercase();
  let id = cast_id.to_ascii_lowercase();
  name.contains(&pat) || id.contains(&pat)
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::io::Write;

  #[test]
  fn missing_file_yields_defaults() {
    let path = Path::new("/tmp/play-on-air-definitely-missing-xyz.toml");
    let cfg = Config::load_path(path).expect("missing is ok");
    assert_eq!(cfg, Config::default());
  }

  #[test]
  fn parse_device_rename_and_hide() {
    let toml = r#"
[[device]]
id = "Living Room TV"
name = "TV"
hide = false

[[device]]
id = "bedroom"
hide = true
"#;
    let cfg = Config::parse_toml(toml).expect("parse");
    assert_eq!(cfg.devices.len(), 2);

    let living = cfg.device_override("Living Room TV", "uuid-living");
    assert_eq!(living.display_name, "TV");
    assert!(!living.hidden);

    let bed = cfg.device_override("Bedroom Speaker", "cast-bedroom-id");
    assert_eq!(bed.display_name, "Bedroom Speaker");
    assert!(bed.hidden);

    let other = cfg.device_override("Kitchen", "other-id");
    assert_eq!(other.display_name, "Kitchen");
    assert!(!other.hidden);
  }

  #[test]
  fn match_is_case_insensitive_substring() {
    let cfg = Config::parse_toml(
      r#"
[[device]]
id = "LIVING"
name = "LR"
"#,
    )
    .expect("parse");
    let o = cfg.device_override("My Living Room", "x");
    assert_eq!(o.display_name, "LR");
  }

  #[test]
  fn match_by_uuid_substring() {
    let cfg = Config::parse_toml(
      r#"
[[device]]
id = "abc123"
name = "Renamed"
"#,
    )
    .expect("parse");
    let o = cfg.device_override("Some Name", "device-abc123-end");
    assert_eq!(o.display_name, "Renamed");
  }

  #[test]
  fn empty_name_keeps_cast_name() {
    let cfg = Config::parse_toml(
      r#"
[[device]]
id = "tv"
name = ""
"#,
    )
    .expect("parse");
    let o = cfg.device_override("Family TV", "id");
    assert_eq!(o.display_name, "Family TV");
  }

  #[test]
  fn load_optional_none_uses_default_path_missing_ok() {
    // Without env override this looks for ./play-on-air.toml; missing → Ok(default).
    let cfg = Config::load_optional(None).expect("defaults");
    // May or may not have devices if a local file exists; just ensure it does not error.
    drop(cfg);
  }

  #[test]
  fn load_explicit_temp_file() {
    let mut tmp = tempfile::NamedTempFile::new().expect("temp");
    write!(
      tmp,
      r#"
[[device]]
id = "x"
name = "Y"
"#
    )
    .expect("write");
    let cfg = Config::load_optional(Some(tmp.path())).expect("load");
    assert_eq!(cfg.devices.len(), 1);
    assert_eq!(cfg.devices[0].name.as_deref(), Some("Y"));
  }
}
