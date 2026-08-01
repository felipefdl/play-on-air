//! AirPlay display-name identity map over Chromecast advertised names.

use crate::config::Config;

/// Compute the AirPlay advertisement name for a Cast device.
///
/// Default: identity map (`cast_name`). Optional TOML may rename via
/// [`Config::device_override`].
pub fn airplay_name(cast_name: &str, config: &Config) -> String {
  airplay_name_with_id(cast_name, "", config)
}

/// Like [`airplay_name`], also matching against Cast device id / UUID.
pub fn airplay_name_with_id(cast_name: &str, cast_id: &str, config: &Config) -> String {
  config.device_override(cast_name, cast_id).display_name
}

/// Whether the device should be hidden from AirPlay advertisement.
pub fn is_hidden(cast_name: &str, cast_id: &str, config: &Config) -> bool {
  config.device_override(cast_name, cast_id).hidden
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::config::Config;

  #[test]
  fn identity_without_config() {
    let cfg = Config::default();
    assert_eq!(airplay_name("Kitchen", &cfg), "Kitchen");
    assert!(!is_hidden("Kitchen", "id", &cfg));
  }

  #[test]
  fn rename_from_config() {
    let cfg = Config::parse_toml(
      r#"
[[device]]
id = "Kitchen"
name = "Cook"
"#,
    )
    .expect("parse");
    assert_eq!(airplay_name("Kitchen Display", &cfg), "Cook");
  }
}
