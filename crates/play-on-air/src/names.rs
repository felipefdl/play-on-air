//! AirPlay display names over Chromecast advertised names.
//!
//! Default: identity map (Cast `fn` → AirPlay name). Optional TOML may rename or hide
//! ([`Config::device_override`]). When two visible Cast devices share a friendly name,
//! the auto-named ones get a deterministic suffix so Bonjour does not pick its own
//! non-deterministic ` (2)` rename for the second AirPlay advertisement.

use std::collections::HashMap;

use crate::config::Config;
use crate::registry::Device;

/// Shortest Cast-id suffix appended when a model cannot tell duplicates apart.
const MIN_ID_SUFFIX_LEN: usize = 4;

/// AirPlay advertisement names for every non-hidden device, keyed by Cast id.
///
/// - A config rename is used verbatim.
/// - A unique Cast name is used as-is.
/// - Auto-named devices whose name collides (case-insensitive, as Bonjour compares)
///   with another visible device get `Name (Model)` when their TXT `md` model is
///   unique in the collision group and differs from the name, else `Name (<id suffix>)`
///   with the shortest (≥ 4 chars) Cast-id prefix that is unique in the group.
///
/// The result depends only on the set of devices (ids, names, models) and config,
/// never on discovery order, so names are stable across restarts.
#[must_use]
pub fn airplay_names(devices: &[Device], config: &Config) -> HashMap<String, String> {
  let mut visible: Vec<Candidate<'_>> = devices
    .iter()
    .filter_map(|device| {
      let over = config.device_override(&device.name, &device.id);
      (!over.hidden).then(|| Candidate {
        id: &device.id,
        model: device.model.trim(),
        name: over.display_name,
        fixed: over.renamed,
      })
    })
    .collect();
  visible.sort_unstable_by_key(|cand| cand.id);

  let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
  for (idx, cand) in visible.iter().enumerate() {
    groups.entry(name_key(&cand.name)).or_default().push(idx);
  }

  let mut names: HashMap<String, String> = HashMap::with_capacity(visible.len());
  for members in groups.values() {
    let auto: Vec<&Candidate<'_>> = members
      .iter()
      .filter_map(|&idx| visible.get(idx))
      .filter(|cand| !cand.fixed)
      .collect();
    if members.len() < 2 {
      for cand in auto {
        drop(names.insert(cand.id.to_owned(), cand.name.clone()));
      }
      continue;
    }
    for (id, name) in disambiguate(&auto) {
      drop(names.insert(id.to_owned(), name));
    }
  }
  for cand in visible.iter().filter(|cand| cand.fixed) {
    drop(names.insert(cand.id.to_owned(), cand.name.clone()));
  }

  resolve_suffix_clashes(&visible, &mut names);
  names
}

/// One visible device on its way to an AirPlay name.
struct Candidate<'a> {
  id: &'a str,
  model: &'a str,
  /// Config display name (rename or Cast `fn`).
  name: String,
  /// Config rename: never suffixed.
  fixed: bool,
}

/// Bonjour compares service instance names case-insensitively.
fn name_key(name: &str) -> String {
  name.trim().to_lowercase()
}

/// Suffix every auto-named member of one collision group.
fn disambiguate<'a>(auto: &[&Candidate<'a>]) -> Vec<(&'a str, String)> {
  let ids: Vec<&str> = auto.iter().map(|cand| cand.id).collect();
  auto
    .iter()
    .map(|cand| {
      let model_unique = !cand.model.is_empty()
        && name_key(cand.model) != name_key(&cand.name)
        && auto
          .iter()
          .filter(|other| name_key(other.model) == name_key(cand.model))
          .count()
          == 1;
      let suffix = if model_unique {
        cand.model.to_owned()
      } else {
        id_suffix(cand.id, &ids)
      };
      (cand.id, format!("{} ({suffix})", cand.name))
    })
    .collect()
}

/// Shortest prefix (≥ [`MIN_ID_SUFFIX_LEN`]) of `id`'s alphanumerics unique among `group`.
fn id_suffix(id: &str, group: &[&str]) -> String {
  let mine = id_chars(id);
  let others: Vec<String> = group.iter().copied().filter(|&other| other != id).map(id_chars).collect();
  let total = mine.chars().count();
  let mut len = MIN_ID_SUFFIX_LEN.min(total);
  loop {
    let prefix: String = mine.chars().take(len).collect();
    let clashes = others.iter().any(|other| other.starts_with(&prefix));
    if !clashes || len >= total {
      return prefix;
    }
    len += 1;
  }
}

/// Lowercased alphanumerics of a Cast id (UUIDs with or without dashes compare equal).
fn id_chars(id: &str) -> String {
  id.chars()
    .filter(char::is_ascii_alphanumeric)
    .map(|c| c.to_ascii_lowercase())
    .collect()
}

/// A generated `Name (Model)` may equal another device's real name: fall back to the
/// id suffix for the generated one so every advertised name stays distinct.
fn resolve_suffix_clashes(visible: &[Candidate<'_>], names: &mut HashMap<String, String>) {
  let mut counts: HashMap<String, usize> = HashMap::new();
  for name in names.values() {
    *counts.entry(name_key(name)).or_default() += 1;
  }
  let all_ids: Vec<&str> = visible.iter().map(|cand| cand.id).collect();
  for cand in visible {
    let Some(current) = names.get(cand.id) else {
      continue;
    };
    let generated = !cand.fixed && *current != cand.name;
    if !generated || counts.get(&name_key(current)).copied().unwrap_or(0) < 2 {
      continue;
    }
    let replacement = format!("{} ({})", cand.name, id_suffix(cand.id, &all_ids));
    drop(names.insert(cand.id.to_owned(), replacement));
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::config::Config;

  fn device(id: &str, name: &str, model: &str) -> Device {
    Device::new(id, name, "192.168.1.10", "x.local", 8009, id).with_model(model)
  }

  fn names_of(devices: &[Device], cfg: &Config) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = airplay_names(devices, cfg).into_iter().collect();
    out.sort();
    out
  }

  #[test]
  fn identity_without_config() {
    let cfg = Config::default();
    let devices = [device("a1", "Kitchen", "Nest Audio")];
    assert_eq!(names_of(&devices, &cfg), vec![("a1".to_owned(), "Kitchen".to_owned())]);
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
    let devices = [device("a1", "Kitchen Display", "Nest Hub")];
    assert_eq!(names_of(&devices, &cfg), vec![("a1".to_owned(), "Cook".to_owned())]);
  }

  #[test]
  fn same_name_different_models_get_model_suffix() {
    let cfg = Config::default();
    let devices = [
      device("7e5621b9975022b24ef6a17173a19135", "Living Room", "Nest Audio"),
      device("d417a9b9bf1ebe211639512dad18be1d", "living room", "Google Nest Mini"),
    ];
    assert_eq!(
      names_of(&devices, &cfg),
      vec![
        (
          "7e5621b9975022b24ef6a17173a19135".to_owned(),
          "Living Room (Nest Audio)".to_owned()
        ),
        (
          "d417a9b9bf1ebe211639512dad18be1d".to_owned(),
          "living room (Google Nest Mini)".to_owned()
        ),
      ]
    );
  }

  #[test]
  fn same_name_same_model_gets_stable_id_suffix() {
    let cfg = Config::default();
    let devices = [
      device("7e5621b9975022b24ef6a17173a19135", "Kitchen", "Nest Audio"),
      device("7e56ffff975022b24ef6a17173a19999", "Kitchen", "Nest Audio"),
      device("d417a9b9bf1ebe211639512dad18be1d", "Kitchen", "Google Nest Mini"),
    ];
    let expected = vec![
      ("7e5621b9975022b24ef6a17173a19135".to_owned(), "Kitchen (7e562)".to_owned()),
      ("7e56ffff975022b24ef6a17173a19999".to_owned(), "Kitchen (7e56f)".to_owned()),
      (
        "d417a9b9bf1ebe211639512dad18be1d".to_owned(),
        "Kitchen (Google Nest Mini)".to_owned(),
      ),
    ];
    assert_eq!(names_of(&devices, &cfg), expected);
    // Discovery order must not matter (stable across restarts).
    let reversed: Vec<Device> = devices.iter().rev().cloned().collect();
    assert_eq!(names_of(&reversed, &cfg), expected);
  }

  #[test]
  fn config_rename_and_hide_take_precedence_over_disambiguation() {
    let cfg = Config::parse_toml(
      r#"
[[device]]
id = "aaaa1111"
name = "Kitchen"

[[device]]
id = "cccc3333"
hide = true
"#,
    )
    .expect("parse");
    let devices = [
      device("aaaa1111", "Den", "Nest Audio"),
      device("bbbb2222", "Kitchen", "Nest Mini"),
      device("cccc3333", "Kitchen", "Nest Mini"),
    ];
    // Renamed device keeps its chosen name verbatim; the hidden one does not count as a
    // collision; the auto-named Cast "Kitchen" is suffixed instead.
    assert_eq!(
      names_of(&devices, &cfg),
      vec![
        ("aaaa1111".to_owned(), "Kitchen".to_owned()),
        ("bbbb2222".to_owned(), "Kitchen (Nest Mini)".to_owned()),
      ]
    );
  }

  #[test]
  fn model_equal_to_name_or_generated_clash_falls_back_to_id() {
    let cfg = Config::default();
    let devices = [
      device("1111aaaa", "Nest Audio", "Nest Audio"),
      device("2222bbbb", "Nest Audio", "Nest Mini"),
      device("3333cccc", "Office", "Nest Mini"),
      device("4444dddd", "Office", "Nest Hub"),
      device("5555eeee", "Office (Nest Hub)", "Chromecast"),
    ];
    let names = airplay_names(&devices, &cfg);
    assert_eq!(names.get("1111aaaa").map(String::as_str), Some("Nest Audio (1111)"));
    assert_eq!(names.get("2222bbbb").map(String::as_str), Some("Nest Audio (Nest Mini)"));
    assert_eq!(names.get("3333cccc").map(String::as_str), Some("Office (Nest Mini)"));
    assert_eq!(names.get("4444dddd").map(String::as_str), Some("Office (4444)"));
    assert_eq!(names.get("5555eeee").map(String::as_str), Some("Office (Nest Hub)"));
    let mut keys: Vec<String> = names.values().map(|n| n.to_lowercase()).collect();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), devices.len(), "every advertised name must be distinct");
  }
}
