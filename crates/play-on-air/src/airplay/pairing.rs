//! Persistent `HomeKit` pairing state (accessory identity + paired controllers).
//!
//! Each AirPlay receiver keeps its Ed25519 identity seed and the long-term public
//! keys of paired controllers in one JSON file, so the advertised `pk` stays stable
//! across restarts and controllers added by the Home app (`/pair-add`) can still
//! pair-verify afterwards.
//!
//! Storage root: `$PLAY_ON_AIR_STATE_DIR` when set, otherwise `/data` when it is a
//! directory (the Home Assistant app data volume). With neither, receivers keep the
//! in-memory store and a fresh identity per start.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use shairplay::PairingStore;

/// Environment variable overriding the state directory.
pub const STATE_DIR_ENV: &str = "PLAY_ON_AIR_STATE_DIR";

/// Home Assistant app data volume, used when present and no override is set.
const HA_DATA_DIR: &str = "/data";

/// Subdirectory holding one pairing file per receiver.
const PAIRING_SUBDIR: &str = "pairing";

/// The resolved pairing directory, if any (resolved once per process).
pub fn pairing_dir() -> Option<&'static Path> {
  static DIR: OnceLock<Option<PathBuf>> = OnceLock::new();
  DIR
    .get_or_init(|| {
      let root = std::env::var_os(STATE_DIR_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| Path::new(HA_DATA_DIR).is_dir().then(|| PathBuf::from(HA_DATA_DIR)))?;
      Some(root.join(PAIRING_SUBDIR))
    })
    .as_deref()
}

/// On-disk shape of one receiver's pairing state (keys hex-encoded).
#[derive(Debug, Default, Serialize, Deserialize)]
struct PairingFile {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  identity: Option<String>,
  #[serde(default)]
  controllers: BTreeMap<String, String>,
}

/// In-memory state mirrored to [`PairingFile`].
#[derive(Debug, Default)]
struct PairingState {
  identity: Option<[u8; 32]>,
  controllers: BTreeMap<String, [u8; 32]>,
}

/// File-backed [`PairingStore`] for one receiver.
#[derive(Debug)]
pub struct FilePairingStore {
  path: PathBuf,
  state: Mutex<PairingState>,
}

impl FilePairingStore {
  /// Open (or start empty) the pairing file for `device_id` under `dir`.
  ///
  /// A missing file is a normal first start. An unreadable or corrupt file is
  /// logged and replaced on the next write, so the receiver still comes up.
  pub fn open(dir: &Path, device_id: &str) -> Self {
    let path = dir.join(format!("{}.json", file_stem(device_id)));
    let state = match std::fs::read(&path) {
      Ok(bytes) => match serde_json::from_slice::<PairingFile>(&bytes) {
        Ok(file) => PairingState::from_file(&file),
        Err(err) => {
          tracing::warn!(path = %path.display(), %err, "pairing file unreadable; starting fresh");
          PairingState::default()
        },
      },
      Err(err) if err.kind() == std::io::ErrorKind::NotFound => PairingState::default(),
      Err(err) => {
        tracing::warn!(path = %path.display(), %err, "pairing file could not be read; starting fresh");
        PairingState::default()
      },
    };
    Self { path, state: Mutex::new(state) }
  }

  /// Write the current state atomically (temp file + rename).
  fn persist(&self, state: &PairingState) {
    if let Err(err) = write_atomic(&self.path, &state.to_file()) {
      tracing::warn!(path = %self.path.display(), %err, "failed to persist pairing state");
    }
  }
}

impl PairingStore for FilePairingStore {
  fn get(&self, device_id: &str) -> Option<[u8; 32]> {
    self.state.lock().controllers.get(device_id).copied()
  }

  fn put(&self, device_id: &str, public_key: [u8; 32]) {
    let mut state = self.state.lock();
    if state.controllers.insert(device_id.to_owned(), public_key) != Some(public_key) {
      self.persist(&state);
    }
  }

  fn remove(&self, device_id: &str) {
    let mut state = self.state.lock();
    if state.controllers.remove(device_id).is_some() {
      self.persist(&state);
    }
  }

  fn has_any_pairing(&self) -> bool {
    !self.state.lock().controllers.is_empty()
  }

  fn list(&self) -> Vec<(String, [u8; 32])> {
    self
      .state
      .lock()
      .controllers
      .iter()
      .map(|(id, key)| (id.clone(), *key))
      .collect()
  }

  fn load_identity(&self) -> Option<[u8; 32]> {
    self.state.lock().identity
  }

  fn save_identity(&self, seed: [u8; 32]) {
    let mut state = self.state.lock();
    state.identity = Some(seed);
    self.persist(&state);
    drop(state);
  }
}

impl PairingState {
  fn from_file(file: &PairingFile) -> Self {
    Self {
      identity: file.identity.as_deref().and_then(decode_key),
      controllers: file
        .controllers
        .iter()
        .filter_map(|(id, hex)| decode_key(hex).map(|key| (id.clone(), key)))
        .collect(),
    }
  }

  fn to_file(&self) -> PairingFile {
    PairingFile {
      identity: self.identity.as_ref().map(encode_key),
      controllers: self.controllers.iter().map(|(id, key)| (id.clone(), encode_key(key))).collect(),
    }
  }
}

/// Filesystem-safe stem for a device id (anything but `[A-Za-z0-9_-]` → `_`).
fn file_stem(device_id: &str) -> String {
  device_id
    .chars()
    .map(|c| {
      if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
        c
      } else {
        '_'
      }
    })
    .collect()
}

fn write_atomic(path: &Path, file: &PairingFile) -> std::io::Result<()> {
  if let Some(parent) = path.parent() {
    std::fs::create_dir_all(parent)?;
  }
  let bytes = serde_json::to_vec_pretty(file).map_err(std::io::Error::other)?;
  let tmp = path.with_extension("json.tmp");
  std::fs::write(&tmp, bytes)?;
  std::fs::rename(&tmp, path)
}

fn encode_key(key: &[u8; 32]) -> String {
  key
    .iter()
    .flat_map(|byte| [byte >> 4, byte & 0x0f])
    .map(|nibble| char::from_digit(u32::from(nibble), 16).unwrap_or('0'))
    .collect()
}

fn decode_key(hex: &str) -> Option<[u8; 32]> {
  if hex.len() != 64 || !hex.is_ascii() {
    return None;
  }
  let mut key = [0_u8; 32];
  let (pairs, _) = hex.as_bytes().as_chunks::<2>();
  for (slot, pair) in key.iter_mut().zip(pairs) {
    let text = std::str::from_utf8(pair).ok()?;
    *slot = u8::from_str_radix(text, 16).ok()?;
  }
  Some(key)
}

#[cfg(test)]
mod tests {
  use super::*;

  fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("play-on-air-pairing-{name}-{}", std::process::id()));
    if dir.exists() {
      std::fs::remove_dir_all(&dir).unwrap();
    }
    dir
  }

  #[test]
  fn identity_and_controllers_survive_reopen() {
    let dir = temp_dir("reopen");
    let store = FilePairingStore::open(&dir, "Chromecast-abc:1");
    assert_eq!(store.load_identity(), None);
    store.save_identity([3; 32]);
    store.put("controller-1", [5; 32]);

    let reopened = FilePairingStore::open(&dir, "Chromecast-abc:1");
    assert_eq!(reopened.load_identity(), Some([3; 32]));
    assert_eq!(reopened.get("controller-1"), Some([5; 32]));
    assert!(reopened.has_any_pairing());
    assert_eq!(reopened.list(), vec![("controller-1".to_owned(), [5; 32])]);

    reopened.remove("controller-1");
    let after_remove = FilePairingStore::open(&dir, "Chromecast-abc:1");
    assert_eq!(after_remove.get("controller-1"), None);
    assert_eq!(after_remove.load_identity(), Some([3; 32]));
    std::fs::remove_dir_all(&dir).unwrap();
  }

  #[test]
  fn corrupt_file_starts_fresh() {
    let dir = temp_dir("corrupt");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("dev.json"), b"not json").unwrap();
    let store = FilePairingStore::open(&dir, "dev");
    assert_eq!(store.load_identity(), None);
    assert!(!store.has_any_pairing());
    std::fs::remove_dir_all(&dir).unwrap();
  }

  #[test]
  fn file_stem_is_filesystem_safe() {
    assert_eq!(file_stem("Kitchen speaker/1:2"), "Kitchen_speaker_1_2");
  }

  #[test]
  fn key_hex_round_trips_and_rejects_bad_input() {
    let key = [0xab; 32];
    assert_eq!(decode_key(&encode_key(&key)), Some(key));
    assert_eq!(decode_key("zz"), None);
    assert_eq!(decode_key(&"g".repeat(64)), None);
  }
}
