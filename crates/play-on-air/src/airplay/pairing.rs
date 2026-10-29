//! Persistent `HomeKit` pairing state (accessory identity, paired controllers, Home name).
//!
//! Each AirPlay receiver keeps its Ed25519 identity seed, the long-term public keys of
//! paired controllers, and the name the Home app gave it in one JSON file, so the
//! advertised `pk` stays stable across restarts, controllers added by the Home app
//! (`/pair-add`) can still pair-verify afterwards, and a speaker renamed in Home keeps
//! that AirPlay name.
//!
//! Files live in [`pairing_dir`] under the state directory the binary resolves
//! (`--state-dir`, `$PLAY_ON_AIR_STATE_DIR`, or the platform state directory). The
//! identity seed is a private key: files are written owner-only (`0600` on Unix).

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use shairplay::PairingStore;

/// Subdirectory of the state directory holding one pairing file per receiver.
const PAIRING_SUBDIR: &str = "pairing";

/// The pairing directory under `state_dir`.
pub fn pairing_dir(state_dir: &Path) -> PathBuf {
  state_dir.join(PAIRING_SUBDIR)
}

/// On-disk shape of one receiver's pairing state (keys hex-encoded).
#[derive(Debug, Default, Serialize, Deserialize)]
struct PairingFile {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  identity: Option<String>,
  #[serde(default)]
  controllers: BTreeMap<String, String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  home_name: Option<String>,
}

/// In-memory state mirrored to [`PairingFile`].
#[derive(Debug, Default)]
struct PairingState {
  identity: Option<[u8; 32]>,
  controllers: BTreeMap<String, [u8; 32]>,
  home_name: Option<String>,
}

/// [`PairingStore`] for one receiver, backed by its pairing file (or memory only).
#[derive(Debug)]
pub struct FilePairingStore {
  /// `None`: no state directory, state lives for this process only.
  path: Option<PathBuf>,
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
        Ok(file) => PairingState::from_file(file),
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
    Self {
      path: Some(path),
      state: Mutex::new(state),
    }
  }

  /// A store kept in memory only (no state directory): a fresh identity per process.
  pub fn in_memory() -> Self {
    Self {
      path: None,
      state: Mutex::new(PairingState::default()),
    }
  }

  /// Write the current state atomically (temp file + rename); no-op in memory.
  fn persist(&self, state: &PairingState) {
    let Some(path) = &self.path else {
      return;
    };
    if let Err(err) = write_atomic(path, &state.to_file()) {
      tracing::warn!(path = %path.display(), %err, "failed to persist pairing state");
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

  /// Removing the last controller means the speaker left the home: forget its Home name.
  fn remove(&self, device_id: &str) {
    let mut state = self.state.lock();
    if state.controllers.remove(device_id).is_some() {
      if state.controllers.is_empty()
        && let Some(name) = state.home_name.take()
      {
        tracing::info!(home_name = %name, "speaker left Apple Home; dropping its Home name");
      }
      self.persist(&state);
    }
    drop(state);
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

  fn home_name(&self) -> Option<String> {
    self.state.lock().home_name.clone()
  }

  fn set_home_name(&self, name: &str) {
    let mut state = self.state.lock();
    if state.home_name.as_deref() != Some(name) {
      tracing::info!(home_name = %name, "saved the speaker's Apple Home name");
      state.home_name = Some(name.to_owned());
      self.persist(&state);
    }
    drop(state);
  }
}

impl PairingState {
  fn from_file(file: PairingFile) -> Self {
    Self {
      identity: file.identity.as_deref().and_then(decode_key),
      controllers: file
        .controllers
        .iter()
        .filter_map(|(id, hex)| decode_key(hex).map(|key| (id.clone(), key)))
        .collect(),
      home_name: file.home_name.filter(|name| !name.trim().is_empty()),
    }
  }

  fn to_file(&self) -> PairingFile {
    PairingFile {
      identity: self.identity.as_ref().map(encode_key),
      controllers: self.controllers.iter().map(|(id, key)| (id.clone(), encode_key(key))).collect(),
      home_name: self.home_name.clone(),
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

/// Write `file` to `path` atomically (owner-only temp file, fsync, rename).
fn write_atomic(path: &Path, file: &PairingFile) -> std::io::Result<()> {
  if let Some(parent) = path.parent() {
    std::fs::create_dir_all(parent)?;
  }
  let bytes = serde_json::to_vec_pretty(file).map_err(std::io::Error::other)?;
  let tmp = path.with_extension("json.tmp");
  // A temp file left by a crash keeps its old mode on open; start from a fresh one.
  if let Err(err) = std::fs::remove_file(&tmp)
    && err.kind() != std::io::ErrorKind::NotFound
  {
    return Err(err);
  }
  #[cfg(unix)]
  let mut out =
    std::os::unix::fs::OpenOptionsExt::mode(std::fs::OpenOptions::new().write(true).create_new(true), 0o600)
      .open(&tmp)?;
  #[cfg(not(unix))]
  let mut out = std::fs::OpenOptions::new().write(true).create_new(true).open(&tmp)?;
  out.write_all(&bytes)?;
  out.sync_all()?;
  drop(out);
  std::fs::rename(&tmp, path)
}

fn encode_key(key: &[u8; 32]) -> String {
  hex::encode(key)
}

fn decode_key(text: &str) -> Option<[u8; 32]> {
  let mut key = [0_u8; 32];
  hex::decode_to_slice(text, &mut key).ok()?;
  Some(key)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn identity_and_controllers_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let store = FilePairingStore::open(dir.path(), "Chromecast-abc:1");
    assert_eq!(store.load_identity(), None);
    store.save_identity([3; 32]);
    store.put("controller-1", [5; 32]);

    let reopened = FilePairingStore::open(dir.path(), "Chromecast-abc:1");
    assert_eq!(reopened.load_identity(), Some([3; 32]));
    assert_eq!(reopened.get("controller-1"), Some([5; 32]));
    assert!(reopened.has_any_pairing());
    assert_eq!(reopened.list(), vec![("controller-1".to_owned(), [5; 32])]);

    reopened.remove("controller-1");
    let after_remove = FilePairingStore::open(dir.path(), "Chromecast-abc:1");
    assert_eq!(after_remove.get("controller-1"), None);
    assert_eq!(after_remove.load_identity(), Some([3; 32]));
  }

  #[test]
  fn corrupt_file_starts_fresh() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("dev.json"), b"not json").unwrap();
    let store = FilePairingStore::open(dir.path(), "dev");
    assert_eq!(store.load_identity(), None);
    assert!(!store.has_any_pairing());
  }

  #[test]
  fn home_name_survives_reopen_until_the_last_controller_leaves() {
    let dir = tempfile::tempdir().unwrap();
    let store = FilePairingStore::open(dir.path(), "dev");
    store.put("iphone", [1; 32]);
    store.put("hub", [2; 32]);
    store.set_home_name("Office Speaker");

    let reopened = FilePairingStore::open(dir.path(), "dev");
    assert_eq!(reopened.home_name().as_deref(), Some("Office Speaker"));

    reopened.remove("hub");
    assert_eq!(reopened.home_name().as_deref(), Some("Office Speaker"));
    reopened.remove("iphone");
    assert_eq!(reopened.home_name(), None);
    assert_eq!(FilePairingStore::open(dir.path(), "dev").home_name(), None);
  }

  #[cfg(unix)]
  #[test]
  fn pairing_file_is_owner_only() {
    use std::os::unix::fs::PermissionsExt as _;

    let dir = tempfile::tempdir().unwrap();
    // A stale temp file from a crash must not pass its mode on to the pairing file.
    let stale = dir.path().join("dev.json.tmp");
    std::fs::write(&stale, b"{}").unwrap();
    std::fs::set_permissions(&stale, std::fs::Permissions::from_mode(0o644)).unwrap();

    let store = FilePairingStore::open(dir.path(), "dev");
    store.save_identity([7; 32]);
    let mode = std::fs::metadata(dir.path().join("dev.json")).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
    assert!(!stale.exists());
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
    assert_eq!(decode_key(&"ab".repeat(31)), None);
  }
}
