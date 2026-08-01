//! In-memory registry of discovered Chromecast devices.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

/// A discovered Google Cast device on the LAN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Device {
  /// Stable id (typically Cast UUID from TXT `id` or instance name).
  pub id: String,
  /// Advertised friendly name.
  pub name: String,
  /// Resolved host or IP for Cast control.
  pub host: String,
  /// Cast port (usually 8009).
  pub port: u16,
  /// Last time this device was seen via mDNS.
  pub last_seen: Instant,
}

/// Thread-safe map of present Cast devices.
#[derive(Debug, Default)]
pub struct DeviceRegistry {
  inner: RwLock<HashMap<String, Device>>,
}

impl DeviceRegistry {
  /// Create an empty registry.
  pub fn new() -> Self {
    Self::default()
  }

  /// Record or refresh a device (appear / re-appear).
  pub fn appear(&self, device: Device) {
    let mut guard = self.inner.write();
    let key = device.id.clone();
    drop(guard.insert(key, device));
  }

  /// Remove a device that left the network.
  pub fn leave(&self, id: &str) -> Option<Device> {
    let mut guard = self.inner.write();
    guard.remove(id)
  }

  /// Snapshot of all currently known devices.
  pub fn list(&self) -> Vec<Device> {
    let guard = self.inner.read();
    guard.values().cloned().collect()
  }

  /// Look up one device by id.
  pub fn get(&self, id: &str) -> Option<Device> {
    let guard = self.inner.read();
    guard.get(id).cloned()
  }

  /// Number of known devices.
  pub fn len(&self) -> usize {
    self.inner.read().len()
  }

  /// Whether the registry has no devices.
  pub fn is_empty(&self) -> bool {
    self.inner.read().is_empty()
  }

  /// Drop devices not seen within `max_age` (stale cleanup helper).
  pub fn prune_older_than(&self, max_age: Duration) -> Vec<Device> {
    let now = Instant::now();
    let mut guard = self.inner.write();
    let stale: Vec<String> = guard
      .iter()
      .filter_map(|(id, dev)| {
        if now.duration_since(dev.last_seen) > max_age {
          Some(id.clone())
        } else {
          None
        }
      })
      .collect();
    let mut removed = Vec::with_capacity(stale.len());
    for id in stale {
      if let Some(dev) = guard.remove(&id) {
        removed.push(dev);
      }
    }
    removed
  }

  /// Expire devices that have not re-appeared within `ttl`.
  ///
  /// Used when mDNS `ServiceRemoved` is missed so AirPlay ads are withdrawn.
  pub fn expire_stale(&self, ttl: Duration) -> Vec<Device> {
    self.prune_older_than(ttl)
  }
}

/// Default TTL for registry entries without a re-sighting.
///
/// System DNS-SD often fires `Added` once until `Removed`. A short TTL was
/// withdrawing live Chromecasts and their AirPlay ads after ~90s. Use a long
/// safety net; primary removal is still `ServiceRemoved`.
pub const DEFAULT_STALE_TTL: Duration = Duration::from_secs(86_400);

#[cfg(test)]
mod tests {
  use super::*;

  fn sample(id: &str, name: &str) -> Device {
    Device {
      id: id.to_owned(),
      name: name.to_owned(),
      host: "192.168.1.10".to_owned(),
      port: 8009,
      last_seen: Instant::now(),
    }
  }

  #[test]
  fn appear_list_leave() {
    let reg = DeviceRegistry::new();
    assert!(reg.is_empty());
    reg.appear(sample("a", "A"));
    reg.appear(sample("b", "B"));
    assert_eq!(reg.len(), 2);
    let list = reg.list();
    assert_eq!(list.len(), 2);
    let left = reg.leave("a").expect("left");
    assert_eq!(left.id, "a");
    assert_eq!(reg.len(), 1);
    assert!(reg.get("b").is_some());
  }

  #[test]
  fn appear_updates_existing() {
    let reg = DeviceRegistry::new();
    reg.appear(sample("a", "Old"));
    reg.appear(sample("a", "New"));
    assert_eq!(reg.len(), 1);
    assert_eq!(reg.get("a").map(|d| d.name), Some("New".to_owned()));
  }

  #[test]
  fn expire_stale_removes_old_devices() {
    let reg = DeviceRegistry::new();
    let mut old = sample("stale", "Stale");
    old.last_seen = Instant::now().checked_sub(Duration::from_secs(86_400 + 60)).expect("clock");
    reg.appear(old);
    reg.appear(sample("fresh", "Fresh"));

    let removed = reg.expire_stale(DEFAULT_STALE_TTL);
    assert_eq!(removed.len(), 1);
    assert_eq!(removed.first().map(|d| d.id.as_str()), Some("stale"));
    assert_eq!(reg.len(), 1);
    assert!(reg.get("fresh").is_some());
    assert!(reg.get("stale").is_none());
  }

  #[test]
  fn expire_stale_keeps_recent() {
    let reg = DeviceRegistry::new();
    reg.appear(sample("a", "A"));
    let removed = reg.expire_stale(DEFAULT_STALE_TTL);
    assert!(removed.is_empty());
    assert_eq!(reg.len(), 1);
  }
}
