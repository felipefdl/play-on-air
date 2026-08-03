//! In-memory registry of discovered Chromecast devices.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

/// How long after a remove event before the device actually leaves the registry.
pub const DEFAULT_PENDING_LEAVE: Duration = Duration::from_secs(20);

/// Floor for withdrawing an AirPlay receiver after a device is no longer desired
/// when a bridge session may have been active (gone + session-ended gate).
pub const SESSION_GUARD_GONE: Duration = Duration::from_secs(60);

/// Default TTL for registry entries without a re-sighting (stale backstop).
///
/// Primary removal is debounced mDNS leave (`pending_leave`). This TTL withdraws
/// AirPlay ads for devices that silently disappear without a remove event.
pub const DEFAULT_STALE_TTL: Duration = Duration::from_secs(600);

/// A discovered Google Cast device on the LAN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Device {
  /// Stable id (typically Cast UUID from TXT `id` or instance name).
  pub id: String,
  /// Advertised friendly name.
  pub name: String,
  /// Preferred IPv4 (or host string) for Cast control.
  pub host: String,
  /// mDNS hostname (e.g. `uuid.local`) for re-resolution before connect.
  pub hostname: String,
  /// Cast port (usually 8009).
  pub port: u16,
  /// Last time this device was seen via mDNS.
  pub last_seen: Instant,
  /// mDNS instance name recorded at appear time (exact leave matching).
  pub instance: String,
  /// When set, device will leave after this deadline unless cancelled by re-appear.
  pub pending_leave_deadline: Option<Instant>,
  /// When pending-leave started (session-guard / observability).
  pub pending_leave_since: Option<Instant>,
}

impl Device {
  /// Build a device with no pending leave (typical appear path).
  #[must_use]
  pub fn new(
    id: impl Into<String>,
    name: impl Into<String>,
    host: impl Into<String>,
    hostname: impl Into<String>,
    port: u16,
    instance: impl Into<String>,
  ) -> Self {
    Self {
      id: id.into(),
      name: name.into(),
      host: host.into(),
      hostname: hostname.into(),
      port,
      last_seen: Instant::now(),
      instance: instance.into(),
      pending_leave_deadline: None,
      pending_leave_since: None,
    }
  }
}

/// Pure: whether a pending-leave deadline has elapsed.
#[must_use]
pub fn pending_leave_is_due(deadline: Instant, now: Instant) -> bool {
  now.checked_duration_since(deadline).is_some()
}

/// Decision for withdrawing an AirPlay receiver that may no longer be desired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WithdrawDecision {
  /// Device is still desired — keep advertising.
  Keep,
  /// Live bridge session or session-guard floor not met — do not withdraw yet.
  Defer,
  /// Safe to withdraw the AirPlay receiver (and warm Cast worker).
  Withdraw,
}

/// Pure: whether `maintain_airplay` may withdraw a receiver.
///
/// - Still desired → [`WithdrawDecision::Keep`]
/// - Live bridge session → always [`WithdrawDecision::Defer`]
/// - Not desired, no session, gone for at least `min_gone` → [`WithdrawDecision::Withdraw`]
/// - Not desired, no session, gone less than `min_gone` → [`WithdrawDecision::Defer`]
///
/// Callers pass `min_gone = Duration::ZERO` for pure idle leave (withdraw on the next tick
/// after the device leaves the desired set). Pass [`SESSION_GUARD_GONE`] only when a live
/// session previously blocked withdraw while the device was already not desired.
#[must_use]
pub fn decide_airplay_withdraw(
  is_desired: bool,
  has_session: bool,
  gone_for: Duration,
  min_gone: Duration,
) -> WithdrawDecision {
  if is_desired {
    return WithdrawDecision::Keep;
  }
  if has_session {
    return WithdrawDecision::Defer;
  }
  if gone_for >= min_gone {
    WithdrawDecision::Withdraw
  } else {
    WithdrawDecision::Defer
  }
}

/// Outcome of attempting to mark a device for debounced leave.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingLeaveMark {
  /// Device id is not in the registry.
  NotFound,
  /// First transition into pending leave (deadline set).
  NewlyMarked,
  /// Already pending; deadline and `pending_leave_since` unchanged.
  AlreadyPending,
}

/// Pure: exact match for leave-by-instance (no substring heuristics).
///
/// Prefer matching `id == instance` (TXT id often equals browse instance on some
/// stacks); otherwise match the stored mDNS `instance` field exactly.
#[must_use]
pub fn match_device_for_leave<'a>(devices: &'a [Device], instance: &str) -> Option<&'a Device> {
  devices
    .iter()
    .find(|d| d.id == instance)
    .or_else(|| devices.iter().find(|d| d.instance == instance))
}

/// Thread-safe map of present Cast devices.
#[derive(Debug, Default)]
pub struct DeviceRegistry {
  inner: RwLock<HashMap<String, Device>>,
  /// Ids that cancelled a pending leave via [`Self::appear`] since the last drain.
  ///
  /// Maintain drains this each tick to reset volume-seed attempts on re-sight.
  pending_leave_cancellations: Mutex<Vec<String>>,
}

impl DeviceRegistry {
  /// Create an empty registry.
  pub fn new() -> Self {
    Self::default()
  }

  /// Record or refresh a device (appear / re-appear).
  ///
  /// Re-appearance cancels any pending leave and refreshes `last_seen`.
  /// Returns `true` if this id was not previously present (first appear).
  ///
  /// When a pending leave is cancelled, the id is queued for
  /// [`Self::drain_pending_leave_cancellations`].
  pub fn appear(&self, mut device: Device) -> bool {
    let mut guard = self.inner.write();
    let key = device.id.clone();
    let is_new = !guard.contains_key(&key);
    let was_pending = guard.get(&key).is_some_and(|d| d.pending_leave_deadline.is_some());
    // Appear always clears pending leave (re-sight cancels debounce).
    device.pending_leave_deadline = None;
    device.pending_leave_since = None;
    device.last_seen = Instant::now();
    drop(guard.insert(key.clone(), device));
    drop(guard);
    if was_pending {
      self.pending_leave_cancellations.lock().push(key);
    }
    is_new
  }

  /// Drain device ids that cancelled a pending leave via re-appear since the last drain.
  #[must_use]
  pub fn drain_pending_leave_cancellations(&self) -> Vec<String> {
    std::mem::take(&mut *self.pending_leave_cancellations.lock())
  }

  /// Remove a device that left the network immediately (no debounce).
  pub fn leave(&self, id: &str) -> Option<Device> {
    let mut guard = self.inner.write();
    guard.remove(id)
  }

  /// Mark a device pending leave by stable id.
  ///
  /// A second call while already pending does **not** move the deadline.
  pub fn mark_pending_leave(&self, id: &str, now: Instant, grace: Duration) -> PendingLeaveMark {
    let mut guard = self.inner.write();
    let Some(dev) = guard.get_mut(id) else {
      return PendingLeaveMark::NotFound;
    };
    if dev.pending_leave_deadline.is_some() {
      return PendingLeaveMark::AlreadyPending;
    }
    dev.pending_leave_since = Some(now);
    dev.pending_leave_deadline = Some(now + grace);
    drop(guard);
    PendingLeaveMark::NewlyMarked
  }

  /// Mark pending leave by exact instance / id match.
  ///
  /// Returns `(device_id, mark)` when a device matched; `None` if no device matches.
  pub fn mark_pending_leave_by_instance(
    &self,
    instance: &str,
    now: Instant,
    grace: Duration,
  ) -> Option<(String, PendingLeaveMark)> {
    let list: Vec<Device> = {
      let guard = self.inner.read();
      guard.values().cloned().collect()
    };
    let id = match_device_for_leave(&list, instance)?.id.clone();
    let mark = self.mark_pending_leave(&id, now, grace);
    Some((id, mark))
  }

  /// Cancel pending leave for `id` (e.g. re-appear). Returns `true` if a pending leave was cleared.
  pub fn cancel_pending_leave(&self, id: &str) -> bool {
    let mut guard = self.inner.write();
    let Some(dev) = guard.get_mut(id) else {
      return false;
    };
    let was_pending = dev.pending_leave_deadline.is_some();
    dev.pending_leave_deadline = None;
    dev.pending_leave_since = None;
    drop(guard);
    was_pending
  }

  /// Whether the device is in pending-leave state.
  pub fn is_pending_leave(&self, id: &str) -> bool {
    let guard = self.inner.read();
    guard.get(id).is_some_and(|d| d.pending_leave_deadline.is_some())
  }

  /// Remove devices whose pending-leave deadline has elapsed. Returns the removed devices.
  pub fn take_due_leaves(&self, now: Instant) -> Vec<Device> {
    let mut guard = self.inner.write();
    let due: Vec<String> = guard
      .iter()
      .filter_map(|(id, dev)| {
        let deadline = dev.pending_leave_deadline?;
        if pending_leave_is_due(deadline, now) {
          Some(id.clone())
        } else {
          None
        }
      })
      .collect();
    let mut removed = Vec::with_capacity(due.len());
    for id in due {
      if let Some(dev) = guard.remove(&id) {
        removed.push(dev);
      }
    }
    removed
  }

  /// Snapshot of all currently known devices (including pending-leave).
  pub fn list(&self) -> Vec<Device> {
    let guard = self.inner.read();
    guard.values().cloned().collect()
  }

  /// Snapshot of devices that are not pending leave (still fully present).
  pub fn list_present(&self) -> Vec<Device> {
    let guard = self.inner.read();
    guard.values().filter(|d| d.pending_leave_deadline.is_none()).cloned().collect()
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
  ///
  /// Skips devices with an active pending leave (those are handled by [`take_due_leaves`]).
  pub fn prune_older_than(&self, max_age: Duration) -> Vec<Device> {
    let now = Instant::now();
    let mut guard = self.inner.write();
    let stale: Vec<String> = guard
      .iter()
      .filter_map(|(id, dev)| {
        if dev.pending_leave_deadline.is_some() {
          return None;
        }
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
  /// Backstop when mDNS remove is missed so AirPlay ads are withdrawn.
  pub fn expire_stale(&self, ttl: Duration) -> Vec<Device> {
    self.prune_older_than(ttl)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn sample(id: &str, name: &str) -> Device {
    Device::new(id, name, "192.168.1.10", "speaker.local", 8009, name)
  }

  fn sample_with_instance(id: &str, name: &str, instance: &str) -> Device {
    Device::new(id, name, "192.168.1.10", "speaker.local", 8009, instance)
  }

  #[test]
  fn appear_list_leave() {
    let reg = DeviceRegistry::new();
    assert!(reg.is_empty());
    assert!(reg.appear(sample("a", "A")));
    assert!(reg.appear(sample("b", "B")));
    assert_eq!(reg.len(), 2);
    let list = reg.list();
    assert_eq!(list.len(), 2);
    let left = reg.leave("a").expect("left");
    assert_eq!(left.id, "a");
    assert_eq!(reg.len(), 1);
    assert!(reg.get("b").is_some());
  }

  #[test]
  fn appear_updates_existing_and_cancels_pending() {
    let reg = DeviceRegistry::new();
    assert!(reg.appear(sample("a", "Old")));
    let now = Instant::now();
    assert_eq!(
      reg.mark_pending_leave("a", now, DEFAULT_PENDING_LEAVE),
      PendingLeaveMark::NewlyMarked
    );
    assert!(reg.is_pending_leave("a"));
    assert!(!reg.appear(sample("a", "New")));
    assert!(!reg.is_pending_leave("a"));
    assert_eq!(reg.len(), 1);
    assert_eq!(reg.get("a").map(|d| d.name), Some("New".to_owned()));
    assert_eq!(reg.drain_pending_leave_cancellations(), vec!["a".to_owned()]);
    assert!(reg.drain_pending_leave_cancellations().is_empty());
  }

  #[test]
  fn appear_without_pending_does_not_queue_cancellation() {
    let reg = DeviceRegistry::new();
    assert!(reg.appear(sample("a", "A")));
    assert!(!reg.appear(sample("a", "A2")));
    assert!(reg.drain_pending_leave_cancellations().is_empty());
  }

  #[test]
  fn expire_stale_removes_old_devices() {
    let reg = DeviceRegistry::new();
    let _ = reg.appear(sample("stale", "Stale"));
    let _ = reg.appear(sample("fresh", "Fresh"));
    // appear() refreshes last_seen; backdate the stale entry in place.
    {
      let mut guard = reg.inner.write();
      if let Some(dev) = guard.get_mut("stale") {
        dev.last_seen = Instant::now()
          .checked_sub(DEFAULT_STALE_TTL + Duration::from_secs(60))
          .expect("clock");
      }
    }

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
    let _ = reg.appear(sample("a", "A"));
    let removed = reg.expire_stale(DEFAULT_STALE_TTL);
    assert!(removed.is_empty());
    assert_eq!(reg.len(), 1);
  }

  #[test]
  fn expire_stale_skips_pending_leave() {
    let reg = DeviceRegistry::new();
    let _ = reg.appear(sample("p", "P"));
    {
      let mut guard = reg.inner.write();
      if let Some(dev) = guard.get_mut("p") {
        dev.last_seen = Instant::now()
          .checked_sub(DEFAULT_STALE_TTL + Duration::from_secs(60))
          .expect("clock");
      }
    }
    assert_eq!(
      reg.mark_pending_leave("p", Instant::now(), DEFAULT_PENDING_LEAVE),
      PendingLeaveMark::NewlyMarked
    );
    let removed = reg.expire_stale(DEFAULT_STALE_TTL);
    assert!(removed.is_empty());
    assert!(reg.get("p").is_some());
  }

  #[test]
  fn pending_leave_deadline_and_take_due() {
    let reg = DeviceRegistry::new();
    let _ = reg.appear(sample("a", "A"));
    let now = Instant::now();
    assert_eq!(
      reg.mark_pending_leave("a", now, Duration::from_secs(20)),
      PendingLeaveMark::NewlyMarked
    );
    assert!(reg.take_due_leaves(now + Duration::from_secs(10)).is_empty());
    assert_eq!(reg.len(), 1);
    let due = reg.take_due_leaves(now + Duration::from_secs(21));
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].id, "a");
    assert!(reg.is_empty());
  }

  #[test]
  fn mark_pending_leave_second_call_is_already_pending_and_keeps_deadline() {
    let reg = DeviceRegistry::new();
    let _ = reg.appear(sample("a", "A"));
    let now = Instant::now();
    assert_eq!(
      reg.mark_pending_leave("a", now, Duration::from_secs(20)),
      PendingLeaveMark::NewlyMarked
    );
    let first_deadline = reg
      .get("a")
      .and_then(|d| d.pending_leave_deadline)
      .expect("deadline after first mark");
    let first_since = reg
      .get("a")
      .and_then(|d| d.pending_leave_since)
      .expect("since after first mark");

    // Later re-mark must not move the deadline or since.
    let later = now + Duration::from_secs(5);
    assert_eq!(
      reg.mark_pending_leave("a", later, Duration::from_secs(20)),
      PendingLeaveMark::AlreadyPending
    );
    let after = reg.get("a").expect("still present");
    assert_eq!(after.pending_leave_deadline, Some(first_deadline));
    assert_eq!(after.pending_leave_since, Some(first_since));
  }

  #[test]
  fn mark_pending_leave_unknown_id() {
    let reg = DeviceRegistry::new();
    assert_eq!(
      reg.mark_pending_leave("missing", Instant::now(), DEFAULT_PENDING_LEAVE),
      PendingLeaveMark::NotFound
    );
  }

  #[test]
  fn mark_pending_leave_by_instance_exact() {
    let reg = DeviceRegistry::new();
    let _ = reg.appear(sample_with_instance("abc123", "Gym", "Nest-Audio-abc123"));
    let now = Instant::now();
    // Substring must not match.
    assert!(
      reg
        .mark_pending_leave_by_instance("Nest-Audio", now, DEFAULT_PENDING_LEAVE)
        .is_none()
    );
    let (id, mark) = reg
      .mark_pending_leave_by_instance("Nest-Audio-abc123", now, DEFAULT_PENDING_LEAVE)
      .expect("exact instance");
    assert_eq!(id, "abc123");
    assert_eq!(mark, PendingLeaveMark::NewlyMarked);
    assert!(reg.is_pending_leave("abc123"));
    // Second mark is already pending (not newly marked).
    let (id2, mark2) = reg
      .mark_pending_leave_by_instance("Nest-Audio-abc123", now + Duration::from_secs(1), DEFAULT_PENDING_LEAVE)
      .expect("still present");
    assert_eq!(id2, "abc123");
    assert_eq!(mark2, PendingLeaveMark::AlreadyPending);
  }

  #[test]
  fn mark_pending_leave_by_id_exact() {
    let reg = DeviceRegistry::new();
    let _ = reg.appear(sample_with_instance("abc123", "Gym", "Nest-Audio-abc123"));
    let now = Instant::now();
    let (id, mark) = reg
      .mark_pending_leave_by_instance("abc123", now, DEFAULT_PENDING_LEAVE)
      .expect("id match");
    assert_eq!(id, "abc123");
    assert_eq!(mark, PendingLeaveMark::NewlyMarked);
  }

  #[test]
  fn match_device_for_leave_no_contains() {
    let devices = vec![
      sample_with_instance("deadbeef", "A", "Nest-Audio-deadbeef"),
      sample_with_instance("cafe", "B", "Other"),
    ];
    assert!(match_device_for_leave(&devices, "dead").is_none());
    assert_eq!(
      match_device_for_leave(&devices, "Nest-Audio-deadbeef").map(|d| d.id.as_str()),
      Some("deadbeef")
    );
    assert_eq!(match_device_for_leave(&devices, "cafe").map(|d| d.id.as_str()), Some("cafe"));
  }

  #[test]
  fn decide_airplay_withdraw_matrix() {
    // Still desired.
    assert_eq!(
      decide_airplay_withdraw(true, false, Duration::from_secs(100), Duration::ZERO),
      WithdrawDecision::Keep
    );
    // Live session always defers, even past any floor.
    assert_eq!(
      decide_airplay_withdraw(false, true, Duration::from_secs(100), SESSION_GUARD_GONE),
      WithdrawDecision::Defer
    );
    // Pure idle leave: min_gone = 0 → withdraw immediately.
    assert_eq!(
      decide_airplay_withdraw(false, false, Duration::ZERO, Duration::ZERO),
      WithdrawDecision::Withdraw
    );
    // Post-session floor: need full SESSION_GUARD_GONE after not-desired.
    assert_eq!(
      decide_airplay_withdraw(false, false, Duration::from_secs(30), SESSION_GUARD_GONE),
      WithdrawDecision::Defer
    );
    assert_eq!(
      decide_airplay_withdraw(false, false, SESSION_GUARD_GONE, SESSION_GUARD_GONE),
      WithdrawDecision::Withdraw
    );
  }

  #[test]
  fn pending_leave_is_due_ordering() {
    let t0 = Instant::now();
    let t1 = t0 + Duration::from_secs(20);
    assert!(!pending_leave_is_due(t1, t0));
    assert!(pending_leave_is_due(t0, t1));
    assert!(pending_leave_is_due(t0, t0));
  }

  #[test]
  fn cancel_pending_leave() {
    let reg = DeviceRegistry::new();
    let _ = reg.appear(sample("a", "A"));
    assert!(!reg.cancel_pending_leave("a"));
    assert_eq!(
      reg.mark_pending_leave("a", Instant::now(), DEFAULT_PENDING_LEAVE),
      PendingLeaveMark::NewlyMarked
    );
    assert!(reg.cancel_pending_leave("a"));
    assert!(!reg.is_pending_leave("a"));
  }

  #[test]
  fn list_present_excludes_pending() {
    let reg = DeviceRegistry::new();
    let _ = reg.appear(sample("a", "A"));
    let _ = reg.appear(sample("b", "B"));
    assert_eq!(
      reg.mark_pending_leave("a", Instant::now(), DEFAULT_PENDING_LEAVE),
      PendingLeaveMark::NewlyMarked
    );
    let present = reg.list_present();
    assert_eq!(present.len(), 1);
    assert_eq!(present[0].id, "b");
  }

  #[test]
  fn default_stale_ttl_is_ten_minutes() {
    assert_eq!(DEFAULT_STALE_TTL, Duration::from_secs(600));
  }
}
