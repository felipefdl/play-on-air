//! Application main loop: discovery, AirPlay ads, bridge, and shutdown.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{mpsc, watch};
use tokio::time::sleep;

use crate::airplay::{AirPlayManager, AirPlaySessionEvent, cast_linear_to_airplay_db};
use crate::bridge::Bridge;
use crate::cast::CastPool;
use crate::config::Config;
use crate::discover::Discovery;
use crate::error::Result;
use crate::names::{airplay_name_with_id, is_hidden};
use crate::registry::{
  DEFAULT_STALE_TTL, Device, DeviceRegistry, SESSION_GUARD_GONE, WithdrawDecision, decide_airplay_withdraw,
};

/// Wall-clock bound for a single Cast `get_volume` seed attempt (attempt accounting).
const VOLUME_SEED_TIMEOUT: Duration = Duration::from_secs(5);
/// Stop retrying volume seed after this many failures until the device re-appears.
const VOLUME_SEED_MAX_ATTEMPTS: u32 = 5;
/// Bound for joining a blocking `cast_pool.remove` / shutdown / task abort join.
const POOL_BLOCKING_JOIN: Duration = Duration::from_secs(5);
/// Max maintain-task restarts per rolling window before giving up.
const MAINTAIN_RESTART_MAX: u32 = 5;
/// Rolling window for maintain-task restart budget.
const MAINTAIN_RESTART_WINDOW: Duration = Duration::from_secs(60);

/// Session-guard and volume-seed bookkeeping shared across maintain respawns.
///
/// Lives in the supervisor so a `JoinError` restart does not wipe the 60s post-session
/// floor or volume attempt counters.
#[derive(Debug, Default)]
struct MaintainGuards {
  volume_attempts: Mutex<HashMap<String, u32>>,
  not_desired_since: Mutex<HashMap<String, Instant>>,
  session_blocked: Mutex<HashSet<String>>,
  known_ids: Mutex<HashSet<String>>,
  /// Device ids with an outstanding `spawn_blocking` volume seed (not merely timed-out await).
  volume_seed_inflight: Mutex<HashSet<String>>,
}

/// Top-level runtime owning shared state.
#[derive(Debug)]
pub struct App {
  config: Config,
  registry: Arc<DeviceRegistry>,
}

impl App {
  /// Create the app with loaded (or default) config.
  pub fn new(config: Config) -> Self {
    Self {
      config,
      registry: Arc::new(DeviceRegistry::new()),
    }
  }

  /// Shared device registry.
  pub fn registry(&self) -> Arc<DeviceRegistry> {
    Arc::clone(&self.registry)
  }

  /// Run until `shutdown` is set true (SIGINT / SIGTERM).
  pub async fn run(self, shutdown: watch::Receiver<bool>) -> Result<()> {
    let registry = Arc::clone(&self.registry);
    let config = self.config;

    // Bind AP2 PTP (319/320) once before any RaopServer starts.
    crate::net::ensure_global_ptp_sink();
    // Give the sink thread a beat to bind before AirPlay ads race it.
    sleep(Duration::from_millis(50)).await;

    let (event_tx, event_rx) = mpsc::unbounded_channel::<AirPlaySessionEvent>();
    let (ownership_tx, mut ownership_rx) = mpsc::unbounded_channel::<String>();
    let airplay = Arc::new(AirPlayManager::new(Some(event_tx)));
    let cast_pool = Arc::new(CastPool::new(Some(ownership_tx)));
    let bridge =
      Arc::new(Bridge::new(Arc::clone(&registry), Arc::clone(&cast_pool)).with_airplay(Arc::clone(&airplay)));

    let discovery = Discovery::new(Arc::clone(&registry));
    let discovery_handle = discovery.spawn(shutdown.clone());

    let bridge_rings = coerce_rings(Arc::clone(&airplay));
    let bridge_task = {
      let bridge_for_task = Arc::clone(&bridge);
      tokio::spawn(async move {
        bridge_for_task.run(event_rx, bridge_rings).await;
      })
    };

    // Cast steal: worker confirmed another app took the receiver → end bridge + kick AP2.
    let ownership_bridge = Arc::clone(&bridge);
    let ownership_airplay = Arc::clone(&airplay);
    let ownership_watch = tokio::spawn(async move {
      while let Some(device_id) = ownership_rx.recv().await {
        tracing::info!(%device_id, "Cast ownership lost; ending bridge and kicking AirPlay clients");
        ownership_bridge.end_session(&device_id).await;
        if let Err(err) = ownership_airplay.kick_clients(&device_id).await {
          tracing::warn!(
            %device_id,
            error = %err,
            "failed to re-advertise AirPlay after ownership-loss kick"
          );
        }
      }
    });

    let maintain = spawn_supervised_maintain(
      Arc::clone(&registry),
      config,
      Arc::clone(&airplay),
      Arc::clone(&cast_pool),
      Arc::clone(&bridge),
      shutdown.clone(),
    );

    // Wait for shutdown signal (already driven by caller updating the watch).
    let mut wait = shutdown.clone();
    while !*wait.borrow() {
      if wait.changed().await.is_err() {
        break;
      }
    }

    tracing::info!("shutting down PlayOnAir");
    join_task_result(maintain.await, "maintain", true);
    discovery_handle.abort();
    // Abort before await: `Bridge::run` only exits when all `event_tx` clones drop, and
    // airplay / bridge / ownership still hold senders here. Without abort, the first
    // signal never reaches `cast_pool.shutdown`.
    bridge_task.abort();
    await_aborted_task(bridge_task, "bridge").await;
    ownership_watch.abort();
    await_aborted_task(ownership_watch, "ownership watch").await;
    shutdown_cast_pool(&cast_pool).await;
    Ok(())
  }
}

fn join_task_result(result: std::result::Result<(), tokio::task::JoinError>, label: &str, on_shutdown: bool) {
  match result {
    Ok(()) => {},
    Err(err) if err.is_cancelled() => {
      if on_shutdown {
        tracing::debug!(task = label, "task cancelled during shutdown");
      } else {
        tracing::info!(task = label, "task aborted on shutdown");
      }
    },
    Err(err) => {
      tracing::error!(task = label, error = %err, "task join error on shutdown");
    },
  }
}

async fn await_aborted_task(handle: tokio::task::JoinHandle<()>, label: &str) {
  if let Ok(result) = tokio::time::timeout(POOL_BLOCKING_JOIN, handle).await {
    join_task_result(result, label, false);
  } else {
    tracing::warn!(task = label, "task join timed out on shutdown");
  }
}

async fn shutdown_cast_pool(cast_pool: &Arc<CastPool>) {
  let pool_for_shutdown = Arc::clone(cast_pool);
  match tokio::time::timeout(
    POOL_BLOCKING_JOIN,
    tokio::task::spawn_blocking(move || {
      pool_for_shutdown.shutdown();
    }),
  )
  .await
  {
    Ok(Ok(())) => {},
    Ok(Err(err)) => tracing::warn!(error = %err, "cast pool shutdown join error"),
    Err(_) => tracing::warn!("cast pool shutdown timed out"),
  }
}

fn coerce_rings(manager: Arc<AirPlayManager>) -> Arc<dyn crate::bridge::RingLookup> {
  manager
}

/// Spawn the maintain loop with bounded restart on panic / join error.
fn spawn_supervised_maintain(
  registry: Arc<DeviceRegistry>,
  config: Config,
  airplay: Arc<AirPlayManager>,
  cast_pool: Arc<CastPool>,
  bridge: Arc<Bridge>,
  shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
  tokio::spawn(async move {
    // Shared across respawns so session-guard floors and volume attempts survive JoinError.
    let guards = Arc::new(MaintainGuards::default());
    let mut restart_times: Vec<Instant> = Vec::new();
    loop {
      if *shutdown.borrow() {
        break;
      }
      let child = spawn_maintain_loop(
        Arc::clone(&registry),
        config.clone(),
        Arc::clone(&airplay),
        Arc::clone(&cast_pool),
        Arc::clone(&bridge),
        Arc::clone(&guards),
        shutdown.clone(),
      );
      match child.await {
        Ok(()) => break,
        Err(err) => {
          tracing::error!(error = %err, "maintain task ended with join error");
          if *shutdown.borrow() {
            break;
          }
          let now = Instant::now();
          restart_times.retain(|t| now.duration_since(*t) < MAINTAIN_RESTART_WINDOW);
          if restart_times.len() >= MAINTAIN_RESTART_MAX as usize {
            tracing::error!(
              max = MAINTAIN_RESTART_MAX,
              window_secs = MAINTAIN_RESTART_WINDOW.as_secs(),
              "maintain task exceeded restart budget; giving up (AirPlay ads may be stale)"
            );
            break;
          }
          restart_times.push(now);
          tracing::warn!(restarts_in_window = restart_times.len(), "respawning maintain task");
          sleep(Duration::from_millis(200)).await;
        },
      }
    }
  })
}

#[expect(
  clippy::too_many_arguments,
  reason = "child loop needs registry, config, airplay, pool, bridge, shared guards, shutdown"
)]
fn spawn_maintain_loop(
  registry: Arc<DeviceRegistry>,
  config: Config,
  airplay: Arc<AirPlayManager>,
  cast_pool: Arc<CastPool>,
  bridge: Arc<Bridge>,
  guards: Arc<MaintainGuards>,
  mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
  tokio::spawn(async move {
    loop {
      if *shutdown.borrow() {
        break;
      }
      if let Err(err) = maintain_airplay(&registry, &config, &airplay, &cast_pool, &bridge, &guards).await {
        tracing::error!(error = %err, "AirPlay maintain loop error");
      }
      tokio::select! {
        () = sleep(Duration::from_secs(2)) => {}
        changed = shutdown.changed() => {
          if changed.is_err() || *shutdown.borrow() {
            break;
          }
        }
      }
    }
    // Withdraw AirPlay ads on shutdown. Cast pool teardown is owned by `App::run`
    // (single spawn_blocking shutdown — avoid double-shutdown races with the bridge).
    for id in airplay.active_ids() {
      airplay.remove(&id);
    }
  })
}

/// Reconcile AirPlay advertisements and warm Cast workers with the registry/config.
async fn maintain_airplay(
  registry: &DeviceRegistry,
  config: &Config,
  airplay: &Arc<AirPlayManager>,
  cast_pool: &Arc<CastPool>,
  bridge: &Bridge,
  guards: &Arc<MaintainGuards>,
) -> Result<()> {
  apply_due_leaves(registry, guards);
  apply_stale_expiry(registry, airplay, cast_pool, bridge, guards).await;

  // Pending-leave cancel (re-sight while still registered) resets volume seed attempts.
  for id in registry.drain_pending_leave_cancellations() {
    let _ = guards.volume_attempts.lock().remove(&id);
    tracing::debug!(%id, "reset volume seed attempts after pending-leave cancel");
  }

  let devices = registry.list();
  let mut desired: HashSet<String> = HashSet::new();
  let mut present_ids: HashSet<String> = HashSet::new();
  // Snapshot before ensure so we can log advertise transitions once.
  let previously_advertised: HashSet<String> = airplay.active_ids().into_iter().collect();

  for device in &devices {
    let _ = present_ids.insert(device.id.clone());
    if is_hidden(&device.name, &device.id, config) {
      continue;
    }
    // Device re-appeared after full leave/expire → reset volume seed attempts.
    if !guards.known_ids.lock().contains(&device.id) {
      let _ = guards.volume_attempts.lock().remove(&device.id);
    }

    let _inserted = desired.insert(device.id.clone());
    let name = airplay_name_with_id(&device.name, &device.id, config);
    if let Err(err) = airplay.ensure(&device.id, &name).await {
      tracing::error!(
        id = %device.id,
        airplay_name = %name,
        error = %err,
        "failed to advertise AirPlay 2 receiver"
      );
    }
    // Warm Cast TCP while idle so LOAD during AirPlay does not dial fresh.
    cast_pool.ensure(device);
  }
  *guards.known_ids.lock() = present_ids;

  log_new_advertisements(airplay, config, &devices, &previously_advertised);
  sync_volume_seeds(airplay, cast_pool, guards, &devices);
  withdraw_undesired(registry, airplay, cast_pool, bridge, &desired, guards).await;
  drop_orphan_cast_workers(cast_pool, bridge, airplay, &desired).await;

  Ok(())
}

fn apply_due_leaves(registry: &DeviceRegistry, guards: &MaintainGuards) {
  let due = registry.take_due_leaves(Instant::now());
  for dev in &due {
    tracing::info!(
      id = %dev.id,
      name = %dev.name,
      instance = %dev.instance,
      "Chromecast left"
    );
    let since = dev.pending_leave_since.unwrap_or_else(Instant::now);
    let _ = guards.not_desired_since.lock().insert(dev.id.clone(), since);
    let _ = guards.known_ids.lock().remove(&dev.id);
  }
}

async fn apply_stale_expiry(
  registry: &DeviceRegistry,
  airplay: &AirPlayManager,
  cast_pool: &Arc<CastPool>,
  bridge: &Bridge,
  guards: &MaintainGuards,
) {
  let expired = registry.expire_stale(DEFAULT_STALE_TTL);
  for dev in &expired {
    tracing::info!(id = %dev.id, name = %dev.name, "expired stale Chromecast");
    // Stale expiry: no session → withdraw immediately (min_gone effectively already met).
    let ancient = Instant::now().checked_sub(SESSION_GUARD_GONE).unwrap_or_else(Instant::now);
    let _ = guards.not_desired_since.lock().insert(dev.id.clone(), ancient);
    let _ = guards.known_ids.lock().remove(&dev.id);
    if bridge.has_session(&dev.id) {
      // Live session during silent disappear: require the post-session floor later.
      let _ = guards.session_blocked.lock().insert(dev.id.clone());
      continue;
    }
    // Re-check immediately before remove (same TOCTOU window as withdraw_undesired).
    // Residual race: session can still insert between this re-check and airplay.remove
    // without a shared lock with bridge session insert; full atomicity needs airplay/bridge
    // coordination (out of territory).
    if !may_withdraw_after_session_recheck(bridge.has_session(&dev.id)) {
      let _ = guards.session_blocked.lock().insert(dev.id.clone());
      tracing::debug!(id = %dev.id, "deferring stale withdraw (session present on re-check)");
      continue;
    }
    airplay.remove(&dev.id);
    remove_cast_worker(cast_pool, &dev.id).await;
    let _ = guards.session_blocked.lock().remove(&dev.id);
    let _ = guards.not_desired_since.lock().remove(&dev.id);
  }
}

fn log_new_advertisements(
  airplay: &AirPlayManager,
  config: &Config,
  devices: &[Device],
  previously_advertised: &HashSet<String>,
) {
  for id in airplay.active_ids() {
    if previously_advertised.contains(&id) {
      continue;
    }
    let name = devices
      .iter()
      .find(|d| d.id == id)
      .map_or_else(|| id.clone(), |d| airplay_name_with_id(&d.name, &d.id, config));
    tracing::info!(%id, airplay_name = %name, "AirPlay receiver advertised");
  }
}

/// True when the active receiver should stay advertised at withdraw time.
///
/// Re-validates beyond the tick-start `desired` snapshot so a mid-tick re-appear
/// after `take_due_leaves` does not flap the AirPlay ad.
#[must_use]
const fn is_desired_at_withdraw(in_desired_snapshot: bool, registry_has_device: bool) -> bool {
  in_desired_snapshot || registry_has_device
}

/// After decide `Withdraw`, re-check live session immediately before remove.
///
/// Returns `true` when withdraw may proceed. Residual race: a session can still
/// insert between this re-check and `airplay.remove` without a shared lock with
/// bridge session insert; full atomicity needs airplay/bridge coordination.
#[must_use]
const fn may_withdraw_after_session_recheck(has_session_now: bool) -> bool {
  !has_session_now
}

/// Whether to start a new volume-seed blocking task for this device.
#[must_use]
const fn should_start_volume_seed(needs_seed: bool, attempts: u32, inflight: bool) -> bool {
  needs_seed && !inflight && attempts < VOLUME_SEED_MAX_ATTEMPTS
}

/// Withdraw receivers that are no longer desired.
///
/// Idle leave: withdraw immediately when `!has_session`.
/// Session-blocked leave: require `gone_for >= SESSION_GUARD_GONE` after `not_desired_since`.
async fn withdraw_undesired(
  registry: &DeviceRegistry,
  airplay: &AirPlayManager,
  cast_pool: &Arc<CastPool>,
  bridge: &Bridge,
  desired: &HashSet<String>,
  guards: &MaintainGuards,
) {
  let active_ids = airplay.active_ids();
  for active in &active_ids {
    let registry_has = registry.get(active).is_some();
    if is_desired_at_withdraw(desired.contains(active), registry_has) {
      let _ = guards.not_desired_since.lock().remove(active);
      let _ = guards.session_blocked.lock().remove(active);
      continue;
    }
    let since = {
      let mut nds = guards.not_desired_since.lock();
      *nds.entry(active.clone()).or_insert_with(Instant::now)
    };
    let gone_for = Instant::now().saturating_duration_since(since);
    let has_session = bridge.has_session(active);
    if has_session {
      let _ = guards.session_blocked.lock().insert(active.clone());
    }
    let min_gone = if guards.session_blocked.lock().contains(active) {
      SESSION_GUARD_GONE
    } else {
      Duration::ZERO
    };
    match decide_airplay_withdraw(false, has_session, gone_for, min_gone) {
      WithdrawDecision::Keep => {},
      WithdrawDecision::Defer => {
        tracing::debug!(
          id = %active,
          has_session,
          session_blocked = guards.session_blocked.lock().contains(active),
          gone_secs = gone_for.as_secs(),
          min_gone_secs = min_gone.as_secs(),
          "deferring AirPlay withdraw"
        );
      },
      WithdrawDecision::Withdraw => {
        // Re-check immediately before remove.
        // Residual race: session can still insert between this re-check and
        // airplay.remove without a shared lock with bridge session insert;
        // full atomicity needs airplay/bridge coordination (out of territory).
        if !may_withdraw_after_session_recheck(bridge.has_session(active)) {
          let _ = guards.session_blocked.lock().insert(active.clone());
          tracing::debug!(
            id = %active,
            "deferring AirPlay withdraw (session present on re-check)"
          );
          continue;
        }
        // airplay.remove logs the info-level withdraw; keep maintain at debug to avoid double info.
        tracing::debug!(id = %active, "withdrawing AirPlay receiver (device no longer desired)");
        airplay.remove(active);
        remove_cast_worker(cast_pool, active).await;
        let _ = guards.not_desired_since.lock().remove(active);
        let _ = guards.session_blocked.lock().remove(active);
      },
    }
  }
}

async fn drop_orphan_cast_workers(
  cast_pool: &Arc<CastPool>,
  bridge: &Bridge,
  airplay: &AirPlayManager,
  desired: &HashSet<String>,
) {
  let still_active: HashSet<String> = airplay.active_ids().into_iter().collect();
  for id in cast_pool.device_ids() {
    if desired.contains(&id) || bridge.has_session(&id) || still_active.contains(&id) {
      continue;
    }
    remove_cast_worker(cast_pool, &id).await;
  }
}

/// Concurrent volume seeds with attempt cap, inflight guard, and non-blocking maintain.
///
/// Spawns background work so the maintain tick does not wait on Cast `get_volume`.
/// Does not start a second `spawn_blocking` while a prior attempt for the same id is
/// still outstanding (including after the attempt-accounting timeout).
fn sync_volume_seeds(
  airplay: &Arc<AirPlayManager>,
  cast_pool: &Arc<CastPool>,
  guards: &Arc<MaintainGuards>,
  devices: &[Device],
) {
  let mut to_spawn: Vec<String> = Vec::new();
  for device in devices {
    if !airplay.needs_volume_seed(&device.id) {
      continue;
    }
    let attempts = guards.volume_attempts.lock().get(&device.id).copied().unwrap_or(0);
    let inflight = guards.volume_seed_inflight.lock().contains(&device.id);
    if !should_start_volume_seed(true, attempts, inflight) {
      if attempts >= VOLUME_SEED_MAX_ATTEMPTS {
        tracing::debug!(
          id = %device.id,
          attempts,
          "volume seed attempts exhausted until re-appear"
        );
      }
      continue;
    }
    // Mark inflight before spawn so a concurrent tick cannot stack another seed.
    if !guards.volume_seed_inflight.lock().insert(device.id.clone()) {
      continue;
    }
    to_spawn.push(device.id.clone());
  }

  for id in to_spawn {
    let pool = Arc::clone(cast_pool);
    let airplay_bg = Arc::clone(airplay);
    let guards_bg = Arc::clone(guards);
    drop(tokio::spawn(async move {
      let seed_id = id.clone();
      let mut handle = tokio::task::spawn_blocking(move || pool.get_volume(&id));
      let outcome = tokio::select! {
        res = &mut handle => Some(res),
        () = sleep(VOLUME_SEED_TIMEOUT) => None,
      };
      match outcome {
        Some(Ok(Ok(linear))) => {
          apply_volume_seed_success(&airplay_bg, &guards_bg, &seed_id, linear);
        },
        Some(Ok(Err(_)) | Err(_)) => {
          bump_volume_seed_attempt(&guards_bg, &seed_id);
        },
        None => {
          // Attempt-accounting timeout: count a failure, but keep inflight until the
          // blocking `get_volume` actually finishes so we do not stack another seed.
          bump_volume_seed_attempt(&guards_bg, &seed_id);
          if let Ok(Ok(linear)) = handle.await {
            apply_volume_seed_success(&airplay_bg, &guards_bg, &seed_id, linear);
          }
        },
      }
      let _ = guards_bg.volume_seed_inflight.lock().remove(&seed_id);
    }));
  }
}

fn apply_volume_seed_success(airplay: &AirPlayManager, guards: &MaintainGuards, device_id: &str, linear: f32) {
  let _ = guards.volume_attempts.lock().remove(device_id);
  let db = cast_linear_to_airplay_db(linear);
  airplay.set_reported_volume_db(device_id, db);
  tracing::info!(
    %device_id,
    cast_linear = linear,
    airplay_db = db,
    "synced AirPlay reported volume from Cast"
  );
}

fn bump_volume_seed_attempt(guards: &MaintainGuards, device_id: &str) {
  let attempts = {
    let mut map = guards.volume_attempts.lock();
    let entry = map.entry(device_id.to_owned()).or_insert(0);
    *entry = entry.saturating_add(1);
    let n = *entry;
    drop(map);
    n
  };
  tracing::debug!(
    %device_id,
    attempts,
    "volume seed attempt failed"
  );
}

/// Run `cast_pool.remove` off the async runtime with a join bound.
async fn remove_cast_worker(cast_pool: &Arc<CastPool>, device_id: &str) {
  let pool = Arc::clone(cast_pool);
  let id = device_id.to_owned();
  match tokio::time::timeout(
    POOL_BLOCKING_JOIN,
    tokio::task::spawn_blocking(move || {
      pool.remove(&id);
    }),
  )
  .await
  {
    Ok(Ok(())) => {},
    Ok(Err(err)) => tracing::warn!(device_id, error = %err, "cast_pool.remove join error"),
    Err(_) => tracing::warn!(device_id, "cast_pool.remove timed out"),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn withdraw_decision_defers_live_session() {
    assert_eq!(
      decide_airplay_withdraw(false, true, Duration::from_secs(600), SESSION_GUARD_GONE),
      WithdrawDecision::Defer
    );
  }

  #[test]
  fn withdraw_decision_idle_leave_immediate() {
    // Pure idle leave: min_gone = 0 → withdraw without the 60s floor.
    assert_eq!(
      decide_airplay_withdraw(false, false, Duration::ZERO, Duration::ZERO),
      WithdrawDecision::Withdraw
    );
  }

  #[test]
  fn withdraw_decision_after_session_guard_floor() {
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
  fn session_recheck_defers_when_session_present() {
    // Would-withdraw path: re-check sees session → do not remove.
    assert!(!may_withdraw_after_session_recheck(true));
    assert!(may_withdraw_after_session_recheck(false));
  }

  #[test]
  fn registry_revalidate_treats_reappeared_as_desired() {
    // Tick-start snapshot missed the device, but registry has it again mid-tick.
    assert!(is_desired_at_withdraw(false, true));
    assert!(is_desired_at_withdraw(true, false));
    assert!(!is_desired_at_withdraw(false, false));
  }

  #[test]
  fn volume_seed_skips_while_inflight() {
    assert!(should_start_volume_seed(true, 0, false));
    assert!(!should_start_volume_seed(true, 0, true));
    assert!(!should_start_volume_seed(true, VOLUME_SEED_MAX_ATTEMPTS, false));
    assert!(!should_start_volume_seed(false, 0, false));
  }

  #[test]
  fn maintain_guards_default_is_empty_shared_state() {
    // Structural: supervisor constructs one MaintainGuards (via Default) and clones Arc into
    // each child; maps live outside the child so respawn continues bookkeeping.
    let guards = Arc::new(MaintainGuards::default());
    let child_view = Arc::clone(&guards);
    let _ = child_view.session_blocked.lock().insert("dev-a".to_owned());
    let _ = child_view.not_desired_since.lock().insert("dev-a".to_owned(), Instant::now());
    let _ = child_view.volume_attempts.lock().insert("dev-a".to_owned(), 3);
    // "Respawn" only clones Arc — same maps.
    let after_respawn = Arc::clone(&guards);
    assert!(after_respawn.session_blocked.lock().contains("dev-a"));
    assert!(after_respawn.not_desired_since.lock().contains_key("dev-a"));
    assert_eq!(after_respawn.volume_attempts.lock().get("dev-a").copied(), Some(3));
  }

  #[test]
  fn pending_leave_cancel_resets_volume_attempts_logic() {
    let reg = DeviceRegistry::new();
    assert!(reg.appear(Device::new("a", "A", "192.168.1.10", "a.local", 8009, "A")));
    let now = Instant::now();
    assert_eq!(
      reg.mark_pending_leave("a", now, crate::registry::DEFAULT_PENDING_LEAVE),
      crate::registry::PendingLeaveMark::NewlyMarked
    );
    assert!(!reg.appear(Device::new("a", "A", "192.168.1.10", "a.local", 8009, "A")));
    let cancelled = reg.drain_pending_leave_cancellations();
    assert_eq!(cancelled, vec!["a".to_owned()]);

    let guards = MaintainGuards::default();
    let _ = guards.volume_attempts.lock().insert("a".to_owned(), VOLUME_SEED_MAX_ATTEMPTS);
    for id in cancelled {
      let _ = guards.volume_attempts.lock().remove(&id);
    }
    assert!(!guards.volume_attempts.lock().contains_key("a"));
    assert!(reg.drain_pending_leave_cancellations().is_empty());
  }

  #[tokio::test]
  async fn app_run_returns_after_one_shutdown_signal() {
    let app = App::new(Config::default());
    let (tx, rx) = watch::channel(false);
    let signal = tokio::spawn(async move {
      // Let App::run pass the PTP settle sleep and spawn tasks.
      sleep(Duration::from_millis(80)).await;
      tx.send(true).expect("shutdown watch open");
    });
    let outcome = tokio::time::timeout(Duration::from_secs(15), app.run(rx)).await;
    signal.await.expect("signal task");
    assert!(outcome.is_ok(), "App::run timed out — shutdown hang regression?");
    assert!(outcome.expect("timeout").is_ok(), "App::run returned Err");
  }

  #[tokio::test]
  async fn volume_seed_inflight_set_clears_after_task() {
    // Pure bookkeeping: insert inflight, spawn a short task that clears it, wait.
    let guards = Arc::new(MaintainGuards::default());
    assert!(guards.volume_seed_inflight.lock().insert("x".to_owned()));
    assert!(!should_start_volume_seed(
      true,
      0,
      guards.volume_seed_inflight.lock().contains("x")
    ));

    let g = Arc::clone(&guards);
    let join = tokio::spawn(async move {
      sleep(Duration::from_millis(20)).await;
      let _ = g.volume_seed_inflight.lock().remove("x");
    });
    // While outstanding, second seed is skipped.
    assert!(!should_start_volume_seed(
      true,
      0,
      guards.volume_seed_inflight.lock().contains("x")
    ));
    join.await.expect("join");
    assert!(should_start_volume_seed(
      true,
      0,
      guards.volume_seed_inflight.lock().contains("x")
    ));
  }
}
