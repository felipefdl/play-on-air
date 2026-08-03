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

/// Wall-clock bound for a single Cast `get_volume` seed attempt.
const VOLUME_SEED_TIMEOUT: Duration = Duration::from_secs(5);
/// Stop retrying volume seed after this many failures until the device re-appears.
const VOLUME_SEED_MAX_ATTEMPTS: u32 = 5;
/// Bound for joining a blocking `cast_pool.remove` / shutdown.
const POOL_BLOCKING_JOIN: Duration = Duration::from_secs(5);
/// Max maintain-task restarts per rolling window before giving up.
const MAINTAIN_RESTART_MAX: u32 = 5;
/// Rolling window for maintain-task restart budget.
const MAINTAIN_RESTART_WINDOW: Duration = Duration::from_secs(60);

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
    match maintain.await {
      Ok(()) => {},
      Err(err) if err.is_cancelled() => {
        tracing::debug!("maintain task cancelled during shutdown");
      },
      Err(err) => {
        tracing::error!(error = %err, "maintain task join error during shutdown");
      },
    }
    discovery_handle.abort();
    match bridge_task.await {
      Ok(()) => {},
      Err(err) if err.is_cancelled() => {
        tracing::info!("bridge task aborted on shutdown");
      },
      Err(err) => {
        tracing::error!(error = %err, "bridge task join error on shutdown");
      },
    }
    ownership_watch.abort();
    match ownership_watch.await {
      Ok(()) => {},
      Err(err) if err.is_cancelled() => {
        tracing::info!("ownership watch aborted on shutdown");
      },
      Err(err) => {
        tracing::error!(error = %err, "ownership watch join error on shutdown");
      },
    }
    // Final pool teardown on a blocking worker (join can take seconds).
    let pool_for_shutdown = Arc::clone(&cast_pool);
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
    Ok(())
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

fn spawn_maintain_loop(
  registry: Arc<DeviceRegistry>,
  config: Config,
  airplay: Arc<AirPlayManager>,
  cast_pool: Arc<CastPool>,
  bridge: Arc<Bridge>,
  mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
  tokio::spawn(async move {
    let volume_attempts: Mutex<HashMap<String, u32>> = Mutex::new(HashMap::new());
    let mut not_desired_since: HashMap<String, Instant> = HashMap::new();
    // Devices that deferred withdraw because a live session was active.
    // Only those require the SESSION_GUARD_GONE floor after the session ends.
    let mut session_blocked: HashSet<String> = HashSet::new();
    // Device ids observed last tick; absence → re-appear resets volume attempts.
    let mut known_ids: HashSet<String> = HashSet::new();

    loop {
      if *shutdown.borrow() {
        break;
      }
      if let Err(err) = maintain_airplay(
        &registry,
        &config,
        &airplay,
        &cast_pool,
        &bridge,
        &volume_attempts,
        &mut not_desired_since,
        &mut session_blocked,
        &mut known_ids,
      )
      .await
      {
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
#[expect(
  clippy::too_many_arguments,
  reason = "maintain state is explicit maps; keep flat rather than a god-struct for one loop"
)]
async fn maintain_airplay(
  registry: &DeviceRegistry,
  config: &Config,
  airplay: &AirPlayManager,
  cast_pool: &Arc<CastPool>,
  bridge: &Bridge,
  volume_attempts: &Mutex<HashMap<String, u32>>,
  not_desired_since: &mut HashMap<String, Instant>,
  session_blocked: &mut HashSet<String>,
  known_ids: &mut HashSet<String>,
) -> Result<()> {
  apply_due_leaves(registry, not_desired_since, known_ids);
  apply_stale_expiry(
    registry,
    airplay,
    cast_pool,
    bridge,
    not_desired_since,
    session_blocked,
    known_ids,
  )
  .await;

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
    // Device re-appeared after leave/expire → reset volume seed attempts.
    if !known_ids.contains(&device.id) {
      let _ = volume_attempts.lock().remove(&device.id);
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
  *known_ids = present_ids;

  log_new_advertisements(airplay, config, &devices, &previously_advertised);
  sync_volume_seeds(airplay, cast_pool, volume_attempts, &devices).await;
  withdraw_undesired(airplay, cast_pool, bridge, &desired, not_desired_since, session_blocked).await;
  drop_orphan_cast_workers(cast_pool, bridge, airplay, &desired).await;

  Ok(())
}

fn apply_due_leaves(
  registry: &DeviceRegistry,
  not_desired_since: &mut HashMap<String, Instant>,
  known_ids: &mut HashSet<String>,
) {
  let due = registry.take_due_leaves(Instant::now());
  for dev in &due {
    tracing::info!(
      id = %dev.id,
      name = %dev.name,
      instance = %dev.instance,
      "Chromecast left"
    );
    let since = dev.pending_leave_since.unwrap_or_else(Instant::now);
    let _ = not_desired_since.insert(dev.id.clone(), since);
    let _ = known_ids.remove(&dev.id);
  }
}

#[expect(
  clippy::too_many_arguments,
  reason = "stale-expiry needs registry + ad/pool + session bookkeeping maps"
)]
async fn apply_stale_expiry(
  registry: &DeviceRegistry,
  airplay: &AirPlayManager,
  cast_pool: &Arc<CastPool>,
  bridge: &Bridge,
  not_desired_since: &mut HashMap<String, Instant>,
  session_blocked: &mut HashSet<String>,
  known_ids: &mut HashSet<String>,
) {
  let expired = registry.expire_stale(DEFAULT_STALE_TTL);
  for dev in &expired {
    tracing::info!(id = %dev.id, name = %dev.name, "expired stale Chromecast");
    // Stale expiry: no session → withdraw immediately (min_gone effectively already met).
    let ancient = Instant::now().checked_sub(SESSION_GUARD_GONE).unwrap_or_else(Instant::now);
    let _ = not_desired_since.insert(dev.id.clone(), ancient);
    let _ = known_ids.remove(&dev.id);
    if bridge.has_session(&dev.id) {
      // Live session during silent disappear: require the post-session floor later.
      let _ = session_blocked.insert(dev.id.clone());
    } else {
      airplay.remove(&dev.id);
      remove_cast_worker(cast_pool, &dev.id).await;
      let _ = session_blocked.remove(&dev.id);
      let _ = not_desired_since.remove(&dev.id);
    }
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

/// Withdraw receivers that are no longer desired.
///
/// Idle leave: withdraw immediately when `!has_session`.
/// Session-blocked leave: require `gone_for >= SESSION_GUARD_GONE` after `not_desired_since`.
async fn withdraw_undesired(
  airplay: &AirPlayManager,
  cast_pool: &Arc<CastPool>,
  bridge: &Bridge,
  desired: &HashSet<String>,
  not_desired_since: &mut HashMap<String, Instant>,
  session_blocked: &mut HashSet<String>,
) {
  let active_ids = airplay.active_ids();
  for active in &active_ids {
    if desired.contains(active) {
      let _ = not_desired_since.remove(active);
      let _ = session_blocked.remove(active);
      continue;
    }
    let since = *not_desired_since.entry(active.clone()).or_insert_with(Instant::now);
    let gone_for = Instant::now().saturating_duration_since(since);
    let has_session = bridge.has_session(active);
    if has_session {
      let _ = session_blocked.insert(active.clone());
    }
    let min_gone = if session_blocked.contains(active) {
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
          session_blocked = session_blocked.contains(active),
          gone_secs = gone_for.as_secs(),
          min_gone_secs = min_gone.as_secs(),
          "deferring AirPlay withdraw"
        );
      },
      WithdrawDecision::Withdraw => {
        // airplay.remove logs the info-level withdraw; keep maintain at debug to avoid double info.
        tracing::debug!(id = %active, "withdrawing AirPlay receiver (device no longer desired)");
        airplay.remove(active);
        remove_cast_worker(cast_pool, active).await;
        let _ = not_desired_since.remove(active);
        let _ = session_blocked.remove(active);
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

/// Concurrent volume seeds with attempt cap and per-call timeout.
async fn sync_volume_seeds(
  airplay: &AirPlayManager,
  cast_pool: &Arc<CastPool>,
  volume_attempts: &Mutex<HashMap<String, u32>>,
  devices: &[Device],
) {
  let mut ids = Vec::new();
  for device in devices {
    if !airplay.needs_volume_seed(&device.id) {
      continue;
    }
    let attempts = volume_attempts.lock().get(&device.id).copied().unwrap_or(0);
    if attempts >= VOLUME_SEED_MAX_ATTEMPTS {
      tracing::debug!(
        id = %device.id,
        attempts,
        "volume seed attempts exhausted until re-appear"
      );
      continue;
    }
    ids.push(device.id.clone());
  }
  if ids.is_empty() {
    return;
  }

  let mut join_set = tokio::task::JoinSet::new();
  for id in ids {
    let pool = Arc::clone(cast_pool);
    let device_id = id.clone();
    drop(join_set.spawn(async move {
      let result = tokio::time::timeout(
        VOLUME_SEED_TIMEOUT,
        tokio::task::spawn_blocking(move || pool.get_volume(&device_id)),
      )
      .await;
      (id, result)
    }));
  }

  while let Some(joined) = join_set.join_next().await {
    let Ok((device_id, result)) = joined else {
      continue;
    };
    if let Ok(Ok(Ok(linear))) = result {
      let _ = volume_attempts.lock().remove(&device_id);
      let db = cast_linear_to_airplay_db(linear);
      airplay.set_reported_volume_db(&device_id, db);
      tracing::info!(
        %device_id,
        cast_linear = linear,
        airplay_db = db,
        "synced AirPlay reported volume from Cast"
      );
      continue;
    }
    let attempts = {
      let mut guard = volume_attempts.lock();
      let entry = guard.entry(device_id.clone()).or_insert(0);
      *entry = entry.saturating_add(1);
      let n = *entry;
      drop(guard);
      n
    };
    tracing::debug!(
      %device_id,
      attempts,
      "volume seed attempt failed"
    );
  }
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
}
