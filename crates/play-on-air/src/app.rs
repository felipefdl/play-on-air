//! Application main loop: discovery, AirPlay ads, bridge, and shutdown.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::time::sleep;

use crate::airplay::{AirPlayManager, AirPlaySessionEvent};
use crate::bridge::Bridge;
use crate::config::Config;
use crate::discover::Discovery;
use crate::error::Result;
use crate::names::{airplay_name_with_id, is_hidden};
use crate::registry::{DEFAULT_STALE_TTL, DeviceRegistry};

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

    let (event_tx, event_rx) = mpsc::unbounded_channel::<AirPlaySessionEvent>();
    let airplay = Arc::new(AirPlayManager::new(Some(event_tx)));
    let bridge = Arc::new(Bridge::new(Arc::clone(&registry)));

    let discovery = Discovery::new(Arc::clone(&registry));
    let discovery_handle = discovery.spawn(shutdown.clone());

    let bridge_rings = coerce_rings(Arc::clone(&airplay));
    let bridge_task = {
      let bridge_for_task = Arc::clone(&bridge);
      tokio::spawn(async move {
        bridge_for_task.run(event_rx, bridge_rings).await;
      })
    };

    let mut maintain_shutdown = shutdown.clone();
    let maintain = tokio::spawn(async move {
      loop {
        if *maintain_shutdown.borrow() {
          break;
        }
        if let Err(err) = maintain_airplay(&registry, &config, &airplay).await {
          tracing::error!(error = %err, "AirPlay maintain loop error");
        }
        tokio::select! {
          () = sleep(Duration::from_secs(2)) => {}
          _ = maintain_shutdown.changed() => {
            if *maintain_shutdown.borrow() {
              break;
            }
          }
        }
      }
      // Withdraw all AirPlay ads on shutdown.
      for id in airplay.active_ids() {
        airplay.remove(&id);
      }
    });

    // Wait for shutdown signal (already driven by caller updating the watch).
    let mut wait = shutdown.clone();
    while !*wait.borrow() {
      if wait.changed().await.is_err() {
        break;
      }
    }

    tracing::info!("shutting down PlayOnAir");
    drop(maintain.await);
    discovery_handle.abort();
    bridge_task.abort();
    Ok(())
  }
}

fn coerce_rings(manager: Arc<AirPlayManager>) -> Arc<dyn crate::bridge::RingLookup> {
  manager
}

/// Reconcile AirPlay advertisements with the current registry and config.
async fn maintain_airplay(registry: &DeviceRegistry, config: &Config, airplay: &AirPlayManager) -> Result<()> {
  // Drop devices that never received ServiceRemoved but stopped advertising.
  let expired = registry.expire_stale(DEFAULT_STALE_TTL);
  for dev in &expired {
    tracing::info!(id = %dev.id, name = %dev.name, "expired stale Chromecast");
    airplay.remove(&dev.id);
  }

  let devices = registry.list();
  let mut desired: HashSet<String> = HashSet::new();

  for device in &devices {
    if is_hidden(&device.name, &device.id, config) {
      continue;
    }
    let name = airplay_name_with_id(&device.name, &device.id, config);
    let _inserted = desired.insert(device.id.clone());
    if let Err(err) = airplay.ensure(&device.id, &name).await {
      tracing::error!(
        id = %device.id,
        airplay_name = %name,
        error = %err,
        "failed to advertise AirPlay 2 receiver"
      );
    }
  }

  for active in airplay.active_ids() {
    if !desired.contains(&active) {
      airplay.remove(&active);
    }
  }

  Ok(())
}
