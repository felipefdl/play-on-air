//! Chromecast mDNS discovery (`_googlecast._tcp.local.`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use mdns_sd::{ServiceDaemon, ServiceEvent};
use tokio::sync::watch;

use crate::error::{Error, Result};
use crate::registry::{Device, DeviceRegistry};

/// Service type advertised by Google Cast devices.
pub const GOOGLECAST_SERVICE: &str = "_googlecast._tcp.local.";

/// Continuous mDNS browser that updates a [`DeviceRegistry`].
#[derive(Debug)]
pub struct Discovery {
  registry: Arc<DeviceRegistry>,
}

impl Discovery {
  /// Create a discovery front-end over the shared registry.
  pub const fn new(registry: Arc<DeviceRegistry>) -> Self {
    Self { registry }
  }

  /// Shared registry reference.
  pub fn registry(&self) -> Arc<DeviceRegistry> {
    Arc::clone(&self.registry)
  }

  /// Spawn a background task that browses until `shutdown` becomes true.
  ///
  /// Discovery failures are logged as typed errors and do not panic the process.
  pub fn spawn(self, shutdown: watch::Receiver<bool>) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
      if let Err(err) = self.run_blocking(&shutdown) {
        tracing::error!(error = %err, "Chromecast discovery stopped");
      }
    })
  }

  /// Blocking browse loop (runs on a blocking thread).
  fn run_blocking(self, shutdown: &watch::Receiver<bool>) -> Result<()> {
    let daemon =
      ServiceDaemon::new().map_err(|err| Error::Discovery(format!("failed to create mDNS daemon: {err}")))?;

    let receiver = daemon
      .browse(GOOGLECAST_SERVICE)
      .map_err(|err| Error::Discovery(format!("failed to browse {GOOGLECAST_SERVICE}: {err}")))?;

    tracing::info!(service = GOOGLECAST_SERVICE, "browsing for Chromecast devices");

    loop {
      if *shutdown.borrow() {
        tracing::info!("discovery shutdown requested");
        break;
      }

      // Timed recv so we can observe shutdown without hanging forever.
      // `mdns_sd::Receiver` is flume; timeout displays as "timed out waiting on a channel".
      match receiver.recv_timeout(Duration::from_millis(500)) {
        Ok(event) => self.handle_event(event),
        Err(err) => {
          let msg = err.to_string();
          if msg.contains("timed out") {
            // Idle LAN — keep polling.
          } else {
            tracing::warn!(error = %err, "mDNS browse receive error");
            break;
          }
        },
      }
    }

    if let Err(err) = daemon.shutdown() {
      tracing::warn!(error = %err, "mDNS daemon shutdown error");
    }
    Ok(())
  }

  fn handle_event(&self, event: ServiceEvent) {
    match event {
      ServiceEvent::ServiceResolved(info) => {
        let fullname = info.get_fullname();
        let name = fullname_to_instance(fullname);
        let id = info.get_property_val_str("id").map_or_else(|| name.clone(), str::to_owned);
        let host = pick_host_address(info.as_ref());
        let port = info.get_port();
        let device = Device {
          id: id.clone(),
          name: name.clone(),
          host,
          port,
          last_seen: Instant::now(),
        };
        tracing::info!(%id, %name, port, "Chromecast appeared");
        self.registry.appear(device);
      },
      ServiceEvent::ServiceRemoved(_service_type, fullname) => {
        let name = fullname_to_instance(&fullname);
        let removed = {
          let list = self.registry.list();
          list.into_iter().find(|d| d.name == name || d.id == name).map(|d| d.id)
        };
        if let Some(id) = removed {
          tracing::info!(%id, %name, "Chromecast left");
          drop(self.registry.leave(&id));
        }
      },
      ServiceEvent::SearchStarted(_) | ServiceEvent::SearchStopped(_) | ServiceEvent::ServiceFound(_, _) => {},
      other => {
        tracing::trace!(?other, "mDNS event");
      },
    }
  }
}

fn pick_host_address(info: &mdns_sd::ResolvedService) -> String {
  // Prefer IPv4 for Cast control URLs.
  if let Some(v4) = info.get_addresses_v4().into_iter().next() {
    return v4.to_string();
  }
  if let Some(addr) = info.get_addresses().iter().next() {
    return addr.to_string();
  }
  info.get_hostname().trim_end_matches('.').to_owned()
}

/// Strip `._googlecast._tcp.local.` suffix from a service fullname.
fn fullname_to_instance(fullname: &str) -> String {
  fullname
    .strip_suffix("._googlecast._tcp.local.")
    .or_else(|| fullname.strip_suffix("._googlecast._tcp.local"))
    .unwrap_or(fullname)
    .to_owned()
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn strips_service_suffix() {
    assert_eq!(fullname_to_instance("Living Room._googlecast._tcp.local."), "Living Room");
  }
}
