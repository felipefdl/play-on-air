//! Chromecast discovery via OS mDNS tools.
//!
//! - **macOS**: system `dns-sd` CLI (Bonjour). Pure-Rust mDNS browse does not
//!   reliably see `_googlecast._tcp` on macOS, while Apple's `dns-sd` does.
//! - **Linux**: in-process `mdns-sd` browse (no avahi-daemon or D-Bus). HAOS
//!   containers rarely have a working Avahi client socket; host network alone
//!   is enough for multicast UDP.
//!
//! Registration (shairplay / Bonjour or Avahi) still works for AirPlay ads;
//! discovery alone uses the backend that matches the OS.

#[cfg(target_os = "macos")]
use std::io::{BufRead, BufReader};
#[cfg(target_os = "macos")]
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;
#[cfg(target_os = "macos")]
use std::time::Instant;

use tokio::sync::watch;

use crate::error::{Error, Result};
#[cfg(target_os = "macos")]
use crate::registry::DEFAULT_PENDING_LEAVE;
use crate::registry::{Device, DeviceRegistry};

/// DNS-SD service type for Google Cast (no domain suffix).
pub const GOOGLECAST_REGTYPE: &str = "_googlecast._tcp";

/// Continuous browser that updates a [`DeviceRegistry`].
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
  pub fn spawn(self, shutdown: watch::Receiver<bool>) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
      self.run_blocking(&shutdown);
    })
  }

  fn run_blocking(self, shutdown: &watch::Receiver<bool>) {
    let backend = discovery_backend_label();
    tracing::info!(service = GOOGLECAST_REGTYPE, backend, "browsing for Chromecast devices");

    // Restart the browser if it exits; stop when shutdown is set.
    while !*shutdown.borrow() {
      match run_browse_session(&self.registry, shutdown) {
        Ok(()) => {
          if *shutdown.borrow() {
            break;
          }
          tracing::warn!(backend, "browse exited; restarting in 1s");
          std::thread::sleep(Duration::from_secs(1));
        },
        Err(err) => {
          tracing::error!(backend, error = %err, "browse failed; retrying in 2s");
          std::thread::sleep(Duration::from_secs(2));
        },
      }
    }
  }
}

const fn discovery_backend_label() -> &'static str {
  #[cfg(target_os = "macos")]
  {
    "dns-sd"
  }
  #[cfg(target_os = "linux")]
  {
    "mdns-sd"
  }
  #[cfg(not(any(target_os = "macos", target_os = "linux")))]
  {
    "unsupported"
  }
}

#[cfg(target_os = "macos")]
fn terminate_child(child: &mut Child) {
  drop(child.kill());
  drop(child.wait());
}

/// Browse line event kind (macOS `dns-sd -B`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrowseKind {
  /// Instance added.
  Add,
  /// Instance removed.
  Remove,
}

/// Parsed `dns-sd -B` line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowseEvent {
  /// Add or remove.
  pub kind: BrowseKind,
  /// Service instance name (e.g. `Nest-Audio-…`).
  pub instance: String,
}

/// Parse one `dns-sd -B` output line into an add/remove event.
///
/// Example:
/// `21:42:26.808  Add        3  15 local.               _googlecast._tcp.    Nest-Audio-deadbeef`
pub fn parse_browse_line(line: &str) -> Option<BrowseEvent> {
  let trimmed = line.trim();
  if trimmed.is_empty()
    || trimmed.starts_with("Browsing")
    || trimmed.starts_with("DATE:")
    || trimmed.starts_with("Timestamp")
  {
    return None;
  }
  // Find Add / Rmv / Remove as a standalone token.
  let (kind, rest) = find_token_after(trimmed, " Add ")
    .map(|r| (BrowseKind::Add, r))
    .or_else(|| find_token_after(trimmed, " Rmv ").map(|r| (BrowseKind::Remove, r)))
    .or_else(|| find_token_after(trimmed, " Remove ").map(|r| (BrowseKind::Remove, r)))?;

  // rest: flags if Domain ServiceType Instance...
  // Service type ends with `_googlecast._tcp.` then instance name.
  let marker = "_googlecast._tcp.";
  let (_before, after) = rest.split_once(marker)?;
  let instance = after.trim();
  if instance.is_empty() {
    return None;
  }
  Some(BrowseEvent { kind, instance: instance.to_owned() })
}

fn find_token_after<'a>(line: &'a str, token: &str) -> Option<&'a str> {
  let idx = line.find(token)?;
  line.get(idx + token.len()..)
}

/// Resolved fields from one `dns-sd -L` lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveInfo {
  /// Hostname (may end with `.local.`).
  pub hostname: String,
  /// Port (8009 for speakers; Cast groups use their own port).
  pub port: u16,
  /// Raw TXT string in dns-sd style (`key=value` space-separated, `\` escapes).
  pub txt: String,
  /// IP literal from the reach line (IPv4 or IPv6), used as the Cast host when set.
  pub address: Option<String>,
}

/// One "can be reached at" line from `dns-sd -L`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LookupReachable {
  hostname: String,
  port: u16,
  /// Optional address if the reach line used an IPv4 instead of a hostname.
  address: Option<String>,
  /// Interface ordinal when present (`(interface N)`).
  interface: Option<u32>,
}

/// Parse `dns-sd -L` output for host, port, and TXT blob.
///
/// When multiple "can be reached at" lines exist (multi-homed hosts), prefer a
/// line whose host equals or shares a /24 with a preferred local IPv4, else the
/// first line (deterministic).
pub fn parse_lookup_output(output: &str) -> Option<ResolveInfo> {
  parse_lookup_output_with_preferred(output, &[])
}

/// Like [`parse_lookup_output`] but with an explicit preferred-IPv4 list (testable).
pub fn parse_lookup_output_with_preferred(output: &str, preferred_ipv4: &[String]) -> Option<ResolveInfo> {
  let mut reachables = Vec::new();
  let mut txt = String::new();

  for raw in output.lines() {
    let trimmed = raw.trim();
    if let Some(reached) = trimmed.find("can be reached at ")
      && let Some(entry) = parse_reachable_line(trimmed, reached)
    {
      reachables.push(entry);
    }
    // TXT keys often appear on the same or following line starting with spaces + id=
    if trimmed.contains("id=") || trimmed.contains("fn=") {
      if !txt.is_empty() {
        txt.push(' ');
      }
      txt.push_str(trimmed);
    }
  }

  let chosen = choose_reachable(&reachables, preferred_ipv4)?;
  Some(ResolveInfo {
    hostname: chosen.hostname,
    port: chosen.port,
    txt,
    address: chosen.address,
  })
}

fn parse_reachable_line(trimmed: &str, reached_idx: usize) -> Option<LookupReachable> {
  let prefix_len = "can be reached at ".len();
  let rest = trimmed.get(reached_idx + prefix_len..)?;
  // HOST:PORT (interface …
  let hostport = rest.split_whitespace().next()?;
  let (h, p) = hostport.rsplit_once(':')?;
  let host_raw = h.trim_end_matches('.').to_owned();
  let port = p.parse().unwrap_or(8009);
  let interface = trimmed.find("(interface ").and_then(|i| {
    let after = trimmed.get(i + "(interface ".len()..)?;
    let digits: String = after.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
  });
  // An IP-literal reach host (IPv4 or IPv6) is the Cast host as-is.
  let address = crate::net::parse_ip_host(&host_raw).map(crate::net::format_ip_host);
  Some(LookupReachable {
    hostname: host_raw,
    port,
    address,
    interface,
  })
}

/// Pure: pick the best reach line.
///
/// Preference order:
/// 1. Exact match of a preferred local IPv4 to the reach host/address
/// 2. Same IPv4 /24 (first three octets) as a preferred local address
/// 3. First reach line that is not an IPv6 literal (IPv4 preferred; hostnames resolve
///    IPv4-first)
/// 4. First reach line (stable, not last-wins)
fn choose_reachable(reachables: &[LookupReachable], preferred_ipv4: &[String]) -> Option<LookupReachable> {
  if reachables.is_empty() {
    return None;
  }
  // Exact host/address match against preferred local IPs.
  for pref in preferred_ipv4 {
    if let Some(hit) = reachables
      .iter()
      .find(|r| r.address.as_deref() == Some(pref.as_str()) || r.hostname == *pref)
    {
      return Some(hit.clone());
    }
  }
  // Same /24 as a preferred local IPv4 (typical LAN multi-homed Cast).
  for pref in preferred_ipv4 {
    if let Some(pref_net) = ipv4_slash24_key(pref)
      && let Some(hit) = reachables.iter().find(|r| {
        let host = r.address.as_deref().unwrap_or(r.hostname.as_str());
        ipv4_slash24_key(host) == Some(pref_net)
      })
    {
      return Some(hit.clone());
    }
  }
  // IPv4 (or hostname) before an IPv6 literal; then first line (deterministic, not last-wins).
  reachables
    .iter()
    .find(|r| r.address.as_deref().is_none_or(|a| !a.contains(':')))
    .or_else(|| reachables.first())
    .cloned()
}

/// First three IPv4 octets as a stable key, or `None` if not a dotted-quad.
fn ipv4_slash24_key(s: &str) -> Option<[u8; 3]> {
  if !is_ipv4_literal(s) {
    return None;
  }
  let mut parts = s.split('.');
  let a = parts.next()?.parse().ok()?;
  let b = parts.next()?.parse().ok()?;
  let c = parts.next()?.parse().ok()?;
  Some([a, b, c])
}

fn is_ipv4_literal(s: &str) -> bool {
  let mut parts = s.split('.');
  let mut n = 0;
  for part in parts.by_ref() {
    if part.is_empty() || part.len() > 3 {
      return false;
    }
    if !part.chars().all(|c| c.is_ascii_digit()) {
      return false;
    }
    let Ok(v) = part.parse::<u16>() else {
      return false;
    };
    if v > 255 {
      return false;
    }
    n += 1;
    if n > 4 {
      return false;
    }
  }
  n == 4
}

/// Parse a DNS-SD TXT blob (`key=value` space-separated, `\` escapes spaces in values).
pub fn parse_txt_blob(txt: &str) -> std::collections::HashMap<String, String> {
  let mut map = std::collections::HashMap::new();
  for part in split_txt_parts(txt) {
    if let Some((k, v)) = part.split_once('=') {
      drop(map.insert(k.to_owned(), unescape_dns_sd(v)));
    }
  }
  map
}

/// Split on unescaped whitespace so `fn=Gym\ speaker` stays one part.
fn split_txt_parts(s: &str) -> Vec<String> {
  let mut parts = Vec::new();
  let mut cur = String::new();
  let mut chars = s.chars();
  while let Some(c) = chars.next() {
    if c == '\\' {
      cur.push('\\');
      if let Some(n) = chars.next() {
        cur.push(n);
      }
    } else if c.is_whitespace() {
      if !cur.is_empty() {
        parts.push(std::mem::take(&mut cur));
      }
    } else {
      cur.push(c);
    }
  }
  if !cur.is_empty() {
    parts.push(cur);
  }
  parts
}

fn unescape_dns_sd(s: &str) -> String {
  // dns-sd prints spaces as `\ ` and other escapes as `\X`.
  let mut out = String::with_capacity(s.len());
  let mut chars = s.chars();
  while let Some(c) = chars.next() {
    if c == '\\' {
      if let Some(n) = chars.next() {
        out.push(n);
      }
    } else {
      out.push(c);
    }
  }
  out
}

/// Build a [`Device`] from instance name + resolve info.
pub fn device_from_resolve(instance: &str, info: &ResolveInfo) -> Device {
  let txt = parse_txt_blob(&info.txt);
  let id = txt
    .get("id")
    .cloned()
    .filter(|s| !s.is_empty())
    .unwrap_or_else(|| instance.to_owned());
  let name = txt
    .get("fn")
    .cloned()
    .filter(|s| !s.is_empty())
    .unwrap_or_else(|| instance.replace('-', " "));
  let model = txt.get("md").cloned().unwrap_or_default();
  let hostname = info.hostname.trim_end_matches('.').to_owned();
  let host = info
    .address
    .as_deref()
    .map(str::trim)
    .filter(|a| !a.is_empty())
    .map_or_else(|| resolve_cast_host(&hostname), ToOwned::to_owned);
  let port = if info.port == 0 { 8009 } else { info.port };
  Device::new(id, name, host, hostname, port, instance).with_model(model)
}

/// Mark a device pending leave when mDNS reports a remove (debounced ~20s).
///
/// Exact match only: by stored TXT `id` or by the instance string recorded at appear.
/// Info-logs only on the first transition into pending leave (not on re-marks).
/// macOS-only: the Linux backend ignores `ServiceRemoved` (TTL + Cast reachability
/// drive leave there), so this has no Linux caller.
#[cfg(target_os = "macos")]
fn leave_by_instance(registry: &DeviceRegistry, instance: &str) {
  let now = Instant::now();
  match registry.mark_pending_leave_by_instance(instance, now, DEFAULT_PENDING_LEAVE) {
    Some((id, crate::registry::PendingLeaveMark::NewlyMarked)) => {
      tracing::info!(
        %id,
        instance,
        grace_secs = DEFAULT_PENDING_LEAVE.as_secs(),
        "Chromecast pending leave"
      );
    },
    Some((_id, crate::registry::PendingLeaveMark::AlreadyPending)) => {
      // Already pending: keep quiet to avoid re-remove spam.
    },
    Some((id, crate::registry::PendingLeaveMark::NotFound)) => {
      // Matched under read lock then vanished before write lock (TOCTOU).
      tracing::debug!(
        %id,
        instance,
        "pending leave mark lost race (device gone between match and mark)"
      );
    },
    None => {
      tracing::debug!(instance, "leave for unknown Chromecast instance (ignored)");
    },
  }
}

/// Cast control host for `hostname`: resolved IPv4 preferred, else IPv6 (link-local
/// as `fe80::…%<scope index>`), else the hostname itself.
pub fn resolve_cast_host(hostname: &str) -> String {
  crate::net::resolve_host_ip(hostname).unwrap_or_else(|| {
    let host = hostname.trim_end_matches('.');
    if host.is_empty() {
      "127.0.0.1".to_owned()
    } else {
      host.to_owned()
    }
  })
}

// ---------------------------------------------------------------------------
// Shared child stdout session helper
// ---------------------------------------------------------------------------

/// Kill `child` when `shutdown` is set so a blocking stdout reader unblocks.
#[cfg(target_os = "macos")]
fn spawn_shutdown_watcher(
  shared_child: Arc<std::sync::Mutex<Child>>,
  session_done: Arc<std::sync::atomic::AtomicBool>,
  shutdown: watch::Receiver<bool>,
) -> std::thread::JoinHandle<()> {
  std::thread::spawn(move || {
    while !session_done.load(std::sync::atomic::Ordering::Relaxed) {
      if *shutdown.borrow() {
        if let Ok(mut guard) = shared_child.lock() {
          terminate_child(&mut guard);
        }
        break;
      }
      std::thread::sleep(Duration::from_millis(200));
    }
  })
}

// ---------------------------------------------------------------------------
// macOS: dns-sd
// ---------------------------------------------------------------------------

#[cfg(target_os = "macos")]
fn run_browse_session(registry: &DeviceRegistry, shutdown: &watch::Receiver<bool>) -> Result<()> {
  let mut child = spawn_dns_sd_browse()?;
  let stdout = child
    .stdout
    .take()
    .ok_or_else(|| Error::Discovery("dns-sd browse missing stdout".to_owned()))?;
  let reader = BufReader::new(stdout);

  let shared_child = Arc::new(std::sync::Mutex::new(child));
  let session_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
  let watcher = spawn_shutdown_watcher(Arc::clone(&shared_child), Arc::clone(&session_done), shutdown.clone());

  for raw_line in reader.lines() {
    if *shutdown.borrow() {
      break;
    }
    let Ok(line) = raw_line else {
      tracing::warn!("dns-sd browse read error");
      break;
    };
    let Some(event) = parse_browse_line(&line) else {
      continue;
    };
    match event.kind {
      BrowseKind::Add => match resolve_instance_dns_sd(&event.instance, shutdown) {
        Ok(device) => {
          let was_pending = registry.is_pending_leave(&device.id);
          let is_new = registry.appear(device.clone());
          if was_pending {
            tracing::info!(
              id = %device.id,
              name = %device.name,
              "Chromecast pending leave cancelled"
            );
          }
          if is_new {
            tracing::info!(
              id = %device.id,
              name = %device.name,
              host = %device.host,
              port = device.port,
              instance = %device.instance,
              "Chromecast appeared"
            );
          } else if !was_pending {
            tracing::debug!(
              id = %device.id,
              host = %device.host,
              "Chromecast re-announced"
            );
          }
        },
        Err(err) => {
          tracing::warn!(
            instance = %event.instance,
            error = %err,
            "failed to resolve Chromecast"
          );
        },
      },
      BrowseKind::Remove => {
        leave_by_instance(registry, &event.instance);
      },
    }
  }

  session_done.store(true, std::sync::atomic::Ordering::Relaxed);
  if let Ok(mut guard) = shared_child.lock() {
    terminate_child(&mut guard);
  }
  drop(watcher.join());
  Ok(())
}

#[cfg(target_os = "macos")]
fn spawn_dns_sd_browse() -> Result<Child> {
  Command::new("dns-sd")
    .args(["-B", GOOGLECAST_REGTYPE, "local."])
    .stdout(Stdio::piped())
    .stderr(Stdio::null())
    .spawn()
    .map_err(|err| Error::Discovery(format!("failed to spawn dns-sd -B: {err}")))
}

#[cfg(target_os = "macos")]
fn resolve_instance_dns_sd(instance: &str, shutdown: &watch::Receiver<bool>) -> Result<Device> {
  let output = run_lookup_dns_sd(instance, shutdown)?;
  // Prefer a resolve line that matches this host's primary LAN IPv4 when multi-homed.
  let preferred = vec![crate::net::advertise_host_ip()];
  let info = parse_lookup_output_with_preferred(&output, &preferred)
    .ok_or_else(|| Error::Discovery(format!("could not parse dns-sd -L for {instance}")))?;
  Ok(device_from_resolve(instance, &info))
}

/// Max wall-clock for one `dns-sd -L` resolve before the child is killed.
#[cfg(target_os = "macos")]
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);
/// Watchdog poll cadence for lookup deadline / shutdown checks.
#[cfg(target_os = "macos")]
const LOOKUP_WATCH_POLL: Duration = Duration::from_millis(100);

#[cfg(target_os = "macos")]
fn run_lookup_dns_sd(instance: &str, shutdown: &watch::Receiver<bool>) -> Result<String> {
  // dns-sd -L streams; collect for a short window then kill.
  let mut child = Command::new("dns-sd")
    .args(["-L", instance, GOOGLECAST_REGTYPE, "local."])
    .stdout(Stdio::piped())
    .stderr(Stdio::null())
    .spawn()
    .map_err(|err| Error::Discovery(format!("failed to spawn dns-sd -L: {err}")))?;

  let stdout = child
    .stdout
    .take()
    .ok_or_else(|| Error::Discovery("dns-sd -L missing stdout".to_owned()))?;

  // `lines()` blocks until dns-sd prints; a never-resolving instance would
  // wedge the discovery thread (and runtime shutdown, which joins blocking
  // tasks) forever. Kill the child at the deadline or on shutdown so the
  // blocking reader always unblocks.
  let deadline = Instant::now() + LOOKUP_TIMEOUT;
  let shared_child = Arc::new(std::sync::Mutex::new(child));
  let lookup_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
  let watchdog = {
    let watchdog_child = Arc::clone(&shared_child);
    let watchdog_done = Arc::clone(&lookup_done);
    let watchdog_shutdown = shutdown.clone();
    std::thread::spawn(move || {
      while !watchdog_done.load(std::sync::atomic::Ordering::Relaxed) {
        if Instant::now() > deadline || *watchdog_shutdown.borrow() {
          if let Ok(mut guard) = watchdog_child.lock() {
            terminate_child(&mut guard);
          }
          break;
        }
        std::thread::sleep(LOOKUP_WATCH_POLL);
      }
    })
  };

  let reader = BufReader::new(stdout);
  let mut buf = String::new();
  for raw_line in reader.lines() {
    if Instant::now() > deadline {
      break;
    }
    let Ok(text) = raw_line else {
      break;
    };
    buf.push_str(&text);
    buf.push('\n');
    // Enough once we have reachability + txt.
    if buf.contains("can be reached at ") && (buf.contains("id=") || buf.contains("fn=")) {
      break;
    }
  }
  lookup_done.store(true, std::sync::atomic::Ordering::Relaxed);
  if let Ok(mut guard) = shared_child.lock() {
    terminate_child(&mut guard);
  }
  drop(watchdog.join());
  if buf.is_empty() {
    return Err(Error::Discovery(format!("empty dns-sd -L for {instance}")));
  }
  Ok(buf)
}

// ---------------------------------------------------------------------------
// Linux: mdns-sd
// ---------------------------------------------------------------------------

/// mDNS service type including domain (required by `mdns-sd`).
#[cfg(target_os = "linux")]
const GOOGLECAST_MDNS_TYPE: &str = "_googlecast._tcp.local.";

/// Poll cadence while waiting for mDNS events so shutdown stays responsive.
#[cfg(target_os = "linux")]
const MDNS_RECV_POLL: Duration = Duration::from_millis(250);

#[cfg(target_os = "linux")]
fn run_browse_session(registry: &DeviceRegistry, shutdown: &watch::Receiver<bool>) -> Result<()> {
  use mdns_sd::{ServiceDaemon, ServiceEvent};

  let daemon = ServiceDaemon::new()
    .map_err(|err| Error::Discovery(format!("mdns-sd daemon failed (need host network / multicast UDP): {err}")))?;
  let receiver = daemon
    .browse(GOOGLECAST_MDNS_TYPE)
    .map_err(|err| Error::Discovery(format!("mdns-sd browse failed: {err}")))?;

  tracing::debug!(service = GOOGLECAST_MDNS_TYPE, "mdns-sd browse started");

  while !*shutdown.borrow() {
    // flume RecvTimeoutError: Timeout keeps the loop (shutdown poll); other = end.
    match receiver.recv_timeout(MDNS_RECV_POLL) {
      Ok(ServiceEvent::ServiceResolved(resolved)) => {
        let device = device_from_mdns_resolved(resolved.as_ref());
        let was_pending = registry.is_pending_leave(&device.id);
        let is_new = registry.appear(device.clone());
        if was_pending {
          tracing::info!(
            id = %device.id,
            name = %device.name,
            "Chromecast pending leave cancelled"
          );
        }
        if is_new {
          tracing::info!(
            id = %device.id,
            name = %device.name,
            host = %device.host,
            port = device.port,
            instance = %device.instance,
            "Chromecast appeared"
          );
        } else if !was_pending {
          tracing::debug!(
            id = %device.id,
            host = %device.host,
            "Chromecast re-resolved"
          );
        }
      },
      Ok(ServiceEvent::ServiceRemoved(_ty, fullname)) => {
        // mdns-sd spuriously emits ServiceRemoved during re-query / interface churn.
        // Honoring it with pending-leave withdrew AirPlay ads for ~44 s on healthy
        // devices. Ignore removals; Linux leave is TTL + warm Cast unreachability
        // (see `should_leave_linux_stale` / app stale expiry).
        debug_assert!(
          !crate::registry::linux_service_removed_triggers_leave(),
          "Linux ServiceRemoved must not mark pending leave"
        );
        tracing::debug!(fullname = %fullname, "mdns-sd ServiceRemoved ignored on Linux");
      },
      Ok(ServiceEvent::SearchStopped(ty)) => {
        tracing::warn!(%ty, "mdns-sd search stopped");
        break;
      },
      // SearchStarted / ServiceFound / future non_exhaustive variants.
      Ok(_) => {},
      Err(err) => {
        // Discriminate without depending on flume types in our crate surface.
        let msg = err.to_string();
        if msg.contains("disconnect") || msg.contains("Disconnect") {
          tracing::warn!(error = %err, "mdns-sd browse channel disconnected");
          break;
        }
        // Timeout: loop and re-check shutdown.
      },
    }
  }

  if let Err(err) = daemon.shutdown() {
    tracing::debug!(error = %err, "mdns-sd daemon shutdown");
  }
  Ok(())
}

/// Instance label from a full mDNS name (`Name._googlecast._tcp.local.`).
#[cfg(target_os = "linux")]
fn instance_from_mdns_fullname(fullname: &str) -> &str {
  fullname
    .split("._googlecast._tcp")
    .next()
    .map(str::trim)
    .filter(|s| !s.is_empty())
    .unwrap_or(fullname)
}

/// Map a resolved mDNS service into a Cast [`Device`].
///
/// Host: IPv4 preferred (same /24 as this host first), else IPv6 with its scope id;
/// see [`crate::net::select_cast_addr`].
#[cfg(target_os = "linux")]
fn device_from_mdns_resolved(resolved: &mdns_sd::ResolvedService) -> Device {
  let props = resolved.get_properties();
  let id = props
    .get("id")
    .map(|p| p.val_str().to_owned())
    .filter(|s| !s.is_empty())
    .unwrap_or_else(|| instance_from_mdns_fullname(resolved.get_fullname()).to_owned());
  let name = props
    .get("fn")
    .map(|p| p.val_str().to_owned())
    .filter(|s| !s.is_empty())
    .unwrap_or_else(|| instance_from_mdns_fullname(resolved.get_fullname()).replace('-', " "));
  let model = props.get("md").map(|p| p.val_str().to_owned()).unwrap_or_default();
  let hostname = resolved.get_hostname().trim_end_matches('.').to_owned();
  let mut candidates: Vec<std::net::SocketAddr> = resolved
    .get_addresses()
    .iter()
    .map(|ip| match ip.to_ip_addr() {
      std::net::IpAddr::V4(v4) => std::net::SocketAddr::from((v4, 0)),
      std::net::IpAddr::V6(v6) => {
        // Link-local IPv6 is only dialable with the interface it was seen on.
        let scope_id = if let mdns_sd::ScopedIp::V6(scoped) = ip {
          scoped.scope_id().index
        } else {
          0
        };
        std::net::SocketAddr::V6(std::net::SocketAddrV6::new(v6, 0, 0, scope_id))
      },
    })
    .collect();
  // The address set is a HashSet: sort so ties pick the same address every resolve.
  candidates.sort_unstable();
  let local_v4 = crate::net::advertise_host_ip().parse::<std::net::Ipv4Addr>().ok();
  let host = crate::net::select_cast_addr(&candidates, local_v4)
    .map_or_else(|| resolve_cast_host(&hostname), crate::net::format_ip_host);
  let port = {
    let p = resolved.get_port();
    if p == 0 { 8009 } else { p }
  };
  let instance = instance_from_mdns_fullname(resolved.get_fullname()).to_owned();
  Device::new(id, name, host, hostname, port, instance).with_model(model)
}

// ---------------------------------------------------------------------------
// Other OS
// ---------------------------------------------------------------------------

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn run_browse_session(_registry: &DeviceRegistry, _shutdown: &watch::Receiver<bool>) -> Result<()> {
  Err(Error::Discovery(
    "Chromecast discovery requires dns-sd (macOS) or mdns-sd (Linux)".to_owned(),
  ))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn parse_browse_add_line() {
    let line = "21:42:26.808  Add        3  15 local.               _googlecast._tcp.    Nest-Audio-56cb7fe7e13f325625325c4a304c57fa";
    let ev = parse_browse_line(line).expect("parse");
    assert_eq!(ev.kind, BrowseKind::Add);
    assert_eq!(ev.instance, "Nest-Audio-56cb7fe7e13f325625325c4a304c57fa");
  }

  #[test]
  fn parse_browse_rmv_line() {
    let line = "21:42:26.808  Rmv        2  15 local.               _googlecast._tcp.    Nest-Audio-56cb7fe7e13f325625325c4a304c57fa";
    let ev = parse_browse_line(line).expect("parse");
    assert_eq!(ev.kind, BrowseKind::Remove);
  }

  #[test]
  fn parse_browse_ignores_header() {
    assert!(parse_browse_line("Browsing for _googlecast._tcp.local.").is_none());
    assert!(parse_browse_line("Timestamp     A/R    Flags  if Domain").is_none());
  }

  #[test]
  fn parse_lookup_and_txt() {
    let out = r"
Lookup Nest-Audio-x._googlecast._tcp.local.
21:42:29.847  Nest-Audio-x._googlecast._tcp.local. can be reached at 56cb7fe7-e13f-3256-2532-5c4a304c57fa.local.:8009 (interface 15) Flags: 1
 id=56cb7fe7e13f325625325c4a304c57fa cd=ABC ve=05 md=Nest\ Audio fn=Gym\ speaker ca=199172 st=0
";
    let info = parse_lookup_output(out).expect("lookup");
    assert_eq!(info.hostname, "56cb7fe7-e13f-3256-2532-5c4a304c57fa.local");
    assert_eq!(info.port, 8009);
    assert!(info.address.is_none());
    let d = device_from_resolve("Nest-Audio-x", &info);
    assert_eq!(d.id, "56cb7fe7e13f325625325c4a304c57fa");
    assert_eq!(d.name, "Gym speaker");
    assert_eq!(d.port, 8009);
    assert_eq!(d.instance, "Nest-Audio-x");
  }

  #[test]
  fn parse_lookup_prefers_preferred_ipv4_not_last_line() {
    let out = r"
Lookup multi-homed._googlecast._tcp.local.
10:00:00.001  multi-homed._googlecast._tcp.local. can be reached at 10.0.0.5:8009 (interface 4) Flags: 1
10:00:00.002  multi-homed._googlecast._tcp.local. can be reached at 192.168.1.50:8009 (interface 15) Flags: 1
10:00:00.003  multi-homed._googlecast._tcp.local. can be reached at 172.16.0.9:8009 (interface 8) Flags: 1
 id=aabbcc fn=Kitchen
";
    // Without preference: first line wins (not last).
    let first = parse_lookup_output(out).expect("lookup");
    assert_eq!(first.hostname, "10.0.0.5");
    assert_eq!(first.address.as_deref(), Some("10.0.0.5"));

    // Preferred LAN IPv4 selects the middle line, not last-wins.
    let preferred = vec!["192.168.1.50".to_owned()];
    let chosen = parse_lookup_output_with_preferred(out, &preferred).expect("lookup");
    assert_eq!(chosen.hostname, "192.168.1.50");
    assert_eq!(chosen.port, 8009);
    assert_eq!(chosen.address.as_deref(), Some("192.168.1.50"));
  }

  #[test]
  fn parse_lookup_prefers_same_slash24_as_local_ip() {
    // Preferred is the *local* host address (advertise_host_ip), not the Cast address.
    // Equality match almost never hits; same /24 must select the LAN reach line.
    let out = r"
Lookup multi-homed._googlecast._tcp.local.
10:00:00.001  multi-homed._googlecast._tcp.local. can be reached at 10.0.0.5:8009 (interface 4) Flags: 1
10:00:00.002  multi-homed._googlecast._tcp.local. can be reached at 192.168.1.50:8009 (interface 15) Flags: 1
 id=aabbcc fn=Kitchen
";
    let preferred = vec!["192.168.1.10".to_owned()];
    let chosen = parse_lookup_output_with_preferred(out, &preferred).expect("lookup");
    assert_eq!(chosen.hostname, "192.168.1.50");
    assert_eq!(chosen.address.as_deref(), Some("192.168.1.50"));
    assert_eq!(chosen.port, 8009);
  }

  #[test]
  fn unescape_spaces_in_txt() {
    let m = parse_txt_blob(r"fn=Gym\ speaker id=abc");
    assert_eq!(m.get("fn").map(String::as_str), Some("Gym speaker"));
    assert_eq!(m.get("id").map(String::as_str), Some("abc"));
  }

  #[test]
  fn resolve_cast_host_trims_dot() {
    let host = resolve_cast_host("definitely-not-a-real-host.invalid.");
    assert_eq!(host, "definitely-not-a-real-host.invalid");
  }

  #[test]
  fn resolve_cast_host_keeps_ipv6_literals_with_scope() {
    // Link-local keeps its numeric scope (needed to dial); brackets are stripped.
    assert_eq!(resolve_cast_host("fe80::1c2b:3cff:fe4d:5e6f%7"), "fe80::1c2b:3cff:fe4d:5e6f%7");
    assert_eq!(resolve_cast_host("[2001:db8::50]"), "2001:db8::50");
    assert_eq!(resolve_cast_host("192.168.1.50."), "192.168.1.50");
  }

  #[test]
  fn parse_lookup_ipv6_only_reach_line_becomes_cast_host() {
    let out = r"
Lookup v6-only._googlecast._tcp.local.
10:00:00.001  v6-only._googlecast._tcp.local. can be reached at 2001:db8::50:8009 (interface 4) Flags: 1
 id=v6id fn=Attic md=Nest\ Mini
";
    let info = parse_lookup_output(out).expect("lookup");
    assert_eq!(info.port, 8009);
    assert_eq!(info.address.as_deref(), Some("2001:db8::50"));
    let d = device_from_resolve("v6-only", &info);
    assert_eq!(d.host, "2001:db8::50");
    assert_eq!(d.model, "Nest Mini");
  }

  #[test]
  fn parse_lookup_prefers_ipv4_over_earlier_ipv6_line() {
    let out = r"
Lookup dual._googlecast._tcp.local.
10:00:00.001  dual._googlecast._tcp.local. can be reached at 2001:db8::50:8009 (interface 4) Flags: 1
10:00:00.002  dual._googlecast._tcp.local. can be reached at 10.0.0.5:8009 (interface 4) Flags: 1
 id=dual fn=Den
";
    let info = parse_lookup_output(out).expect("lookup");
    assert_eq!(info.address.as_deref(), Some("10.0.0.5"));
  }

  #[test]
  fn cast_group_keeps_its_own_port_and_model() {
    // Groups advertise on the leader's host with a non-8009 port; Cast control must use it.
    let out = r"
Lookup Google-Cast-Group-abc._googlecast._tcp.local.
10:00:00.001  Google-Cast-Group-abc._googlecast._tcp.local. can be reached at 7e5621b9-9750-22b2-4ef6-a17173a19135.local.:32187 (interface 4) Flags: 1
 id=1c2a3b4c5d6e7f80 cd=1c2a md=Google\ Cast\ Group fn=Downstairs ca=199204 st=0
";
    let info = parse_lookup_output(out).expect("lookup");
    let d = device_from_resolve("Google-Cast-Group-abc", &info);
    assert_eq!(d.port, 32187);
    assert_eq!(d.model, "Google Cast Group");
    assert_eq!(d.name, "Downstairs");
    assert_eq!(d.id, "1c2a3b4c5d6e7f80");
  }

  #[test]
  fn device_from_resolve_prefers_address() {
    let info = ResolveInfo {
      hostname: "cast.local".to_owned(),
      port: 8009,
      txt: "id=xyz fn=Kitchen".to_owned(),
      address: Some("10.0.0.9".to_owned()),
    };
    let d = device_from_resolve("Kitchen-Speaker", &info);
    assert_eq!(d.host, "10.0.0.9");
    assert_eq!(d.id, "xyz");
    assert_eq!(d.name, "Kitchen");
  }
}
