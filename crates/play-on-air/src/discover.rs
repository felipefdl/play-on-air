//! Chromecast discovery via the system `dns-sd` CLI (Bonjour).
//!
//! Pure-Rust mDNS and `astro-dnssd` browse do not reliably see `_googlecast._tcp`
//! on macOS, while Apple's `dns-sd` does. Registration (shairplay / Bonjour) still
//! works for AirPlay ads; discovery alone uses the CLI that matches the OS.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::error::{Error, Result};
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
    tracing::info!(service = GOOGLECAST_REGTYPE, "browsing for Chromecast devices via dns-sd");

    // Restart the browser if it exits; stop when shutdown is set.
    while !*shutdown.borrow() {
      match run_browse_session(&self.registry, shutdown) {
        Ok(()) => {
          if *shutdown.borrow() {
            break;
          }
          tracing::warn!("dns-sd browse exited; restarting in 1s");
          std::thread::sleep(Duration::from_secs(1));
        },
        Err(err) => {
          tracing::error!(error = %err, "dns-sd browse failed; retrying in 2s");
          std::thread::sleep(Duration::from_secs(2));
        },
      }
    }
  }
}

fn run_browse_session(registry: &DeviceRegistry, shutdown: &watch::Receiver<bool>) -> Result<()> {
  let mut child = spawn_browse()?;
  let stdout = child
    .stdout
    .take()
    .ok_or_else(|| Error::Discovery("dns-sd browse missing stdout".to_owned()))?;
  let reader = BufReader::new(stdout);

  // `lines()` blocks until dns-sd prints; kill the child on shutdown so the
  // reader unblocks and runtime shutdown never hangs on this thread.
  let shared_child = Arc::new(std::sync::Mutex::new(child));
  let session_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
  let watcher = {
    let watcher_child = Arc::clone(&shared_child);
    let watcher_done = Arc::clone(&session_done);
    let watcher_shutdown = shutdown.clone();
    std::thread::spawn(move || {
      while !watcher_done.load(std::sync::atomic::Ordering::Relaxed) {
        if *watcher_shutdown.borrow() {
          if let Ok(mut guard) = watcher_child.lock() {
            terminate_child(&mut guard);
          }
          break;
        }
        std::thread::sleep(Duration::from_millis(200));
      }
    })
  };

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
      BrowseKind::Add => match resolve_instance(&event.instance) {
        Ok(device) => {
          tracing::info!(
            id = %device.id,
            name = %device.name,
            host = %device.host,
            port = device.port,
            "Chromecast appeared"
          );
          registry.appear(device);
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

fn spawn_browse() -> Result<Child> {
  Command::new("dns-sd")
    .args(["-B", GOOGLECAST_REGTYPE, "local."])
    .stdout(Stdio::piped())
    .stderr(Stdio::null())
    .spawn()
    .map_err(|err| Error::Discovery(format!("failed to spawn dns-sd -B: {err}")))
}

fn terminate_child(child: &mut Child) {
  drop(child.kill());
  drop(child.wait());
}

/// Browse line event kind.
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

/// Resolved fields from `dns-sd -L`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveInfo {
  /// Hostname (may end with `.local.`).
  pub hostname: String,
  /// Port (usually 8009).
  pub port: u16,
  /// Raw TXT string from the lookup line(s).
  pub txt: String,
}

/// Parse `dns-sd -L` output for host, port, and TXT blob.
pub fn parse_lookup_output(output: &str) -> Option<ResolveInfo> {
  let mut hostname = None;
  let mut port = None;
  let mut txt = String::new();

  for raw in output.lines() {
    let trimmed = raw.trim();
    if let Some(reached) = trimmed.find("can be reached at ") {
      let rest = trimmed.get(reached + "can be reached at ".len()..)?;
      // HOST:PORT (interface …
      let hostport = rest.split_whitespace().next()?;
      let (h, p) = hostport.rsplit_once(':')?;
      hostname = Some(h.trim_end_matches('.').to_owned());
      port = p.parse().ok();
    }
    // TXT keys often appear on the same or following line starting with spaces + id=
    if trimmed.contains("id=") || trimmed.contains("fn=") {
      if !txt.is_empty() {
        txt.push(' ');
      }
      txt.push_str(trimmed);
    }
  }

  Some(ResolveInfo {
    hostname: hostname?,
    port: port.unwrap_or(8009),
    txt,
  })
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
  let hostname = info.hostname.trim_end_matches('.').to_owned();
  let host = resolve_cast_host(&hostname);
  let port = if info.port == 0 { 8009 } else { info.port };
  Device {
    id,
    name,
    host,
    hostname,
    port,
    last_seen: Instant::now(),
  }
}

fn resolve_instance(instance: &str) -> Result<Device> {
  let output = run_lookup(instance)?;
  let info = parse_lookup_output(&output)
    .ok_or_else(|| Error::Discovery(format!("could not parse dns-sd -L for {instance}")))?;
  Ok(device_from_resolve(instance, &info))
}

fn run_lookup(instance: &str) -> Result<String> {
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

  let reader = BufReader::new(stdout);
  let mut buf = String::new();
  let deadline = Instant::now() + Duration::from_secs(3);
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
  terminate_child(&mut child);
  if buf.is_empty() {
    return Err(Error::Discovery(format!("empty dns-sd -L for {instance}")));
  }
  Ok(buf)
}

fn leave_by_instance(registry: &DeviceRegistry, instance: &str) {
  // Instance name may match id or be a prefix of name; also try TXT id later.
  let list = registry.list();
  let rid = list
    .iter()
    .find(|d| d.id == instance || d.name == instance || d.id.contains(instance) || instance.contains(&d.id))
    .map(|d| d.id.clone());
  if let Some(id) = rid {
    tracing::info!(%id, instance, "Chromecast left");
    drop(registry.leave(&id));
  }
}

/// Prefer a resolved IPv4 address; fall back to hostname for Cast control.
pub fn resolve_cast_host(hostname: &str) -> String {
  crate::net::resolve_host_ipv4(hostname).unwrap_or_else(|| {
    let host = hostname.trim_end_matches('.');
    if host.is_empty() {
      "127.0.0.1".to_owned()
    } else {
      host.to_owned()
    }
  })
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
    let d = device_from_resolve("Nest-Audio-x", &info);
    assert_eq!(d.id, "56cb7fe7e13f325625325c4a304c57fa");
    assert_eq!(d.name, "Gym speaker");
    assert_eq!(d.port, 8009);
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
}
