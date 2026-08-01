//! LAN host IP helpers for advertising Cast-reachable media URLs and Cast connect.

use std::net::{IpAddr, Ipv4Addr, UdpSocket};

use if_addrs::{IfAddr, get_if_addrs};

/// Best-effort non-loopback IPv4 for media URLs Chromecasts can pull.
pub fn advertise_host_ip() -> String {
  if let Some(ip) = preferred_local_ipv4s().into_iter().next() {
    return ip.to_string();
  }
  if let Some(ip) = udp_discover_ipv4(Ipv4Addr::new(8, 8, 8, 8), 80) {
    return ip.to_string();
  }
  Ipv4Addr::LOCALHOST.to_string()
}

/// Pick a local IPv4 that can reach `peer_host` (Cast device).
///
/// Multi-homed Macs often have two addresses on the same /24 (e.g. Wi‑Fi `en0` and
/// Thunderbolt `en7`). Advertising / routing via the wrong one yields
/// `No route to host` to Nest speakers and silent playback.
pub fn advertise_host_for_peer(peer_host: &str) -> String {
  let peer = peer_host.trim().trim_end_matches('.');
  if peer.is_empty() {
    return advertise_host_ip();
  }

  let peer_v4 = resolve_host_ipv4(peer).and_then(|s| s.parse::<Ipv4Addr>().ok());

  if let Some(peer_ip) = peer_v4 {
    // Prefer local IPv4s on the same subnet, ranked by interface preference.
    if let Some(ip) = preferred_local_ipv4s()
      .into_iter()
      .find(|local| same_class_c(*local, peer_ip) || on_same_iface_subnet(*local, peer_ip))
    {
      return ip.to_string();
    }
    // Fall back: kernel route toward peer.
    if let Some(local) = udp_discover_ipv4(peer_ip, 8009) {
      return local.to_string();
    }
  }

  advertise_host_ip()
}

/// Ordered Cast connect targets for a device: prefer IPv4 literals, then hostname.
pub fn cast_connect_hosts(host: &str, hostname: Option<&str>) -> Vec<String> {
  let mut out = Vec::new();
  let mut push = |s: String| {
    if !s.is_empty() && !out.iter().any(|e| e == &s) {
      out.push(s);
    }
  };

  // Fresh resolve of primary host field.
  if let Some(ip) = resolve_host_ipv4(host) {
    push(ip);
  }
  push(host.trim().trim_end_matches('.').to_owned());

  if let Some(raw_hn) = hostname {
    let name = raw_hn.trim().trim_end_matches('.');
    if let Some(ip) = resolve_host_ipv4(name) {
      push(ip);
    }
    push(name.to_owned());
  }

  // Also try other local-subnet guesses from preferred interfaces' peers? skip.
  out
}

/// Best-effort wake so Nest ARP/route is hot before Cast TLS.
pub fn wake_cast_host(host: &str) {
  let target = host.trim().trim_end_matches('.');
  if target.is_empty() {
    return;
  }
  // UDP poke on Cast port (no response required).
  if let Ok(sock) = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)) {
    drop(sock.send_to(&[0_u8], (target, 8009)));
  }
  // ICMP/ARP via system ping (macOS: -W is ms).
  drop(
    std::process::Command::new("ping")
      .args(["-c", "1", "-W", "500", target])
      .stdout(std::process::Stdio::null())
      .stderr(std::process::Stdio::null())
      .status(),
  );
}

/// Resolve `hostname` (often `uuid.local`) to an IPv4 string, if possible.
pub fn resolve_host_ipv4(hostname: &str) -> Option<String> {
  use std::net::ToSocketAddrs;
  let host = hostname.trim().trim_end_matches('.');
  if host.is_empty() {
    return None;
  }
  if let Ok(v4) = host.parse::<Ipv4Addr>() {
    return Some(v4.to_string());
  }
  let addrs = (host, 0_u16).to_socket_addrs().ok()?;
  for addr in addrs {
    if let IpAddr::V4(v4) = addr.ip() {
      return Some(v4.to_string());
    }
  }
  None
}

/// Local advertiseable IPv4 addresses, preferred interface first (`en0` before `en7`/bridges).
fn preferred_local_ipv4s() -> Vec<Ipv4Addr> {
  let mut scored: Vec<(i32, Ipv4Addr)> = Vec::new();
  let Ok(ifaces) = get_if_addrs() else {
    return Vec::new();
  };
  for iface in ifaces {
    if iface.is_loopback() {
      continue;
    }
    let IfAddr::V4(v4) = iface.addr else {
      continue;
    };
    let ip = v4.ip;
    if !is_advertiseable_v4(ip) {
      continue;
    }
    // Skip link-local 169.254/16.
    if ip.octets()[0] == 169 && ip.octets()[1] == 254 {
      continue;
    }
    let score = iface_preference(&iface.name);
    if score < 0 {
      continue; // virtual / tunnel
    }
    scored.push((score, ip));
  }
  scored.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.octets().cmp(&b.1.octets())));
  let mut out = Vec::new();
  for (_, ip) in scored {
    if !out.contains(&ip) {
      out.push(ip);
    }
  }
  out
}

/// Lower is better. Negative = skip.
fn iface_preference(name: &str) -> i32 {
  if name == "en0" {
    return 0; // primary Wi‑Fi / Ethernet
  }
  if name.starts_with("en") {
    // en7 Thunderbolt/dock often shares 192.168.1.x poorly with Wi‑Fi Nest
    if name == "en7" || name == "en8" || name == "en9" {
      return 50;
    }
    return 10;
  }
  if name.starts_with("bridge") || name.starts_with("vmenet") || name.starts_with("utun") || name.starts_with("awdl") {
    return -1;
  }
  40
}

const fn same_class_c(a: Ipv4Addr, b: Ipv4Addr) -> bool {
  let ao = a.octets();
  let bo = b.octets();
  ao[0] == bo[0] && ao[1] == bo[1] && ao[2] == bo[2]
}

fn on_same_iface_subnet(local: Ipv4Addr, peer: Ipv4Addr) -> bool {
  let Ok(ifaces) = get_if_addrs() else {
    return false;
  };
  for iface in ifaces {
    let IfAddr::V4(v4) = iface.addr else {
      continue;
    };
    if v4.ip != local {
      continue;
    }
    let mask = v4.netmask;
    let a = u32::from(local) & u32::from(mask);
    let b = u32::from(peer) & u32::from(mask);
    return a == b;
  }
  false
}

/// Discover the outbound IPv4 via a UDP connect toward `dest` (no packet required).
fn udp_discover_ipv4(dest: Ipv4Addr, port: u16) -> Option<Ipv4Addr> {
  let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
  socket.connect((dest, port)).ok()?;
  match socket.local_addr().ok()?.ip() {
    IpAddr::V4(v4) if is_advertiseable_v4(v4) => Some(v4),
    IpAddr::V4(_) | IpAddr::V6(_) => None,
  }
}

/// True when `ip` is a reasonable LAN-facing address for Cast pull.
const fn is_advertiseable_v4(ip: Ipv4Addr) -> bool {
  !ip.is_unspecified() && !ip.is_loopback() && !ip.is_multicast() && !ip.is_broadcast()
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn advertise_host_ip_returns_non_empty() {
    let ip = advertise_host_ip();
    assert!(!ip.is_empty(), "advertise_host_ip must return a non-empty string");
    let parsed: Ipv4Addr = ip.parse().expect("IPv4 dotted-quad");
    assert!(!parsed.is_unspecified());
  }

  #[test]
  fn loopback_and_unspecified_not_advertiseable() {
    assert!(!is_advertiseable_v4(Ipv4Addr::LOCALHOST));
    assert!(!is_advertiseable_v4(Ipv4Addr::UNSPECIFIED));
    assert!(is_advertiseable_v4(Ipv4Addr::new(192, 168, 1, 10)));
  }

  #[test]
  fn advertise_host_for_peer_loopback_falls_back() {
    let ip = advertise_host_for_peer("127.0.0.1");
    let _parsed: Ipv4Addr = ip.parse().expect("IPv4");
  }

  #[test]
  fn cast_connect_hosts_dedupes_and_includes_hostname() {
    let hosts = cast_connect_hosts("192.168.1.10", Some("speaker.local."));
    assert!(hosts.iter().any(|h| h == "192.168.1.10"));
    assert!(hosts.iter().any(|h| h == "speaker.local"));
  }

  #[test]
  fn iface_preference_ranks_en0_best() {
    assert!(iface_preference("en0") < iface_preference("en7"));
    assert!(iface_preference("bridge100") < 0);
  }

  #[test]
  fn preferred_local_ipv4s_does_not_panic() {
    drop(preferred_local_ipv4s());
  }
}

#[cfg(test)]
mod live_iface_tests {
  use super::*;
  #[test]
  fn peer_on_home_lan_prefers_en0_when_present() {
    let ip = advertise_host_for_peer("192.168.1.171");
    println!("advertise_host_for_peer(192.168.1.171) = {ip}");
    // On this machine en0 is .254; if present it must win over en7 .168.
    let locals = preferred_local_ipv4s();
    println!("preferred_local_ipv4s = {locals:?}");
    if locals.iter().any(|a| a.octets() == [192, 168, 1, 254]) {
      assert_eq!(ip, "192.168.1.254");
    }
  }
}
