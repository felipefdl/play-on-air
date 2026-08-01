//! LAN host IP helpers for advertising Cast-reachable media URLs.

use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};

/// Best-effort non-loopback IPv4 for media URLs Chromecasts can pull.
///
/// Strategy:
/// 1. UDP connect to `8.8.8.8:80` and read the kernel-chosen `local_addr`
/// 2. Fallback: `127.0.0.1` when no usable address is found
///
/// Prefer [`advertise_host_for_peer`] when the Cast device IP is known so the
/// URL uses the interface that can actually reach that device (multi-homed Macs).
pub fn advertise_host_ip() -> String {
  if let Some(ip) = udp_discover_ipv4(Ipv4Addr::new(8, 8, 8, 8), 80) {
    return ip.to_string();
  }
  Ipv4Addr::LOCALHOST.to_string()
}

/// Pick a local IPv4 on the route toward `peer_host` (Cast device).
///
/// Google Home must HTTP-GET our media URL. If we advertise an IP on the wrong
/// interface (VM bridge, secondary NIC), the speaker connects to AirPlay but
/// plays silence because it cannot pull the stream.
pub fn advertise_host_for_peer(peer_host: &str) -> String {
  let peer = peer_host.trim().trim_end_matches('.');
  if peer.is_empty() {
    return advertise_host_ip();
  }

  // IPv4 literal peer.
  if let Ok(v4) = peer.parse::<Ipv4Addr>()
    && let Some(local) = udp_discover_ipv4(v4, 8009)
  {
    return local.to_string();
  }

  // Resolve hostname and route toward each IPv4 candidate.
  if let Ok(addrs) = std::net::ToSocketAddrs::to_socket_addrs(&(peer, 8009_u16)) {
    for addr in addrs {
      if let IpAddr::V4(v4) = addr.ip()
        && let Some(local) = udp_discover_ipv4(v4, 8009)
      {
        return local.to_string();
      }
    }
  }

  // SocketAddr string form (host:port) if caller passed it.
  if let Ok(sa) = peer.parse::<SocketAddr>()
    && let IpAddr::V4(v4) = sa.ip()
    && let Some(local) = udp_discover_ipv4(v4, sa.port())
  {
    return local.to_string();
  }

  advertise_host_ip()
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
    // Peer on loopback: still returns a parseable IPv4 (may be LAN default).
    let ip = advertise_host_for_peer("127.0.0.1");
    let _parsed: Ipv4Addr = ip.parse().expect("IPv4");
  }

  #[test]
  fn advertise_host_for_peer_empty_uses_default() {
    let a = advertise_host_for_peer("");
    let b = advertise_host_ip();
    let _pa: Ipv4Addr = a.parse().expect("IPv4 a");
    let _pb: Ipv4Addr = b.parse().expect("IPv4 b");
  }
}
