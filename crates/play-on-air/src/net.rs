//! LAN host IP helpers for advertising Cast-reachable media URLs.

use std::net::{IpAddr, Ipv4Addr, UdpSocket};

/// Best-effort non-loopback IPv4 for media URLs Chromecasts can pull.
///
/// Strategy:
/// 1. UDP connect to `8.8.8.8:80` and read the kernel-chosen `local_addr`
/// 2. Fallback: `127.0.0.1` when no usable address is found
///
/// Does not require external interface crates. Never panics.
pub fn advertise_host_ip() -> String {
  if let Some(ip) = udp_discover_ipv4() {
    return ip.to_string();
  }
  Ipv4Addr::LOCALHOST.to_string()
}

/// Discover the outbound IPv4 via a UDP connect (no packets need to arrive).
fn udp_discover_ipv4() -> Option<Ipv4Addr> {
  let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
  // Destination only selects a route; nothing is sent until we write.
  socket.connect((Ipv4Addr::new(8, 8, 8, 8), 80)).ok()?;
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
    // Must parse as IPv4 (including loopback fallback).
    let parsed: Ipv4Addr = ip.parse().expect("IPv4 dotted-quad");
    assert!(!parsed.is_unspecified());
  }

  #[test]
  fn loopback_and_unspecified_not_advertiseable() {
    assert!(!is_advertiseable_v4(Ipv4Addr::LOCALHOST));
    assert!(!is_advertiseable_v4(Ipv4Addr::UNSPECIFIED));
    assert!(is_advertiseable_v4(Ipv4Addr::new(192, 168, 1, 10)));
  }
}
