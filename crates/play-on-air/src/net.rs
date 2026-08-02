//! LAN host IP helpers for advertising Cast-reachable media URLs and Cast connect.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::sync::Once;
use std::time::Duration;

use if_addrs::{IfAddr, get_if_addrs};
use socket2::{Domain, Protocol, Socket, Type};

/// Per-candidate timeout for source-bound Cast TCP connect.
const CAST_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// How long the localhost relay waits for `rust_cast` to dial in.
const CAST_RELAY_ACCEPT_TIMEOUT: Duration = Duration::from_secs(5);
/// Short TCP SYN used only to warm ARP / routes during wake.
const CAST_WAKE_TCP_TIMEOUT: Duration = Duration::from_millis(250);

/// Start a process-global AP2 PTP drain on UDP 319/320 (once).
///
/// shairplay also tries to bind these ports on every `RaopServer::start`. Only
/// the first bind wins; the rest log EADDRINUSE. Binding early means every
/// virtual speaker shares one host sink so iOS multi-select PTP traffic is
/// accepted (avoids `ICMPv6` port-unreachable stalls).
pub fn ensure_global_ptp_sink() {
  static START: Once = Once::new();
  START.call_once(|| {
    let handle = std::thread::Builder::new().name("ap2-ptp-sink".into()).spawn(|| {
      let Ok(rt) = tokio::runtime::Builder::new_current_thread().enable_all().build() else {
        tracing::warn!("PTP sink: failed to build runtime");
        return;
      };
      rt.block_on(async {
        let mut bound = 0_u8;
        for port in [319_u16, 320_u16] {
          match tokio::net::UdpSocket::bind((std::net::Ipv6Addr::UNSPECIFIED, port)).await {
            Ok(sock) => {
              bound = bound.saturating_add(1);
              // Detached for the process lifetime; do not join.
              drop(tokio::spawn(async move {
                let mut buf = [0_u8; 1024];
                loop {
                  match sock.recv_from(&mut buf).await {
                    Ok(_) => {},
                    Err(err) => {
                      tracing::debug!(port, error = %err, "PTP sink recv ended");
                      break;
                    },
                  }
                }
              }));
            },
            Err(err) => {
              tracing::warn!(
                port,
                error = %err,
                "global PTP sink bind failed (may need CAP_NET_BIND_SERVICE)"
              );
            },
          }
        }
        if bound > 0 {
          tracing::info!(ports = bound, "global AP2 PTP sink active on 319/320 (shared by all receivers)");
        }
        // Keep the runtime alive for the recv tasks.
        std::future::pending::<()>().await;
      });
    });
    if let Err(err) = handle {
      tracing::warn!(error = %err, "PTP sink thread spawn failed");
    }
  });
}

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
  // TCP SYN from each preferred local IP so ARP is populated on the right iface
  // (unbound probes only use the default route, which may be the wrong NIC).
  if let Some(ip_s) = resolve_host_ipv4(target)
    && let Ok(ip) = ip_s.parse::<Ipv4Addr>()
  {
    let dest = SocketAddr::from((ip, 8009));
    for local in preferred_local_ipv4s() {
      if let Ok(stream) = tcp_connect_from(Some(local), dest, CAST_WAKE_TCP_TIMEOUT) {
        drop(stream);
      }
    }
    if let Ok(stream) = tcp_connect_from(None, dest, CAST_WAKE_TCP_TIMEOUT) {
      drop(stream);
    }
  }
}

/// Connect to Cast `dest_host:dest_port` trying each preferred local IPv4 as source bind.
///
/// Multi-homed hosts (e.g. Wi‑Fi `en0` + USB LAN `en7` on the same /24) often
/// have a default route on the wrong NIC while AirPlay is active. Binding the
/// source address forces the kernel onto a working path without needing root
/// route changes. Returns the connected stream and the local IPv4 used.
pub fn tcp_connect_cast_bound(dest_host: &str, dest_port: u16) -> io::Result<(TcpStream, Ipv4Addr)> {
  let dest_ip = resolve_dest_ipv4(dest_host)?;
  let dest = SocketAddr::from((dest_ip, dest_port));

  // Loopback destinations (unit tests): bind to 127.0.0.1, never a LAN NIC.
  if dest_ip.is_loopback() {
    match tcp_connect_from(Some(Ipv4Addr::LOCALHOST), dest, CAST_CONNECT_TIMEOUT) {
      Ok(stream) => {
        tracing::info!(local = %Ipv4Addr::LOCALHOST, %dest, "Cast TCP loopback connect ok");
        return Ok((stream, Ipv4Addr::LOCALHOST));
      },
      Err(err) => {
        tracing::warn!(%dest, error = %err, "Cast TCP loopback connect failed; trying unbound");
      },
    }
    let stream = tcp_connect_from(None, dest, CAST_CONNECT_TIMEOUT)?;
    return Ok((stream, Ipv4Addr::LOCALHOST));
  }

  let mut last_err: Option<io::Error> = None;
  for local in preferred_local_ipv4s() {
    match tcp_connect_from(Some(local), dest, CAST_CONNECT_TIMEOUT) {
      Ok(stream) => {
        tracing::info!(%local, %dest, "Cast TCP bound connect ok");
        return Ok((stream, local));
      },
      Err(err) => {
        tracing::warn!(%local, %dest, error = %err, "Cast TCP bound connect failed");
        last_err = Some(err);
      },
    }
  }

  match tcp_connect_from(None, dest, CAST_CONNECT_TIMEOUT) {
    Ok(stream) => {
      let local = local_ipv4_of(&stream).unwrap_or(Ipv4Addr::UNSPECIFIED);
      tracing::info!(%local, %dest, "Cast TCP unbound connect ok");
      Ok((stream, local))
    },
    Err(err) => {
      tracing::warn!(%dest, error = %err, "Cast TCP unbound connect failed");
      Err(last_err.unwrap_or(err))
    },
  }
}

/// Pre-dial Nest with multi-source bind, then expose a one-shot localhost TCP relay
/// so `rust_cast` (which has no source-bind API) only dials `127.0.0.1`.
///
/// Preferred flow: remote is already connected before the listener is published,
/// so `rust_cast` only dials after the Nest path is known good.
pub fn spawn_cast_connect_relay(dest_host: &str, dest_port: u16) -> io::Result<(String, u16)> {
  let (remote, local_src) = tcp_connect_cast_bound(dest_host, dest_port)?;
  tracing::info!(
    dest_host,
    dest_port,
    %local_src,
    "Cast control-plane pre-connected via source-bound TCP"
  );

  let std_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
  let port = std_listener.local_addr()?.port();
  // socket2 exposes accept timeout; std::net::TcpListener does not.
  let listener = Socket::from(std_listener);
  listener.set_read_timeout(Some(CAST_RELAY_ACCEPT_TIMEOUT))?;

  let dest_label = dest_host.to_owned();
  drop(
    std::thread::Builder::new()
      .name("cast-tcp-relay".to_owned())
      .spawn(move || match listener.accept() {
        Ok((client_sock, _)) => {
          let client: TcpStream = client_sock.into();
          bridge_tcp_streams(client, remote);
        },
        Err(err) => {
          tracing::warn!(
            dest = %dest_label,
            dest_port,
            error = %err,
            "Cast localhost relay accept failed (rust_cast never dialed in)"
          );
          drop(remote);
        },
      })?,
  );

  Ok((Ipv4Addr::LOCALHOST.to_string(), port))
}

/// Try TCP connect to `host:port` from each preferred local IPv4 and log results.
///
/// Used after Cast connect failures so the next log capture shows which interface
/// still works while AirPlay is active.
pub fn probe_cast_reachability(host: &str, port: u16) {
  let target = host.trim().trim_end_matches('.');
  if target.is_empty() {
    return;
  }
  let Ok(dest_ip) = resolve_dest_ipv4(target) else {
    tracing::warn!(%target, port, "Cast reachability probe: no IPv4 for host");
    return;
  };
  let dest = SocketAddr::from((dest_ip, port));
  for local in preferred_local_ipv4s() {
    match tcp_connect_from(Some(local), dest, CAST_CONNECT_TIMEOUT) {
      Ok(stream) => {
        tracing::info!(%local, %dest, "Cast reachability probe: bound ok");
        drop(stream);
      },
      Err(err) => {
        tracing::warn!(%local, %dest, error = %err, "Cast reachability probe: bound failed");
      },
    }
  }
  match tcp_connect_from(None, dest, CAST_CONNECT_TIMEOUT) {
    Ok(stream) => {
      let local = local_ipv4_of(&stream).unwrap_or(Ipv4Addr::UNSPECIFIED);
      tracing::info!(%local, %dest, "Cast reachability probe: unbound ok");
      drop(stream);
    },
    Err(err) => {
      tracing::warn!(%dest, error = %err, "Cast reachability probe: unbound failed");
    },
  }
}

fn resolve_dest_ipv4(host: &str) -> io::Result<Ipv4Addr> {
  let ip_s = resolve_host_ipv4(host)
    .ok_or_else(|| io::Error::new(io::ErrorKind::AddrNotAvailable, format!("no IPv4 address for Cast host {host}")))?;
  ip_s.parse::<Ipv4Addr>().map_err(|parse_err| {
    io::Error::new(
      io::ErrorKind::InvalidInput,
      format!("invalid IPv4 for Cast host {host}: {ip_s} ({parse_err})"),
    )
  })
}

fn tcp_connect_from(local: Option<Ipv4Addr>, dest: SocketAddr, timeout: Duration) -> io::Result<TcpStream> {
  let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
  if let Some(ip) = local {
    socket.bind(&SocketAddr::from((ip, 0)).into())?;
  }
  socket.connect_timeout(&dest.into(), timeout)?;
  // `connect_timeout` may leave the socket non-blocking on some platforms.
  socket.set_nonblocking(false)?;
  Ok(socket.into())
}

fn local_ipv4_of(stream: &TcpStream) -> Option<Ipv4Addr> {
  match stream.local_addr().ok()?.ip() {
    IpAddr::V4(v4) => Some(v4),
    IpAddr::V6(_) => None,
  }
}

/// Bidirectional byte bridge until either side closes.
fn bridge_tcp_streams(client: TcpStream, remote: TcpStream) {
  let Ok(mut client_read) = client.try_clone() else {
    tracing::warn!("Cast relay: failed to clone client stream");
    return;
  };
  let Ok(mut remote_read) = remote.try_clone() else {
    tracing::warn!("Cast relay: failed to clone remote stream");
    return;
  };
  let mut client_write = client;
  let mut remote_write = remote;

  let up = std::thread::spawn(move || {
    drop(io::copy(&mut client_read, &mut remote_write));
    drop(remote_write.shutdown(Shutdown::Write));
  });
  drop(io::copy(&mut remote_read, &mut client_write));
  drop(client_write.shutdown(Shutdown::Write));
  drop(up.join());
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

/// Force-close open TCP sockets bound to `local_port` (accepted RTSP conns).
///
/// shairplay's `RaopServer::stop` only stops the accept loop; live connection tasks
/// keep TCP streams open, so iOS stays in Now Playing after a kick. On Linux we use
/// `ss -K` (iproute2) — no `unsafe` (workspace `unsafe_code = forbid`).
///
/// No-op on non-Linux hosts. Requires `ss` in PATH (HA image installs `iproute2`).
pub fn force_close_tcp_on_local_port(local_port: u16) {
  #[cfg(target_os = "linux")]
  force_close_tcp_on_local_port_linux(local_port);
  #[cfg(not(target_os = "linux"))]
  {
    tracing::trace!(local_port, "force_close: no-op on this OS");
  }
}

/// Kill sockets with local port `local_port` via `ss -K` (safe, no unsafe).
#[cfg(target_os = "linux")]
fn force_close_tcp_on_local_port_linux(local_port: u16) {
  // Filter: any socket whose source port is our RAOP listen/accept port.
  let filter = format!("sport = :{local_port}");
  match std::process::Command::new("ss").args(["-K", filter.as_str()]).output() {
    Ok(out) => {
      let stdout = String::from_utf8_lossy(&out.stdout);
      let stderr = String::from_utf8_lossy(&out.stderr);
      if out.status.success() {
        // ss -K prints closed sockets; count non-empty lines as a rough signal.
        let lines = stdout.lines().filter(|l| !l.trim().is_empty()).count();
        tracing::info!(local_port, lines, "force-closed TCP sockets on AirPlay port via ss -K (kick)");
      } else {
        tracing::warn!(
          local_port,
          status = ?out.status,
          stdout = %stdout.trim(),
          stderr = %stderr.trim(),
          "ss -K failed while kicking AirPlay sockets"
        );
      }
    },
    Err(err) => {
      tracing::warn!(
        local_port,
        error = %err,
        "ss not available for RTSP kick; install iproute2 (HA image should include it)"
      );
    },
  }
}

#[cfg(test)]
mod tests {
  use std::io::{Read, Write};

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

  #[test]
  fn tcp_connect_cast_bound_to_local_listener() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind listener");
    let port = listener.local_addr().expect("local_addr").port();
    let accept = std::thread::spawn(move || {
      let (stream, _) = listener.accept().expect("accept");
      drop(stream);
    });

    let (stream, local) = tcp_connect_cast_bound("127.0.0.1", port).expect("bound connect to 127.0.0.1");
    assert!(local.is_loopback(), "local source should be loopback, got {local}");
    drop(stream);
    accept.join().expect("accept thread");
  }

  #[test]
  fn cast_connect_relay_forwards_bytes_both_ways() {
    // Echo server on an ephemeral port (stands in for Nest).
    let echo = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind echo");
    let echo_port = echo.local_addr().expect("echo addr").port();
    let echo_thread = std::thread::spawn(move || {
      let (mut sock, _) = echo.accept().expect("echo accept");
      let mut buf = [0_u8; 64];
      let n = sock.read(&mut buf).expect("echo read");
      sock.write_all(&buf[..n]).expect("echo write");
      drop(sock.shutdown(Shutdown::Both));
    });

    let (relay_host, relay_port) = spawn_cast_connect_relay("127.0.0.1", echo_port).expect("spawn relay");
    assert_eq!(relay_host, "127.0.0.1");

    let mut client = TcpStream::connect((relay_host.as_str(), relay_port)).expect("dial relay");
    client.set_read_timeout(Some(Duration::from_secs(2))).expect("read timeout");
    client.write_all(b"ping-cast").expect("client write");
    let mut got = vec![0_u8; 9];
    client.read_exact(&mut got).expect("client read");
    assert_eq!(&got, b"ping-cast");
    drop(client);
    echo_thread.join().expect("echo thread");
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
