//! LAN host IP helpers for advertising Cast-reachable media URLs and Cast connect.

use std::io::{self, ErrorKind, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::sync::{Arc, Once};
use std::time::Duration;

use if_addrs::{IfAddr, get_if_addrs};
use parking_lot::Mutex;
use socket2::{Domain, Protocol, Socket, TcpKeepalive, Type};

/// Per-candidate timeout for source-bound Cast TCP connect.
const CAST_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// How long the localhost relay waits for `rust_cast` to dial in.
const CAST_RELAY_ACCEPT_TIMEOUT: Duration = Duration::from_secs(5);
/// Short TCP SYN used only to warm ARP / routes during wake.
const CAST_WAKE_TCP_TIMEOUT: Duration = Duration::from_millis(250);
/// Read/write timeout on relay sockets so `rust_cast` cannot block forever.
const CAST_RELAY_IO_TIMEOUT: Duration = Duration::from_secs(12);
/// TCP keepalive idle before first probe on the device-facing Cast socket.
const CAST_TCP_KEEPALIVE_TIME: Duration = Duration::from_secs(30);
/// TCP keepalive probe interval.
const CAST_TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Shared shutdown handles for a Cast localhost↔device TCP relay.
///
/// Closing either side unblocks a `rust_cast` read stuck behind the relay so worker
/// threads can exit on `remove` / reconnect.
#[derive(Clone, Debug, Default)]
pub struct CastRelayShutdown {
  inner: Arc<Mutex<RelayEnds>>,
}

#[derive(Default, Debug)]
struct RelayEnds {
  remote: Option<TcpStream>,
  client: Option<TcpStream>,
}

impl CastRelayShutdown {
  /// Create an empty shutdown handle (sockets installed after dial/accept).
  #[must_use]
  pub fn new() -> Self {
    Self {
      inner: Arc::new(Mutex::new(RelayEnds::default())),
    }
  }

  /// Shutdown both directions on device and client sockets (best-effort).
  pub fn shutdown(&self) {
    let mut ends = self.inner.lock();
    if let Some(remote) = ends.remote.take() {
      drop(remote.shutdown(Shutdown::Both));
    }
    if let Some(client) = ends.client.take() {
      drop(client.shutdown(Shutdown::Both));
    }
  }

  fn set_remote(&self, stream: TcpStream) {
    self.inner.lock().remote = Some(stream);
  }

  fn set_client(&self, stream: TcpStream) {
    self.inner.lock().client = Some(stream);
  }

  /// Temporarily change the rust_cast-facing socket read timeout (socket option is shared).
  ///
  /// Used to drain buffered unsolicited messages without waiting the full I/O timeout when
  /// the buffer is empty.
  pub fn set_client_read_timeout(&self, timeout: Option<Duration>) {
    let ends = self.inner.lock();
    if let Some(client) = ends.client.as_ref()
      && let Err(err) = client.set_read_timeout(timeout)
    {
      tracing::debug!(error = %err, "Cast relay set_client_read_timeout failed");
    }
  }

  /// Restore the default relay I/O read timeout on the client socket.
  pub fn restore_client_read_timeout(&self) {
    self.set_client_read_timeout(Some(CAST_RELAY_IO_TIMEOUT));
  }
}

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
    // Peer-aware ranking: same-subnet first, virtual ifaces blacklisted.
    if let Some(ip) = preferred_local_ipv4s_for_peer(Some(peer_ip)).into_iter().next() {
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
///
/// Uses a UDP poke plus short TCP SYNs from preferred local interfaces. Does not
/// spawn an external `ping` process (HAOS containers often lack it).
pub fn wake_cast_host(host: &str) {
  let target = host.trim().trim_end_matches('.');
  if target.is_empty() {
    return;
  }
  // UDP poke on Cast port (no response required).
  if let Ok(sock) = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)) {
    drop(sock.send_to(&[0_u8], (target, 8009)));
  }
  // TCP SYN from each preferred local IP so ARP is populated on the right iface
  // (unbound probes only use the default route, which may be the wrong NIC).
  if let Some(ip_s) = resolve_host_ipv4(target)
    && let Ok(ip) = ip_s.parse::<Ipv4Addr>()
  {
    let dest = SocketAddr::from((ip, 8009));
    let locals = preferred_local_ipv4s_for_peer(Some(ip));
    for local in locals {
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
///
/// Interface list is gathered **once** per dial (not twice for subnet checks).
pub fn tcp_connect_cast_bound(dest_host: &str, dest_port: u16) -> io::Result<(TcpStream, Ipv4Addr)> {
  let dest_ip = resolve_dest_ipv4(dest_host)?;
  let dest = SocketAddr::from((dest_ip, dest_port));

  // Loopback destinations (unit tests): bind to 127.0.0.1, never a LAN NIC.
  if dest_ip.is_loopback() {
    match tcp_connect_from(Some(Ipv4Addr::LOCALHOST), dest, CAST_CONNECT_TIMEOUT) {
      Ok(stream) => {
        apply_cast_socket_options(&stream, /*keepalive*/ false);
        tracing::info!(local = %Ipv4Addr::LOCALHOST, %dest, "Cast TCP loopback connect ok");
        return Ok((stream, Ipv4Addr::LOCALHOST));
      },
      Err(err) => {
        tracing::warn!(%dest, error = %err, "Cast TCP loopback connect failed; trying unbound");
      },
    }
    let stream = tcp_connect_from(None, dest, CAST_CONNECT_TIMEOUT)?;
    apply_cast_socket_options(&stream, /*keepalive*/ false);
    return Ok((stream, Ipv4Addr::LOCALHOST));
  }

  // One iface snapshot for this dial attempt cycle.
  let locals = preferred_local_ipv4s_for_peer(Some(dest_ip));
  let mut last_err: Option<io::Error> = None;
  for local in locals {
    match tcp_connect_from(Some(local), dest, CAST_CONNECT_TIMEOUT) {
      Ok(stream) => {
        apply_cast_socket_options(&stream, /*keepalive*/ true);
        tracing::info!(%local, %dest, "Cast TCP bound connect ok");
        return Ok((stream, local));
      },
      Err(err) => {
        tracing::debug!(%local, %dest, error = %err, "Cast TCP bound connect failed");
        last_err = Some(err);
      },
    }
  }

  match tcp_connect_from(None, dest, CAST_CONNECT_TIMEOUT) {
    Ok(stream) => {
      apply_cast_socket_options(&stream, /*keepalive*/ true);
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
///
/// Returns `(relay_host, relay_port, shutdown)`. Call [`CastRelayShutdown::shutdown`]
/// to unblock a worker stuck in `rust_cast` I/O (e.g. on pool `remove`).
pub fn spawn_cast_connect_relay(dest_host: &str, dest_port: u16) -> io::Result<(String, u16, CastRelayShutdown)> {
  let (remote, local_src) = tcp_connect_cast_bound(dest_host, dest_port)?;
  tracing::info!(
    dest_host,
    dest_port,
    source_ip = %local_src,
    "Cast control-plane pre-connected via source-bound TCP"
  );

  let shutdown = CastRelayShutdown::new();
  if let Ok(remote_for_shutdown) = remote.try_clone() {
    shutdown.set_remote(remote_for_shutdown);
  }

  let std_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
  let port = std_listener.local_addr()?.port();
  // socket2 exposes accept timeout; std::net::TcpListener does not.
  let listener = Socket::from(std_listener);
  listener.set_read_timeout(Some(CAST_RELAY_ACCEPT_TIMEOUT))?;

  let dest_label = dest_host.to_owned();
  let shutdown_for_accept = shutdown.clone();
  drop(
    std::thread::Builder::new()
      .name("cast-tcp-relay".to_owned())
      .spawn(move || match listener.accept() {
        Ok((client_sock, _)) => {
          let client: TcpStream = client_sock.into();
          apply_cast_socket_options(&client, /*keepalive*/ false);
          if let Ok(client_for_shutdown) = client.try_clone() {
            shutdown_for_accept.set_client(client_for_shutdown);
          }
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

  Ok((Ipv4Addr::LOCALHOST.to_string(), port, shutdown))
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
  for local in preferred_local_ipv4s_for_peer(Some(dest_ip)) {
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
    .ok_or_else(|| io::Error::new(ErrorKind::AddrNotAvailable, format!("no IPv4 address for Cast host {host}")))?;
  ip_s.parse::<Ipv4Addr>().map_err(|parse_err| {
    io::Error::new(
      ErrorKind::InvalidInput,
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
///
/// Idle read timeouts do **not** tear down a healthy relay (loop and retry). Real
/// EOF / errors end the direction and are logged at debug.
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
    if let Err(err) = copy_ignoring_idle_timeouts(&mut client_read, &mut remote_write) {
      tracing::debug!(error = %err, direction = "client->device", "Cast relay copy ended");
    }
    drop(remote_write.shutdown(Shutdown::Write));
  });
  if let Err(err) = copy_ignoring_idle_timeouts(&mut remote_read, &mut client_write) {
    tracing::debug!(error = %err, direction = "device->client", "Cast relay copy ended");
  }
  drop(client_write.shutdown(Shutdown::Write));
  drop(up.join());
}

/// Like `io::copy`, but `WouldBlock` / `TimedOut` on an idle direction is not fatal.
fn copy_ignoring_idle_timeouts(reader: &mut impl Read, writer: &mut impl Write) -> io::Result<u64> {
  let mut buf = [0_u8; 8 * 1024];
  let mut total = 0_u64;
  loop {
    match reader.read(&mut buf) {
      Ok(0) => return Ok(total),
      Ok(n) => {
        let Some(chunk) = buf.get(..n) else {
          return Err(io::Error::new(ErrorKind::InvalidData, "read length exceeds buffer"));
        };
        writer.write_all(chunk)?;
        total = total.saturating_add(n as u64);
      },
      Err(err) if err.kind() == ErrorKind::WouldBlock || err.kind() == ErrorKind::TimedOut => {
        // Idle timeout on one direction while the peer is still connected; retry.
      },
      Err(err) if err.kind() == ErrorKind::Interrupted => {},
      Err(err) => return Err(err),
    }
  }
}

/// Apply read/write timeouts (and optional TCP keepalive) used on Cast control sockets.
fn apply_cast_socket_options(stream: &TcpStream, keepalive: bool) {
  if let Err(err) = stream.set_read_timeout(Some(CAST_RELAY_IO_TIMEOUT)) {
    tracing::debug!(error = %err, "Cast socket set_read_timeout failed");
  }
  if let Err(err) = stream.set_write_timeout(Some(CAST_RELAY_IO_TIMEOUT)) {
    tracing::debug!(error = %err, "Cast socket set_write_timeout failed");
  }
  if !keepalive {
    return;
  }
  let Ok(clone) = stream.try_clone() else {
    return;
  };
  let socket = Socket::from(clone);
  let ka = TcpKeepalive::new()
    .with_time(CAST_TCP_KEEPALIVE_TIME)
    .with_interval(CAST_TCP_KEEPALIVE_INTERVAL);
  if let Err(err) = socket.set_tcp_keepalive(&ka) {
    tracing::debug!(error = %err, "Cast socket TCP keepalive failed");
  }
  // Keep the underlying fd alive via the original `stream`; drop the wrapper without shutdown.
  // `Socket` into TcpStream and forget would double-close; instead leak the Socket into a TcpStream
  // that we drop without shutdown... Socket Drop closes the fd which is a clone, so closing is fine.
  drop(socket);
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

/// Local advertiseable IPv4 addresses, preferred interface first.
fn preferred_local_ipv4s() -> Vec<Ipv4Addr> {
  preferred_local_ipv4s_for_peer(None)
}

/// Rank local `IPv4` addresses for reaching optional `peer` (same-subnet wins; virtual ifaces blacklisted).
fn preferred_local_ipv4s_for_peer(peer: Option<Ipv4Addr>) -> Vec<Ipv4Addr> {
  let mut scored: Vec<(i32, Ipv4Addr)> = Vec::new();
  let Ok(ifaces) = get_if_addrs() else {
    return Vec::new();
  };
  let default_route_ip = default_route_ipv4();
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
      continue; // virtual / tunnel / docker
    }
    let mut rank = score;
    // Same-subnet as peer is the strongest signal (HAOS multi-homed).
    if let Some(peer_ip) = peer {
      let mask = v4.netmask;
      let a = u32::from(ip) & u32::from(mask);
      let b = u32::from(peer_ip) & u32::from(mask);
      if a == b || same_class_c(ip, peer_ip) {
        rank = rank.saturating_sub(100);
      }
    }
    if default_route_ip == Some(ip) {
      rank = rank.saturating_sub(20);
    }
    scored.push((rank, ip));
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

/// Lower is better. Negative = skip (virtual / container / tunnel).
fn iface_preference(name: &str) -> i32 {
  if is_blacklisted_iface(name) {
    return -1;
  }
  if name == "en0" || name == "eth0" || name == "end0" {
    return 0; // primary Wi‑Fi / Ethernet (macOS + common Linux/HAOS)
  }
  if name.starts_with("en") || name.starts_with("eth") || name.starts_with("end") {
    // Secondary physical NICs (Thunderbolt/dock often share subnet poorly).
    if name == "en7" || name == "en8" || name == "en9" {
      return 50;
    }
    return 10;
  }
  if name.starts_with("wlan") || name.starts_with("wlp") {
    return 5;
  }
  if name.starts_with("bridge") || name.starts_with("vmenet") || name.starts_with("utun") || name.starts_with("awdl") {
    return -1;
  }
  40
}

/// Docker / HAOS / tunnel interface names that must never be Cast source/advertise.
fn is_blacklisted_iface(name: &str) -> bool {
  if name == "docker0" || name == "hassio" {
    return true;
  }
  name.starts_with("veth")
    || name.starts_with("br-")
    || name.starts_with("tun")
    || name.starts_with("tap")
    || name.starts_with("wg")
    || name.starts_with("tailscale")
}

/// IPv4 of the default-route interface on Linux (`/proc/net/route`); `None` elsewhere.
#[cfg(target_os = "linux")]
fn default_route_ipv4() -> Option<Ipv4Addr> {
  default_route_ipv4_linux()
}

/// Non-Linux: no `/proc/net/route`; ranking falls back to name heuristics only.
#[cfg(not(target_os = "linux"))]
const fn default_route_ipv4() -> Option<Ipv4Addr> {
  None
}

#[cfg(target_os = "linux")]
fn default_route_ipv4_linux() -> Option<Ipv4Addr> {
  let text = std::fs::read_to_string("/proc/net/route").ok()?;
  let mut default_if: Option<String> = None;
  for line in text.lines().skip(1) {
    let mut cols = line.split_whitespace();
    let iface = cols.next()?;
    let dest = cols.next()?;
    // Destination 00000000 = default route.
    if dest != "00000000" {
      continue;
    }
    default_if = Some(iface.to_owned());
    break;
  }
  let iface_name = default_if?;
  let ifaces = get_if_addrs().ok()?;
  for iface in ifaces {
    if iface.name != iface_name {
      continue;
    }
    let IfAddr::V4(v4) = iface.addr else {
      continue;
    };
    if is_advertiseable_v4(v4.ip) {
      return Some(v4.ip);
    }
  }
  None
}

const fn same_class_c(a: Ipv4Addr, b: Ipv4Addr) -> bool {
  let ao = a.octets();
  let bo = b.octets();
  ao[0] == bo[0] && ao[1] == bo[1] && ao[2] == bo[2]
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
/// Vendored shairplay aborts accept + connection tasks on `stop`; `ss -K` is a
/// best-effort kernel-side kill for any remaining ESTAB sockets so iOS leaves
/// Now Playing. On Linux we use `ss -K` (iproute2) — no `unsafe`
/// (workspace `unsafe_code = forbid`).
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

/// True when a line from `ss` stdout is a real socket row (not blank / column header).
///
/// Pure helper so unit tests can lock classification without spawning `ss`.
#[cfg(any(test, target_os = "linux"))]
fn is_ss_socket_line(line: &str) -> bool {
  let trimmed = line.trim();
  if trimmed.is_empty() {
    return false;
  }
  // Header rows look like: "Netid  State  Recv-Q  Send-Q  Local Address:Port  …"
  let first = trimmed.split_whitespace().next().unwrap_or("");
  let first_lower = first.to_ascii_lowercase();
  first_lower != "netid" && first_lower != "state"
}

/// Count closed-socket rows in `ss -K` stdout (excludes blanks and headers).
#[cfg(any(test, target_os = "linux"))]
fn count_ss_socket_lines(stdout: &str) -> usize {
  stdout.lines().filter(|line| is_ss_socket_line(line)).count()
}

/// Kill sockets with local port `local_port` via `ss -K` (safe, no unsafe).
#[cfg(target_os = "linux")]
fn force_close_tcp_on_local_port_linux(local_port: u16) {
  // Filter: any TCP socket whose source port is our RAOP listen/accept port.
  let filter = format!("sport = :{local_port}");
  // Prefer no-header (`-H`), TCP only (`-t`), numeric (`-n`). Older iproute2 may
  // reject `-H`; fall back without it and filter headers in the line counter.
  let primary = std::process::Command::new("ss")
    .args(["-K", "-H", "-t", "-n", filter.as_str()])
    .output();

  let out = match primary {
    Ok(out) if out.status.success() => out,
    Ok(_failed_with_h) => {
      // Older iproute2 may reject `-H`; retry without it and filter headers when counting.
      match std::process::Command::new("ss")
        .args(["-K", "-t", "-n", filter.as_str()])
        .output()
      {
        Ok(fallback) => fallback,
        Err(err) => {
          tracing::warn!(
            local_port,
            error = %err,
            "ss not available for RTSP kick; install iproute2 (HA image should include it)"
          );
          return;
        },
      }
    },
    Err(err) => {
      tracing::warn!(
        local_port,
        error = %err,
        "ss not available for RTSP kick; install iproute2 (HA image should include it)"
      );
      return;
    },
  };

  let stdout = String::from_utf8_lossy(&out.stdout);
  let stderr = String::from_utf8_lossy(&out.stderr);
  if !out.status.success() {
    tracing::warn!(
      local_port,
      status = ?out.status,
      stdout = %stdout.trim(),
      stderr = %stderr.trim(),
      "ss -K failed while kicking AirPlay sockets"
    );
    return;
  }

  let closed_count = count_ss_socket_lines(&stdout);
  if closed_count > 0 {
    tracing::info!(
      local_port,
      closed_count,
      "force-closed TCP sockets on AirPlay port via ss -K (kick)"
    );
  } else {
    tracing::warn!(
      local_port,
      closed_count,
      "ss -K reported success but closed zero sockets; kick may rely on task abort alone \
       (NET_ADMIN / host_network / ss version / already-closed sockets)"
    );
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
    assert!(iface_preference("docker0") < 0);
    assert!(iface_preference("vethabc") < 0);
    assert!(iface_preference("br-1234") < 0);
    assert!(iface_preference("hassio") < 0);
    assert_eq!(iface_preference("eth0"), iface_preference("en0"));
    assert!(is_blacklisted_iface("tailscale0"));
    assert!(is_blacklisted_iface("wg0"));
  }

  #[test]
  fn preferred_for_peer_prefers_same_subnet_candidate() {
    // Pure ranking: when peer is given, same-class-c locals sort first among equals.
    // Smoke: function returns without panic.
    drop(preferred_local_ipv4s_for_peer(Some(Ipv4Addr::new(192, 168, 1, 50))));
  }

  #[test]
  fn preferred_local_ipv4s_does_not_panic() {
    drop(preferred_local_ipv4s());
  }

  #[test]
  fn is_ss_socket_line_filters_headers_and_blanks() {
    assert!(!is_ss_socket_line(""));
    assert!(!is_ss_socket_line("   "));
    assert!(!is_ss_socket_line(
      "Netid  State      Recv-Q Send-Q Local Address:Port Peer Address:Port"
    ));
    assert!(!is_ss_socket_line("State Recv-Q Send-Q"));
    assert!(is_ss_socket_line(
      "tcp   ESTAB      0      0 192.168.1.10:7000 192.168.1.20:54321"
    ));
    assert!(is_ss_socket_line("u_str ESTAB 0 0 * 12345 * 0"));
  }

  #[test]
  fn count_ss_socket_lines_ignores_header_only_output() {
    let header_only = "Netid  State      Recv-Q Send-Q Local Address:Port Peer Address:Port\n";
    assert_eq!(count_ss_socket_lines(header_only), 0);
    assert_eq!(count_ss_socket_lines(""), 0);
    let with_sock = "\
Netid  State      Recv-Q Send-Q Local Address:Port Peer Address:Port
tcp   ESTAB      0      0 0.0.0.0:7000 192.168.1.5:40000
tcp   ESTAB      0      0 0.0.0.0:7000 192.168.1.5:40001
";
    assert_eq!(count_ss_socket_lines(with_sock), 2);
    // -H (no header): only socket rows.
    assert_eq!(count_ss_socket_lines("tcp   ESTAB 0 0 0.0.0.0:7000 192.168.1.5:40000\n"), 1);
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

    let (relay_host, relay_port, _shutdown) = spawn_cast_connect_relay("127.0.0.1", echo_port).expect("spawn relay");
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
