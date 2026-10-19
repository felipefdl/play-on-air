//! Passive PTP follower for AirPlay 2 buffered audio.
//!
//! AirPlay 2 senders express every `SETRATEANCHORTIME` anchor on a PTP timeline: whole seconds
//! plus a 2^-64 fraction, tagged with the clock identity of the grandmaster (the
//! `networkTimeTimelineID`). To turn that anchor into a local instant the receiver must know the
//! offset between the grandmaster's clock and its own monotonic clock.
//!
//! This module listens on the PTP event (319) and general (320) ports and follows every
//! grandmaster it hears, keyed by clock identity:
//!
//! - two-step `Sync` (event port) records the local receive instant; the matching `Follow_Up`
//!   (general port, same sequence id) carries the precise origin timestamp;
//! - one-step `Sync` carries the origin timestamp itself;
//! - each pair yields one raw offset sample `master_time - local_receive_time`.
//!
//! Network queuing only ever delays a packet, which makes a raw sample *smaller* than the true
//! offset, so the filter keeps the maximum of a short sliding window (minimum-delay filter). A
//! clock step is accepted only after several consecutive samples agree on it, so one badly
//! delayed packet cannot move the estimate. The follower never transmits: it does not take part
//! in master election (spec section 3) and does not answer `Delay_Req`.
//!
//! Lock: a timeline is locked once [`LOCK_MIN_SAMPLES`] samples are in the filter and the newest
//! is younger than [`LOCK_STALE_NS`]. Only timelines whose last sender address is in the
//! session's peer set (SETUP `timingPeerInfo`, SETPEERS / SETPEERSX) are used for that session.
//! A fresh peer timeline with fewer samples is *locking* (it locks within about half a second
//! at 8 Hz Sync): an anchor on it is deferred until lock rather than falling back at once
//! ([`AnchorMapping::Locking`]).
//!
//! Sockets are bound once per process for a given fixed port pair and shared by every
//! [`crate::RaopServer`] that asks for it, so several advertised receivers in one process do not
//! fight over 319/320. Bind failure (another PTP stack such as macOS ControlCenter owns the
//! ports, or no privilege for ports below 1024) is logged once and leaves playback on the
//! unlocked anchor fallback in [`crate::raop::buffered_audio`].

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::util::mono_now_ns;

/// Default PTP event port (Sync, Delay_Req).
pub(crate) const DEFAULT_PTP_EVENT_PORT: u16 = 319;
/// Default PTP general port (Follow_Up, Announce).
pub(crate) const DEFAULT_PTP_GENERAL_PORT: u16 = 320;

/// Samples kept by the minimum-delay filter (about 2 s at the 8 Hz Sync rate Apple uses).
const FILTER_WINDOW: usize = 16;
/// Samples required before a timeline counts as locked.
const LOCK_MIN_SAMPLES: u32 = 4;
/// A timeline with no sample for this long is unlocked.
const LOCK_STALE_NS: u64 = 3_000_000_000;
/// Deviation from the estimate that marks a sample as a step candidate.
const STEP_THRESHOLD_NS: i64 = 50_000_000;
/// Consecutive step candidates that confirm a clock step (window resets to the new value).
const STEP_CONFIRM: u8 = 3;
/// Two-step Sync messages remembered while waiting for their Follow_Up.
const PENDING_SYNCS: usize = 16;
/// Timelines tracked at once; the stalest is evicted beyond this.
const MAX_TIMELINES: usize = 8;
/// Receive timeout so worker threads notice shutdown even when no wake datagram arrives.
const RECV_TIMEOUT: Duration = Duration::from_millis(250);

/// PTP common header length (IEEE 1588-2008 section 13.3).
const PTP_HEADER_LEN: usize = 34;
/// Header plus one 10-byte timestamp (Sync / Follow_Up body).
const PTP_TIMESTAMP_MSG_LEN: usize = PTP_HEADER_LEN + 10;
const MSG_SYNC: u8 = 0x0;
const MSG_FOLLOW_UP: u8 = 0x8;
/// `twoStepFlag` in the first flag octet.
const FLAG_TWO_STEP: u8 = 0x02;

/// Convert an AirPlay network time (`networkTimeSecs`, `networkTimeFrac`) to nanoseconds.
///
/// Matches the reference receiver: only the top 32 bits of the fraction are used.
pub(crate) fn network_time_ns(secs: u64, frac: u64) -> u64 {
    let frac_ns = ((frac >> 32) * 1_000_000_000) >> 32;
    secs.saturating_mul(1_000_000_000).saturating_add(frac_ns)
}

/// Sliding-window minimum-delay offset filter with step confirmation.
#[derive(Debug, Clone)]
struct OffsetFilter {
    window: [i64; FILTER_WINDOW],
    len: usize,
    next: usize,
    /// Consecutive samples that disagreed with the estimate by more than the step threshold.
    step_candidates: u8,
    /// Samples accepted since the last reset.
    samples: u32,
}

impl OffsetFilter {
    const fn new() -> Self {
        Self {
            window: [0; FILTER_WINDOW],
            len: 0,
            next: 0,
            step_candidates: 0,
            samples: 0,
        }
    }

    /// Current estimate: the largest (least delayed) sample in the window.
    fn estimate(&self) -> Option<i64> {
        self.window.iter().take(self.len).copied().max()
    }

    fn push(&mut self, sample: i64) {
        if let Some(estimate) = self.estimate() {
            if sample.abs_diff(estimate) > STEP_THRESHOLD_NS.unsigned_abs() {
                self.step_candidates = self.step_candidates.saturating_add(1);
                if self.step_candidates < STEP_CONFIRM {
                    return;
                }
                tracing::debug!(estimate, sample, "PTP offset step confirmed; filter reset");
                *self = Self::new();
            } else {
                self.step_candidates = 0;
            }
        }
        if let Some(slot) = self.window.get_mut(self.next) {
            *slot = sample;
        }
        self.next = (self.next + 1) % FILTER_WINDOW;
        self.len = (self.len + 1).min(FILTER_WINDOW);
        self.samples = self.samples.saturating_add(1);
    }
}

#[derive(Debug, Clone)]
struct Timeline {
    clock_id: u64,
    /// Sender address of the newest sample (canonical: IPv4-mapped IPv6 folded to IPv4).
    source: IpAddr,
    filter: OffsetFilter,
    /// Local monotonic time of the newest accepted sample.
    last_sample_ns: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct PendingSync {
    clock_id: u64,
    seq: u16,
    rx_ns: u64,
    correction_ns: i64,
    valid: bool,
}

#[derive(Debug)]
struct ClockState {
    timelines: Vec<Timeline>,
    pending: [PendingSync; PENDING_SYNCS],
    pending_next: usize,
}

/// Parsed fields of a Sync or Follow_Up message.
struct TimestampMessage {
    msg_type: u8,
    two_step: bool,
    correction_ns: i64,
    clock_id: u64,
    seq: u16,
    timestamp_ns: i64,
}

fn parse_timestamp_message(buf: &[u8]) -> Option<TimestampMessage> {
    if buf.len() < PTP_TIMESTAMP_MSG_LEN {
        return None;
    }
    let msg_type = buf.first()? & 0x0F;
    if msg_type != MSG_SYNC && msg_type != MSG_FOLLOW_UP {
        return None;
    }
    let two_step = buf.get(6)? & FLAG_TWO_STEP != 0;
    // correctionField: nanoseconds scaled by 2^16.
    let correction_ns = i64::from_be_bytes(buf.get(8..16)?.try_into().ok()?) >> 16;
    let clock_id = u64::from_be_bytes(buf.get(20..28)?.try_into().ok()?);
    let seq = u16::from_be_bytes(buf.get(30..32)?.try_into().ok()?);
    let secs_hi = u64::from(u16::from_be_bytes(buf.get(34..36)?.try_into().ok()?));
    let secs_lo = u64::from(u32::from_be_bytes(buf.get(36..40)?.try_into().ok()?));
    let nanos = i128::from(u32::from_be_bytes(buf.get(40..44)?.try_into().ok()?));
    let secs = i128::from((secs_hi << 32) | secs_lo);
    let timestamp_ns = i64::try_from(secs * 1_000_000_000 + nanos).ok()?;
    Some(TimestampMessage {
        msg_type,
        two_step,
        correction_ns,
        clock_id,
        seq,
        timestamp_ns,
    })
}

/// Grandmaster offsets learned from Sync / Follow_Up traffic, keyed by clock identity.
///
/// Cheap to clone (shared state). Times are local [`mono_now_ns`] nanoseconds.
#[derive(Clone, Debug)]
pub(crate) struct PtpClock {
    state: Arc<Mutex<ClockState>>,
}

/// What the follower can do with a network time on one timeline, for one session, now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimelineMapping {
    /// Locked: the network time maps to this local monotonic instant (ns).
    Locked(u64),
    /// A session-peer timeline with fresh samples, not yet enough of them to lock.
    Locking,
    /// Unknown timeline, not a session peer, or stale: no lock is coming soon.
    Unavailable,
}

impl PtpClock {
    /// An empty clock that knows no timeline yet.
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ClockState {
                timelines: Vec::with_capacity(MAX_TIMELINES),
                pending: [PendingSync::default(); PENDING_SYNCS],
                pending_next: 0,
            })),
        }
    }

    /// Feed one received PTP datagram (either port) received from `source` at local `rx_ns`.
    pub(crate) fn handle_packet(&self, buf: &[u8], source: IpAddr, rx_ns: u64) {
        let Some(msg) = parse_timestamp_message(buf) else {
            return;
        };
        let source = source.to_canonical();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match msg.msg_type {
            MSG_SYNC if msg.two_step => {
                let slot_index = state.pending_next;
                if let Some(slot) = state.pending.get_mut(slot_index) {
                    *slot = PendingSync {
                        clock_id: msg.clock_id,
                        seq: msg.seq,
                        rx_ns,
                        correction_ns: msg.correction_ns,
                        valid: true,
                    };
                }
                state.pending_next = (slot_index + 1) % PENDING_SYNCS;
            }
            MSG_SYNC => {
                let master_ns = msg.timestamp_ns.saturating_add(msg.correction_ns);
                state.add_sample(msg.clock_id, source, master_ns, rx_ns);
            }
            _ => {
                let Some(sync) = state
                    .pending
                    .iter_mut()
                    .find(|p| p.valid && p.clock_id == msg.clock_id && p.seq == msg.seq)
                else {
                    return;
                };
                sync.valid = false;
                let (sync_rx_ns, sync_correction_ns) = (sync.rx_ns, sync.correction_ns);
                let master_ns = msg
                    .timestamp_ns
                    .saturating_add(msg.correction_ns)
                    .saturating_add(sync_correction_ns);
                state.add_sample(msg.clock_id, source, master_ns, sync_rx_ns);
            }
        }
    }

    /// Map a network time on `timeline_id` to a local monotonic instant (ns).
    ///
    /// Locked when the timeline has [`LOCK_MIN_SAMPLES`] samples, its newest sample is fresh at
    /// `now_ns`, and its sender address is one of `peers`; locking when only the sample count
    /// is missing.
    pub(crate) fn map(&self, timeline_id: u64, network_ns: u64, peers: &[IpAddr], now_ns: u64) -> TimelineMapping {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(timeline) = state.timelines.iter().find(|t| t.clock_id == timeline_id) else {
            return TimelineMapping::Unavailable;
        };
        if !peers.contains(&timeline.source) || now_ns.saturating_sub(timeline.last_sample_ns) > LOCK_STALE_NS {
            return TimelineMapping::Unavailable;
        }
        if timeline.filter.samples < LOCK_MIN_SAMPLES {
            return TimelineMapping::Locking;
        }
        timeline
            .filter
            .estimate()
            .and_then(|offset| u64::try_from(i128::from(network_ns) - i128::from(offset)).ok())
            .map_or(TimelineMapping::Unavailable, TimelineMapping::Locked)
    }

    /// [`Self::map`] reduced to the locked instant.
    #[cfg(test)]
    pub(crate) fn to_local_ns(&self, timeline_id: u64, network_ns: u64, peers: &[IpAddr], now_ns: u64) -> Option<u64> {
        match self.map(timeline_id, network_ns, peers, now_ns) {
            TimelineMapping::Locked(local) => Some(local),
            TimelineMapping::Locking | TimelineMapping::Unavailable => None,
        }
    }

    /// Inject one offset sample (`master_ns` observed at local `rx_ns`) as a Sync pair would.
    #[cfg(test)]
    pub(crate) fn add_sample(&self, clock_id: u64, source: IpAddr, master_ns: i64, rx_ns: u64) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .add_sample(clock_id, source, master_ns, rx_ns);
    }
}

/// A session's view of the PTP follower (clock plus the peer set at anchor time), carried by a
/// deferred anchor so the delivery thread can map it once the timeline locks.
#[derive(Clone, Debug)]
pub(crate) struct PtpMapper {
    clock: PtpClock,
    peers: Vec<IpAddr>,
}

impl PtpMapper {
    pub(crate) const fn new(clock: PtpClock, peers: Vec<IpAddr>) -> Self {
        Self { clock, peers }
    }

    /// [`PtpClock::map`] for this session's peers.
    pub(crate) fn map(&self, timeline_id: u64, network_ns: u64, now_ns: u64) -> TimelineMapping {
        self.clock.map(timeline_id, network_ns, &self.peers, now_ns)
    }
}

/// How a `SETRATEANCHORTIME` network time maps for a session.
#[derive(Debug)]
pub(crate) enum AnchorMapping {
    /// Mapped to this local monotonic instant (ns).
    Locked(u64),
    /// The timeline is locking: map later through this mapper.
    Locking(PtpMapper),
    /// No PTP mapping (not PTP timing, no follower, unknown, foreign or stale timeline).
    Unavailable,
}

impl ClockState {
    fn add_sample(&mut self, clock_id: u64, source: IpAddr, master_ns: i64, rx_ns: u64) {
        let Ok(rx) = i64::try_from(rx_ns) else {
            return;
        };
        let offset = master_ns.saturating_sub(rx);
        if let Some(timeline) = self.timelines.iter_mut().find(|t| t.clock_id == clock_id) {
            timeline.source = source;
            timeline.filter.push(offset);
            timeline.last_sample_ns = rx_ns;
            return;
        }
        if self.timelines.len() >= MAX_TIMELINES
            && let Some(stalest) = self
                .timelines
                .iter()
                .enumerate()
                .min_by_key(|(_, t)| t.last_sample_ns)
                .map(|(i, _)| i)
        {
            self.timelines.swap_remove(stalest);
        }
        let mut filter = OffsetFilter::new();
        filter.push(offset);
        tracing::info!(
            clock_id = format_args!("{clock_id:016x}"),
            %source,
            "PTP: following new grandmaster timeline"
        );
        self.timelines.push(Timeline {
            clock_id,
            source,
            filter,
            last_sample_ns: rx_ns,
        });
    }
}

/// One bound PTP socket and the thread draining it.
struct Worker {
    port: u16,
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

/// Bound PTP sockets feeding one [`PtpClock`]. Dropping the last handle closes the sockets.
pub(crate) struct PtpService {
    clock: PtpClock,
    ports: Option<(u16, u16)>,
    workers: Vec<Worker>,
}

impl PtpService {
    /// The clock fed by this service.
    pub(crate) fn clock(&self) -> &PtpClock {
        &self.clock
    }

    /// Bound `(event, general)` ports, or `None` when binding failed.
    pub(crate) const fn ports(&self) -> Option<(u16, u16)> {
        self.ports
    }

    fn bind(event_port: u16, general_port: u16) -> Self {
        let clock = PtpClock::new();
        let sockets = bind_socket(event_port).and_then(|event| Ok((event, bind_socket(general_port)?)));
        let (event, general) = match sockets {
            Ok(pair) => pair,
            Err(err) => {
                log_bind_failure(event_port, general_port, &err);
                return Self {
                    clock,
                    ports: None,
                    workers: Vec::new(),
                };
            }
        };
        let bound = (local_port(&event), local_port(&general));
        let mut workers = Vec::with_capacity(2);
        for socket in [event, general] {
            match spawn_worker(socket, clock.clone()) {
                Ok(worker) => workers.push(worker),
                Err(err) => {
                    tracing::warn!(error = %err, "PTP: worker thread spawn failed; unlocked anchor fallback");
                    return Self {
                        clock,
                        ports: None,
                        workers,
                    };
                }
            }
        }
        tracing::info!(event_port = bound.0, general_port = bound.1, "PTP follower listening");
        Self {
            clock,
            ports: Some(bound),
            workers,
        }
    }
}

impl Drop for PtpService {
    fn drop(&mut self) {
        for worker in &self.workers {
            worker.stop.store(true, Ordering::Release);
            wake_worker(worker.port);
        }
        for worker in &mut self.workers {
            if let Some(join) = worker.join.take()
                && join.join().is_err()
            {
                tracing::warn!(port = worker.port, "PTP worker thread panicked");
            }
        }
    }
}

fn bind_socket(port: u16) -> std::io::Result<UdpSocket> {
    // IPv6 unspecified also receives IPv4 (mapped) on the platforms PlayOnAir targets. Hosts
    // with IPv6 disabled fall back to IPv4 only; a taken or privileged port fails either way.
    let socket = UdpSocket::bind((Ipv6Addr::UNSPECIFIED, port)).or_else(|err| {
        if matches!(
            err.kind(),
            std::io::ErrorKind::AddrInUse | std::io::ErrorKind::PermissionDenied
        ) {
            Err(err)
        } else {
            UdpSocket::bind((Ipv4Addr::UNSPECIFIED, port))
        }
    })?;
    socket.set_read_timeout(Some(RECV_TIMEOUT))?;
    Ok(socket)
}

fn local_port(socket: &UdpSocket) -> u16 {
    socket.local_addr().map(|a| a.port()).unwrap_or(0)
}

fn spawn_worker(socket: UdpSocket, clock: PtpClock) -> std::io::Result<Worker> {
    let port = local_port(&socket);
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let join = std::thread::Builder::new()
        .name(format!("ptp-{port}"))
        .spawn(move || recv_loop(&socket, &clock, &thread_stop))?;
    Ok(Worker {
        port,
        stop,
        join: Some(join),
    })
}

fn recv_loop(socket: &UdpSocket, clock: &PtpClock, stop: &AtomicBool) {
    let mut buf = [0u8; 512];
    while !stop.load(Ordering::Acquire) {
        match socket.recv_from(&mut buf) {
            Ok((n, from)) => {
                if let Some(datagram) = buf.get(..n) {
                    clock.handle_packet(datagram, from.ip(), mono_now_ns());
                }
            }
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
            Err(err) => {
                tracing::warn!(error = %err, "PTP receive failed; follower thread exiting");
                break;
            }
        }
    }
}

/// Unblock a worker parked in `recv_from` so shutdown does not wait for the receive timeout.
fn wake_worker(port: u16) {
    let targets = [
        SocketAddr::from((Ipv6Addr::LOCALHOST, port)),
        SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
    ];
    for target in targets {
        let bind: SocketAddr = if target.is_ipv6() {
            (Ipv6Addr::LOCALHOST, 0).into()
        } else {
            (Ipv4Addr::LOCALHOST, 0).into()
        };
        if let Ok(socket) = UdpSocket::bind(bind)
            && socket.send_to(&[0], target).is_ok()
        {
            return;
        }
    }
}

fn log_bind_failure(event_port: u16, general_port: u16, err: &std::io::Error) {
    static WARNED: AtomicBool = AtomicBool::new(false);
    if WARNED.swap(true, Ordering::Relaxed) {
        tracing::debug!(event_port, general_port, error = %err, "PTP bind failed");
    } else {
        tracing::warn!(
            event_port,
            general_port,
            error = %err,
            "PTP: cannot bind ports (another PTP stack owns them, or ports <1024 need \
             CAP_NET_BIND_SERVICE); buffered audio uses the unlocked anchor fallback"
        );
    }
}

/// Process-wide services for fixed port pairs, so receivers in one process share 319/320.
static SHARED_SERVICES: Mutex<Vec<((u16, u16), Weak<PtpService>)>> = Mutex::new(Vec::new());

/// Get the PTP follower for `(event_port, general_port)`, binding it on first use.
///
/// Fixed port pairs are shared process-wide while any handle is alive. A port of `0` asks the
/// OS for an ephemeral port and always creates a private service (tests).
pub(crate) fn acquire(event_port: u16, general_port: u16) -> Arc<PtpService> {
    if event_port == 0 || general_port == 0 {
        return Arc::new(PtpService::bind(event_port, general_port));
    }
    let key = (event_port, general_port);
    let mut services = SHARED_SERVICES.lock().unwrap_or_else(PoisonError::into_inner);
    services.retain(|(_, weak)| weak.strong_count() > 0);
    if let Some(service) = services
        .iter()
        .find(|(k, _)| *k == key)
        .and_then(|(_, weak)| weak.upgrade())
    {
        return service;
    }
    let service = Arc::new(PtpService::bind(event_port, general_port));
    services.push((key, Arc::downgrade(&service)));
    service
}

/// Parse a peer address string from `timingPeerInfo` / SETPEERS (drops an IPv6 zone suffix).
fn parse_peer_address(text: &str) -> Option<IpAddr> {
    let host = text.split('%').next().unwrap_or(text).trim();
    host.parse::<IpAddr>().ok().map(|ip| ip.to_canonical())
}

/// Addresses from one peer dictionary (`Addresses` array of strings).
fn peer_dict_addresses(dict: &plist::Dictionary, out: &mut Vec<IpAddr>) {
    if let Some(addresses) = dict.get("Addresses").and_then(plist::Value::as_array) {
        out.extend(
            addresses
                .iter()
                .filter_map(|a| a.as_string())
                .filter_map(parse_peer_address),
        );
    }
}

/// Per-session PTP configuration from SETUP and SETPEERS / SETPEERSX.
#[derive(Debug, Default)]
pub(crate) struct SessionTiming {
    /// SETUP negotiated `timingProtocol` = `PTP`.
    ptp: bool,
    /// Sender clock addresses this session accepts timelines from.
    peers: Vec<IpAddr>,
}

impl SessionTiming {
    /// Record the SETUP `timingProtocol` and its peer info.
    ///
    /// `timingPeerInfo` (one peer dictionary) is added; `timingPeerList` (array of peer
    /// dictionaries) replaces the whole set.
    pub(crate) fn configure_from_setup(&mut self, setup: &plist::Dictionary) {
        self.ptp = setup.get("timingProtocol").and_then(plist::Value::as_string) == Some("PTP");
        if !self.ptp {
            return;
        }
        if let Some(list) = setup.get("timingPeerList").and_then(plist::Value::as_array) {
            self.replace_peers(list);
        }
        if let Some(info) = setup.get("timingPeerInfo").and_then(plist::Value::as_dictionary) {
            let mut added = Vec::new();
            peer_dict_addresses(info, &mut added);
            for ip in added {
                if !self.peers.contains(&ip) {
                    self.peers.push(ip);
                }
            }
        }
        tracing::debug!(peers = ?self.peers, "PTP session peers from SETUP");
    }

    /// Apply a SETPEERS (array of address strings) or SETPEERSX (array of peer dictionaries)
    /// body: the list is the complete peer set. Ignored unless the session negotiated PTP.
    pub(crate) fn replace_peers(&mut self, list: &[plist::Value]) {
        if !self.ptp {
            tracing::debug!("SETPEERS ignored: session timing is not PTP");
            return;
        }
        let mut peers = Vec::with_capacity(list.len());
        for entry in list {
            if let Some(text) = entry.as_string() {
                peers.extend(parse_peer_address(text));
            } else if let Some(dict) = entry.as_dictionary() {
                peer_dict_addresses(dict, &mut peers);
            }
        }
        peers.sort_unstable();
        peers.dedup();
        tracing::debug!(?peers, "PTP session peers replaced");
        self.peers = peers;
    }

    /// Map an anchor network time through `clock` for this session.
    pub(crate) fn map_anchor(
        &self,
        clock: Option<PtpClock>,
        timeline_id: u64,
        network_ns: u64,
        now_ns: u64,
    ) -> AnchorMapping {
        let Some(clock) = clock.filter(|_| self.ptp) else {
            return AnchorMapping::Unavailable;
        };
        match clock.map(timeline_id, network_ns, &self.peers, now_ns) {
            TimelineMapping::Locked(local) => AnchorMapping::Locked(local),
            TimelineMapping::Locking => AnchorMapping::Locking(PtpMapper::new(clock, self.peers.clone())),
            TimelineMapping::Unavailable => AnchorMapping::Unavailable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GM: u64 = 0x0011_22ff_fe33_4455;

    fn sender() -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20))
    }

    fn timestamp_msg(msg_type: u8, two_step: bool, clock_id: u64, seq: u16, ts_ns: u64) -> Vec<u8> {
        let mut buf = vec![0u8; 76];
        buf[0] = 0x10 | msg_type;
        buf[1] = 0x02;
        if two_step {
            buf[6] = FLAG_TWO_STEP;
        }
        buf[20..28].copy_from_slice(&clock_id.to_be_bytes());
        buf[30..32].copy_from_slice(&seq.to_be_bytes());
        let secs = ts_ns / 1_000_000_000;
        let nanos = (ts_ns % 1_000_000_000) as u32;
        buf[34..36].copy_from_slice(&((secs >> 32) as u16).to_be_bytes());
        buf[36..40].copy_from_slice(&(secs as u32).to_be_bytes());
        buf[40..44].copy_from_slice(&nanos.to_be_bytes());
        buf
    }

    /// Two-step Sync at local `rx_ns` followed by its Follow_Up carrying master time `master_ns`.
    fn feed_pair(clock: &PtpClock, seq: u16, master_ns: u64, rx_ns: u64) {
        clock.handle_packet(&timestamp_msg(MSG_SYNC, true, GM, seq, 0), sender(), rx_ns);
        clock.handle_packet(
            &timestamp_msg(MSG_FOLLOW_UP, false, GM, seq, master_ns),
            sender(),
            rx_ns + 1_000,
        );
    }

    #[test]
    fn network_time_uses_top_fraction_bits() {
        assert_eq!(network_time_ns(1, 0x8000_0000_0000_0000), 1_500_000_000);
        assert_eq!(network_time_ns(2, 0x4000_0000_ffff_ffff), 2_250_000_000);
    }

    /// Offset mapping: master runs 1000 s ahead of local; a network time maps back exactly once
    /// locked, and only for listed peers.
    #[test]
    fn locked_timeline_maps_network_time_to_local() {
        let clock = PtpClock::new();
        let offset = 1_000_000_000_000u64;
        let peers = [sender()];
        for i in 0..LOCK_MIN_SAMPLES as u16 {
            let rx = 5_000_000_000 + u64::from(i) * 125_000_000;
            assert_eq!(
                clock.to_local_ns(GM, rx + offset, &peers, rx),
                None,
                "locks only after enough samples"
            );
            feed_pair(&clock, i, rx + offset, rx);
        }
        let now = 5_400_000_000;
        assert_eq!(
            clock.to_local_ns(GM, offset + 7_000_000_000, &peers, now),
            Some(7_000_000_000)
        );
        assert_eq!(clock.to_local_ns(GM + 1, offset, &peers, now), None, "other timeline");
        let stranger = [IpAddr::V4(Ipv4Addr::new(10, 0, 0, 9))];
        assert_eq!(
            clock.to_local_ns(GM, offset, &stranger, now),
            None,
            "not a session peer"
        );
        assert_eq!(
            clock.to_local_ns(GM, offset, &peers, now + LOCK_STALE_NS + 1),
            None,
            "stale lock"
        );
    }

    /// A fresh session-peer timeline is locking until it has enough samples; unknown, foreign
    /// or stale timelines are unavailable (no wait is worth it); a non-PTP session never maps.
    #[test]
    fn timeline_reports_locking_before_lock() {
        let clock = PtpClock::new();
        let offset = 1_000_000_000_000u64;
        let peers = [sender()];
        assert_eq!(
            clock.map(GM, offset, &peers, 0),
            TimelineMapping::Unavailable,
            "unknown"
        );
        feed_pair(&clock, 0, 5_000_000_000 + offset, 5_000_000_000);
        assert_eq!(clock.map(GM, offset, &peers, 5_100_000_000), TimelineMapping::Locking);
        let stranger = [IpAddr::V4(Ipv4Addr::new(10, 0, 0, 9))];
        assert_eq!(
            clock.map(GM, offset, &stranger, 5_100_000_000),
            TimelineMapping::Unavailable,
            "not a session peer"
        );
        assert_eq!(
            clock.map(GM, offset, &peers, 5_000_000_000 + LOCK_STALE_NS + 1),
            TimelineMapping::Unavailable,
            "stale"
        );

        let mut setup = plist::Dictionary::new();
        setup.insert("timingProtocol".into(), "PTP".into());
        let mut timing = SessionTiming::default();
        timing.configure_from_setup(&setup);
        timing.replace_peers(&["192.168.1.20".into()]);
        let AnchorMapping::Locking(mapper) = timing.map_anchor(Some(clock.clone()), GM, offset, 5_100_000_000) else {
            panic!("session sees the timeline locking");
        };
        for seq in 1..LOCK_MIN_SAMPLES as u16 {
            let rx = 5_000_000_000 + u64::from(seq) * 125_000_000;
            feed_pair(&clock, seq, rx + offset, rx);
        }
        assert_eq!(
            mapper.map(GM, offset + 7_000_000_000, 5_500_000_000),
            TimelineMapping::Locked(7_000_000_000),
            "the deferred mapper sees the lock"
        );
        assert!(matches!(
            SessionTiming::default().map_anchor(Some(clock), GM, offset, 5_500_000_000),
            AnchorMapping::Unavailable
        ));
    }

    /// Queuing delay only lowers raw samples; the filter keeps the least-delayed one and ignores
    /// a single wild outlier.
    #[test]
    fn filter_keeps_min_delay_sample_and_rejects_single_outlier() {
        let clock = PtpClock::new();
        let offset = 50_000_000_000u64;
        let peers = [sender()];
        let delays = [300_000u64, 40_000, 2_000_000, 900_000, 120_000_000];
        for (i, delay) in delays.iter().enumerate() {
            let send = 1_000_000_000 + i as u64 * 125_000_000;
            feed_pair(&clock, i as u16, send + offset, send + delay);
        }
        // Estimate = offset - 40 µs (least delayed); the 120 ms outlier never entered the window.
        let local = clock.to_local_ns(GM, offset + 10_000_000_000, &peers, 2_000_000_000);
        assert_eq!(local, Some(10_000_040_000));
    }

    /// A real clock step is accepted after consecutive confirming samples.
    #[test]
    fn filter_accepts_confirmed_step() {
        let clock = PtpClock::new();
        let peers = [sender()];
        let mut seq = 0u16;
        for i in 0..6u64 {
            let rx = 1_000_000_000 + i * 125_000_000;
            feed_pair(&clock, seq, rx + 1_000_000_000, rx);
            seq += 1;
        }
        // Grandmaster steps 2 s forward.
        for i in 6..16u64 {
            let rx = 1_000_000_000 + i * 125_000_000;
            feed_pair(&clock, seq, rx + 3_000_000_000, rx);
            seq += 1;
        }
        assert_eq!(
            clock.to_local_ns(GM, 13_000_000_000, &peers, 3_000_000_000),
            Some(10_000_000_000)
        );
    }

    #[test]
    fn one_step_sync_is_a_sample() {
        let clock = PtpClock::new();
        let peers = [sender()];
        for i in 0..LOCK_MIN_SAMPLES as u16 {
            let rx = 1_000_000_000 + u64::from(i) * 125_000_000;
            clock.handle_packet(&timestamp_msg(MSG_SYNC, false, GM, i, rx + 500), sender(), rx);
        }
        assert_eq!(
            clock.to_local_ns(GM, 2_000_000_500, &peers, 1_500_000_000),
            Some(2_000_000_000)
        );
    }

    /// SETUP peer info adds, SETPEERS strings and SETPEERSX dictionaries replace, non-PTP ignores.
    #[test]
    fn session_peers_follow_setup_and_setpeers() {
        let mut setup = plist::Dictionary::new();
        setup.insert("timingProtocol".into(), "PTP".into());
        let mut info = plist::Dictionary::new();
        info.insert(
            "Addresses".into(),
            plist::Value::Array(vec!["fe80::1%en0".into(), "192.168.1.20".into()]),
        );
        setup.insert("timingPeerInfo".into(), plist::Value::Dictionary(info));
        let mut timing = SessionTiming::default();
        timing.configure_from_setup(&setup);
        assert_eq!(timing.peers.len(), 2);

        timing.replace_peers(&["192.168.1.30".into()]);
        assert_eq!(timing.peers, vec![IpAddr::V4(Ipv4Addr::new(192, 168, 1, 30))]);

        let mut dict = plist::Dictionary::new();
        dict.insert("ID".into(), "peer".into());
        dict.insert(
            "Addresses".into(),
            plist::Value::Array(vec!["::ffff:192.168.1.40".into()]),
        );
        timing.replace_peers(&[plist::Value::Dictionary(dict)]);
        assert_eq!(timing.peers, vec![IpAddr::V4(Ipv4Addr::new(192, 168, 1, 40))]);

        let mut ntp = SessionTiming::default();
        let mut ntp_setup = plist::Dictionary::new();
        ntp_setup.insert("timingProtocol".into(), "NTP".into());
        ntp.configure_from_setup(&ntp_setup);
        ntp.replace_peers(&["192.168.1.30".into()]);
        assert!(ntp.peers.is_empty());
    }

    /// End to end over real sockets on ephemeral ports: Sync + Follow_Up datagrams lock the
    /// clock; dropping the service frees the ports.
    #[test]
    fn service_on_ephemeral_ports_locks_from_datagrams() {
        let service = acquire(0, 0);
        let (event, general) = service.ports().expect("ephemeral bind");
        let tx = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("tx bind");
        let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let offset = 3_000_000_000_000u64;
        for seq in 0..6u16 {
            let master = mono_now_ns() + offset;
            tx.send_to(&timestamp_msg(MSG_SYNC, true, GM, seq, 0), (Ipv4Addr::LOCALHOST, event))
                .expect("sync");
            tx.send_to(
                &timestamp_msg(MSG_FOLLOW_UP, false, GM, seq, master),
                (Ipv4Addr::LOCALHOST, general),
            )
            .expect("follow_up");
            std::thread::sleep(Duration::from_millis(5));
        }
        let mut locked = None;
        for _ in 0..100 {
            let now = mono_now_ns();
            locked = service.clock().to_local_ns(GM, now + offset, &[loopback], now);
            if locked.is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let local = locked.expect("clock locks from loopback datagrams");
        let now = mono_now_ns();
        assert!(local.abs_diff(now) < 50_000_000, "mapped {local} vs now {now}");
        drop(service);
        UdpSocket::bind((Ipv6Addr::UNSPECIFIED, event)).expect("event port freed on drop");
    }
}
