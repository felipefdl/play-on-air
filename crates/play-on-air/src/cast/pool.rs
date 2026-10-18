//! Warm Cast control-plane pool.
//!
//! Each Chromecast gets one dedicated OS thread owning a live `rust_cast::CastDevice`
//! (not `Send` without the `thread_safe` feature). TCP is established while the device
//! is idle so AirPlay sessions only issue LOAD on the existing session — avoiding the
//! Nest `:8009` black-hole window where **new** connects fail with "No route to host".
//!
//! Resilience goals (this module + `crate::net`):
//! - bounded I/O (relay socket timeouts + hard op deadline)
//! - reconnect with exponential backoff and quiet logging
//! - deadline-based PING, drain unsolicited messages, answer device PINGs
//! - parse errors are not ownership theft; IDLE recover-then-kick policy

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex as StdMutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::mpsc::UnboundedSender;

use crate::error::{Error, Result};
use crate::net::CastRelayShutdown;
use crate::registry::Device;

use super::{
  ActiveCastSession, CastControl, CastTrack, MEDIA_NAMESPACE, MULTIZONE_NAMESPACE, MediaLoadRequest, MediaPlayback,
  PlaybackSessionUpdate, SetPlaybackDevices, media_session_from_status, parse_set_playback_devices_reply,
  volume_level_clamped,
};

/// Map Cast `Volume` from `get_status` to linear `0.0..=1.0` (muted → `0.0`).
fn volume_from_cast_status(volume: rust_cast::channels::receiver::Volume) -> f32 {
  if volume.muted.unwrap_or(false) {
    return 0.0;
  }
  volume_level_clamped(volume.level.unwrap_or(0.0))
}

/// A change on the Cast device that we did not command (touch controls, voice, Google Home app).
#[derive(Debug, Clone, PartialEq)]
pub struct CastRemoteEvent {
  /// Registry device id of the Cast receiver.
  pub device_id: String,
  /// What changed.
  pub kind: CastRemoteKind,
}

/// Kind of [`CastRemoteEvent`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CastRemoteKind {
  /// Our live media session went to PAUSED without a PAUSE from us.
  Paused,
  /// Our live media session went to PLAYING without a PLAY or LOAD from us.
  Played,
  /// Receiver volume or mute changed without a volume command from us.
  Volume {
    /// Receiver level `0.0..=1.0`.
    level: f32,
    /// Receiver mute flag.
    muted: bool,
  },
}

/// After our own PAUSE / PLAY / LOAD / volume command, statuses are not attributed to the
/// user for this long: the device can still broadcast a status from before our command.
const OWN_COMMAND_SETTLE: Duration = Duration::from_millis(1_500);

/// Volume differences below this are the same level (Cast reports floats with noise).
const VOLUME_MATCH_EPSILON: f32 = 0.005;

/// Player state we commanded last (only PLAYING / PAUSED are attributable).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlayerIntent {
  Playing,
  Paused,
}

/// Separates user-initiated Cast changes from echoes of our own commands.
///
/// Player state: only while our media session is live (an intent is set on LOAD / PLAY /
/// PAUSE and cleared when the session ends). Volume: always, against the last level we set
/// or observed. Within [`OWN_COMMAND_SETTLE`] of our own command, observations are ignored
/// entirely; a real user change in that window is still caught by the next status after it.
#[derive(Debug, Default)]
struct RemoteTracker {
  intent: Option<PlayerIntent>,
  player_settle_until: Option<Instant>,
  volume: Option<(f32, bool)>,
  volume_settle_until: Option<Instant>,
}

impl RemoteTracker {
  /// We are about to command `intent` (LOAD and PLAY → playing, PAUSE → paused).
  fn expect_player(&mut self, intent: PlayerIntent, now: Instant) {
    self.intent = Some(intent);
    self.player_settle_until = Some(now + OWN_COMMAND_SETTLE);
  }

  /// Our media session ended: nothing to attribute any more.
  const fn clear_player(&mut self) {
    self.intent = None;
    self.player_settle_until = None;
  }

  /// A status for our media session reported `state`.
  fn observe_player(&mut self, state: rust_cast::channels::media::PlayerState, now: Instant) -> Option<CastRemoteKind> {
    use rust_cast::channels::media::PlayerState;
    let observed = match state {
      PlayerState::Playing => PlayerIntent::Playing,
      PlayerState::Paused => PlayerIntent::Paused,
      PlayerState::Idle | PlayerState::Buffering => return None,
    };
    let expected = self.intent?;
    if self.player_settle_until.is_some_and(|until| now < until) || observed == expected {
      return None;
    }
    self.intent = Some(observed);
    Some(match observed {
      PlayerIntent::Playing => CastRemoteKind::Played,
      PlayerIntent::Paused => CastRemoteKind::Paused,
    })
  }

  /// We are about to set the receiver level.
  fn expect_volume_command(&mut self, now: Instant) {
    self.volume_settle_until = Some(now + OWN_COMMAND_SETTLE);
  }

  /// Level we set or read ourselves: the new baseline, never reported.
  const fn note_volume(&mut self, level: f32, muted: bool) {
    self.volume = Some((volume_level_clamped(level), muted));
  }

  /// A receiver status reported `level` / `muted`.
  fn observe_volume(&mut self, reported_level: f32, muted: bool, now: Instant) -> Option<CastRemoteKind> {
    if self.volume_settle_until.is_some_and(|until| now < until) {
      return None;
    }
    let level = volume_level_clamped(reported_level);
    let previous = self.volume.replace((level, muted))?;
    if (previous.0 - level).abs() < VOLUME_MATCH_EPSILON && previous.1 == muted {
      return None;
    }
    Some(CastRemoteKind::Volume { level, muted })
  }
}

/// Heartbeat / PING cadence on the warm control-plane connection.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(4);

/// Idle drain cadence while our media session is live, so touch / voice pause on the
/// device reaches the sender within half a second (ownership probes keep the heartbeat cadence).
const ACTIVE_DRAIN_INTERVAL: Duration = Duration::from_millis(500);

/// Wall-clock wait for load / stop / pause / play worker replies.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(6);

/// Wall-clock wait for volume get/set worker replies.
const VOLUME_TIMEOUT: Duration = Duration::from_secs(3);

/// Best-effort wait when joining a worker after `Shutdown`.
const JOIN_TIMEOUT: Duration = Duration::from_secs(3);

/// Skip ownership checks for this long after a successful LOAD (Cast settle time).
const OWNERSHIP_GRACE: Duration = Duration::from_secs(3);

/// Consecutive heartbeats that look stolen before declaring ownership lost.
const OWNERSHIP_LOSS_CONFIRMATIONS: u8 = 2;

/// Consecutive BUFFERING probes (~4 s each) before attempting internal re-LOAD.
const BUFFERING_STUCK_PROBES: u8 = 3;

/// Skip internal re-LOAD if a LOAD command arrived within this window (bridge rollover guard).
const RECENT_LOAD_GUARD: Duration = Duration::from_secs(10);

/// Hard deadline for any single blocking Cast op; forces relay shutdown + rebuild.
const WORKER_OP_DEADLINE: Duration = Duration::from_secs(30);

/// Initial reconnect backoff.
const RECONNECT_BACKOFF_START: Duration = Duration::from_secs(1);

/// Cap on reconnect backoff.
const RECONNECT_BACKOFF_CAP: Duration = Duration::from_secs(60);

/// Max unsolicited `receive()` calls drained per idle tick.
const UNSOLICITED_DRAIN_LIMIT: u32 = 8;

/// `SO_RCVTIMEO` for idle drain reads on the **real** `rust_cast` TCP socket (must be ≤100 ms).
///
/// Applied via a dual-owned `TcpStream` clone of the TLS underlying socket — not the
/// relay's peer end (that never affected `CastDevice::receive`).
const DRAIN_READ_TIMEOUT: Duration = Duration::from_millis(50);

/// Whether our Cast media transport is still listed among receiver applications.
#[must_use]
pub(crate) fn cast_transport_still_owned(applications_transport_ids: &[&str], ours: &str) -> bool {
  applications_transport_ids.contains(&ours)
}

/// Kind of failure when a status probe errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbeFailureKind {
  /// I/O, timeout, TLS — device may be unreachable; reconnect path.
  Transport,
  /// Deserialization / parsing — device answered; do not count as theft.
  Parse,
}

/// Classify a `rust_cast` error into transport vs parse for ownership probes.
#[must_use]
pub(crate) fn classify_cast_probe_error(err: &rust_cast::errors::Error) -> ProbeFailureKind {
  use rust_cast::errors::Error as CastErr;
  match err {
    // I/O, TLS, DNS, timeouts, and protobuf framing are transport-adjacent.
    CastErr::Io(_) | CastErr::Tls(_) | CastErr::Timeout(_) | CastErr::Dns(_) | CastErr::Protobuf(_) => {
      ProbeFailureKind::Transport
    },
    // Strict enum / field quirks: device answered; not theft.
    CastErr::Serialization(_) | CastErr::Parsing(_) | CastErr::Namespace(_) => ProbeFailureKind::Parse,
    CastErr::Internal(msg) => {
      let lower = msg.to_ascii_lowercase();
      if lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("broken pipe")
        || lower.contains("connection")
      {
        ProbeFailureKind::Transport
      } else {
        ProbeFailureKind::Parse
      }
    },
  }
}

/// Process-lifetime class of a Cast control-plane error for a given device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CastFailureClass {
  /// Retry with backoff; device may recover.
  Transient,
  /// Permanent for this process (e.g. no TLS version or cipher suite in common).
  Terminal,
}

/// Classify a pool-level Cast error string for terminal vs transient handling.
///
/// Terminal failures hide the AirPlay advertisement and stop reconnect hammering.
/// Only deterministic TLS negotiation mismatches are terminal; certificate quirks
/// (self-signed, X.509 v1) are accepted by the Cast verifier in `cast::tls`.
#[must_use]
pub(crate) fn classify_cast_control_error(err: &Error) -> CastFailureClass {
  classify_cast_control_message(&err.to_string())
}

/// Pure message classifier (unit-testable without building full [`Error`] variants).
#[must_use]
pub(crate) fn classify_cast_control_message(message: &str) -> CastFailureClass {
  // rustls `Error::PeerIncompatible`: `peer is incompatible: NoCipherSuitesInCommon` etc.
  // The device's TLS stack cannot negotiate with ours; retrying cannot change that.
  if message.contains("peer is incompatible") {
    CastFailureClass::Terminal
  } else {
    CastFailureClass::Transient
  }
}

/// After a successful wire LOAD, whether to install `active` or stop the late session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoadCommitDecision {
  /// Caller still waiting; publish session as active.
  InstallActive,
  /// Caller timed out / abandoned; stop media and do not keep `active`.
  AbandonAndStop,
}

/// Pure decision: late LOAD success must not install worker state if abandoned.
#[must_use]
pub(crate) const fn load_commit_decision(abandoned: bool) -> LoadCommitDecision {
  if abandoned {
    LoadCommitDecision::AbandonAndStop
  } else {
    LoadCommitDecision::InstallActive
  }
}

/// Far-future backoff so terminal devices do not reconnect every idle tick.
const TERMINAL_RECONNECT_HOLD: Duration = Duration::from_secs(24 * 60 * 60);

/// Result of one ownership / IDLE policy evaluation (pure; unit-tested).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OwnershipAction {
  /// Still our session; reset loss streak.
  Owned,
  /// Genuine steal / missing session / unrecoverable IDLE; count lost streak.
  LostSignal,
  /// Parse quirk; do not change loss streak.
  Inconclusive,
  /// Transport blip; reconnect, do not count as lost yet.
  SuspectReconnect,
  /// Recoverable IDLE / stuck buffering: attempt one internal re-LOAD.
  AttemptReload,
}

/// IDLE reason classes used by the pure ownership policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdleReasonKind {
  /// Another sender took the player — genuine steal.
  Interrupted,
  /// Recoverable player idle (error / finished / cancelled).
  Recoverable,
}

/// Pure inputs for one ownership evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(
  clippy::struct_excessive_bools,
  reason = "ownership fold is a flat pure-input record; bools map 1:1 to probe signals"
)]
pub(crate) struct OwnershipInputs {
  /// Our transport id appears in receiver applications.
  pub transport_listed: bool,
  /// Media status returned an entry for our session (`None` if media query failed).
  pub media_session_present: Option<bool>,
  /// When session present and IDLE, classified idle reason.
  pub idle_reason: Option<IdleReasonKind>,
  /// Session present and player is BUFFERING.
  pub buffering: bool,
  /// Consecutive BUFFERING probes observed so far (including this one if buffering).
  pub buffering_streak: u8,
  /// Media/receiver query failure classification.
  pub media_failure: Option<ProbeFailureKind>,
  /// We already tried one internal re-LOAD for this trouble episode.
  pub reload_attempted: bool,
  /// A LOAD command arrived within the rollover guard window.
  pub load_within_guard: bool,
}

/// Combine probe signals into a pure ownership action.
///
/// Branches (every path covered by unit tests):
/// - parse failure → [`OwnershipAction::Inconclusive`]
/// - transport failure → [`OwnershipAction::SuspectReconnect`]
/// - IDLE Interrupted → [`OwnershipAction::LostSignal`]
/// - IDLE recoverable / stuck BUFFERING → re-LOAD once, else lost
/// - session missing → lost; present/playing → owned
#[must_use]
pub(crate) fn ownership_action(inputs: OwnershipInputs) -> OwnershipAction {
  if let Some(fail) = inputs.media_failure {
    return match fail {
      ProbeFailureKind::Parse => OwnershipAction::Inconclusive,
      ProbeFailureKind::Transport => OwnershipAction::SuspectReconnect,
    };
  }

  if inputs.idle_reason == Some(IdleReasonKind::Interrupted) {
    return OwnershipAction::LostSignal;
  }

  if inputs.idle_reason == Some(IdleReasonKind::Recoverable) {
    return recoverable_or_lost(inputs.reload_attempted, inputs.load_within_guard);
  }

  if inputs.buffering && inputs.buffering_streak >= BUFFERING_STUCK_PROBES {
    return recoverable_or_lost(inputs.reload_attempted, inputs.load_within_guard);
  }

  if inputs.media_session_present == Some(false) {
    return OwnershipAction::LostSignal;
  }

  if inputs.media_session_present == Some(true) || inputs.transport_listed {
    return OwnershipAction::Owned;
  }

  if !inputs.transport_listed {
    return OwnershipAction::LostSignal;
  }

  OwnershipAction::Inconclusive
}

/// Recoverable IDLE / stuck buffering → re-LOAD, wait for bridge, or lost after a real attempt.
///
/// - Recent LOAD (bridge rollover): [`OwnershipAction::Inconclusive`] — do not fight the bridge
///   and do not count as steal.
/// - Already attempted one re-LOAD this episode: [`OwnershipAction::LostSignal`].
/// - Otherwise: [`OwnershipAction::AttemptReload`].
const fn recoverable_or_lost(reload_attempted: bool, load_within_guard: bool) -> OwnershipAction {
  if load_within_guard {
    OwnershipAction::Inconclusive
  } else if reload_attempted {
    OwnershipAction::LostSignal
  } else {
    OwnershipAction::AttemptReload
  }
}

/// Next reconnect delay: exponential from 1 s doubling to 60 s, ±20% jitter.
#[must_use]
pub(crate) fn reconnect_backoff_delay(attempt: u32, jitter_seed: u64) -> Duration {
  let exp = attempt.min(6); // 1,2,4,8,16,32,64 → cap 60
  let base_ms = RECONNECT_BACKOFF_START
    .as_millis()
    .saturating_mul(1_u128 << exp)
    .min(RECONNECT_BACKOFF_CAP.as_millis());
  // Jitter in [-20%, +20%] via deterministic mix (no `rand` dependency).
  let mix = jitter_seed
    .wrapping_mul(0x9E37_79B9_7F4A_7C15)
    .wrapping_add(u64::from(attempt).wrapping_mul(0x85EB_CA6B));
  let unit = mix % 1000; // 0..999
  // factor = 0.8 + unit/1000 * 0.4  →  800 + unit*0.4  (permille)
  let factor_permille = 800 + (unit * 400) / 1000;
  let jittered = base_ms.saturating_mul(u128::from(factor_permille)) / 1000;
  Duration::from_millis(u64::try_from(jittered).unwrap_or(u64::MAX).max(1))
}

/// Shared slot so the pool can force-close the active relay during `remove`.
type SharedRelaySlot = Arc<StdMutex<Option<CastRelayShutdown>>>;

/// Commands processed on a per-device Cast worker thread.
enum CastWorkerCmd {
  /// Update host / hostname / port (re-resolve path may change after mDNS refresh).
  UpdateEndpoint {
    host: String,
    hostname: String,
    port: u16,
  },
  /// Launch Default Media Receiver if needed, LOAD media, return session ids.
  ///
  /// `abandoned` is set by the caller when [`COMMAND_TIMEOUT`] elapses so a late
  /// success cannot install `active` (and must STOP the orphaned media session).
  Load {
    request: MediaLoadRequest,
    reply: SyncSender<Result<ActiveCastSession>>,
    abandoned: Arc<AtomicBool>,
  },
  /// Set receiver volume on the warm connection.
  SetVolume {
    level: f32,
    reply: SyncSender<Result<()>>,
  },
  /// Read receiver volume from `get_status` on the warm connection (linear `0.0..=1.0`).
  GetVolume { reply: SyncSender<Result<f32>> },
  /// Stop active media session (if any); keep TCP warm.
  Stop { reply: SyncSender<Result<()>> },
  /// Pause active media session (keep session id for later PLAY).
  ///
  /// `queued_at` is when the caller sent it, so the worker can log how long it waited.
  Pause {
    queued_at: Instant,
    reply: SyncSender<Result<MediaPlayback>>,
  },
  /// Resume a paused media session.
  Play { reply: SyncSender<Result<()>> },
  /// Read the active media session's `MEDIA_STATUS`.
  MediaStatus { reply: SyncSender<Result<MediaPlayback>> },
  /// Best-effort stop; always replies after attempt (or immediately if no session).
  StopBestEffort { reply: SyncSender<()> },
  /// Bridge-initiated session end: clear ownership tracking without Cast STOP.
  ///
  /// Lets probes stand down immediately so a missing media session is not declared
  /// as "another app took the receiver" after our own teardown.
  SessionEnded,
  /// Replace the now-playing metadata of the live media (no reply; latest wins).
  UpdateMetadata { track: CastTrack },
  /// Send multizone `SET_PLAYBACK_DEVICES` for the active session and return the answer.
  SetPlaybackDevices {
    request: SetPlaybackDevices,
    reply: SyncSender<Result<PlaybackSessionUpdate>>,
  },
  /// Release the active session (and its LOAD request) without a Cast STOP.
  TakeSession {
    reply: SyncSender<Option<(ActiveCastSession, Option<MediaLoadRequest>)>>,
  },
  /// Take over a session that already plays on this worker's host, as receiver app session
  /// `session_id` (its transport is looked up on the host).
  Adopt {
    session: ActiveCastSession,
    session_id: String,
    load: Option<MediaLoadRequest>,
    reply: SyncSender<Result<()>>,
  },
  /// Exit the worker loop and drop the Cast device.
  Shutdown,
}

/// App-facing notification channels handed to each worker thread.
struct WorkerNotify {
  ownership_lost: Option<UnboundedSender<String>>,
  media_recovered: Option<UnboundedSender<String>>,
  remote_events: Option<UnboundedSender<CastRemoteEvent>>,
}

/// Handle to a live worker (command channel + join handle).
struct CastWorkerHandle {
  cmd_tx: mpsc::Sender<CastWorkerCmd>,
  thread: Option<JoinHandle<()>>,
  host: String,
  hostname: String,
  port: u16,
  /// Active relay shutdown handle (updated by the worker on each connect).
  relay_slot: SharedRelaySlot,
  /// `remove` in progress (or waiting for late join). Blocks `ensure` re-spawn.
  exiting: bool,
  /// Shared with the worker thread: last known Cast control-plane reachability.
  reachable: Arc<AtomicBool>,
  /// Process-lifetime terminal Cast failure (e.g. TLS negotiation impossible).
  terminal_failed: Arc<AtomicBool>,
  /// Bridge-initiated end: probes stand down even before the worker drains cmds.
  ownership_stand_down: Arc<AtomicBool>,
  /// Multizone session host (not a registry speaker): excluded from [`CastPool::device_ids`].
  session_host: bool,
}

/// Shared pool of warm Cast control-plane workers (`Send + Sync` via `Arc`).
///
/// Workers are started when devices appear (see app maintain loop) and torn down
/// only when the device leaves — not when an AirPlay media session ends.
pub struct CastPool {
  workers: Arc<Mutex<HashMap<String, CastWorkerHandle>>>,
  /// Notifies the app when a warm LOAD session loses Cast media ownership
  /// (another app took the receiver). Payload is the registry device id.
  ownership_lost: Option<UnboundedSender<String>>,
  /// Notifies when an internal re-LOAD recovered media after recoverable IDLE.
  media_recovered: Option<UnboundedSender<String>>,
  /// Notifies user-initiated play / pause / volume changes on the Cast device.
  remote_events: Option<UnboundedSender<CastRemoteEvent>>,
}

impl Default for CastPool {
  fn default() -> Self {
    Self::new(None)
  }
}

impl std::fmt::Debug for CastPool {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("CastPool")
      .field("workers", &self.workers.lock().len())
      .field("ownership_watch", &self.ownership_lost.is_some())
      .field("media_recovered_watch", &self.media_recovered.is_some())
      .field("remote_events_watch", &self.remote_events.is_some())
      .finish()
  }
}

impl CastPool {
  /// Create an empty pool (no workers yet).
  ///
  /// When `ownership_lost` is set, workers notify it with the device id after
  /// confirmed Cast media ownership loss (another app took the receiver).
  #[must_use]
  pub fn new(ownership_lost: Option<UnboundedSender<String>>) -> Self {
    Self {
      workers: Arc::new(Mutex::new(HashMap::new())),
      ownership_lost,
      media_recovered: None,
      remote_events: None,
    }
  }

  /// Attach a channel for internal media recovery events (additive).
  #[must_use]
  pub fn with_media_recovered(mut self, media_recovered: UnboundedSender<String>) -> Self {
    self.media_recovered = Some(media_recovered);
    self
  }

  /// Attach a channel for user-initiated play / pause / volume changes on Cast devices.
  ///
  /// Workers report only changes they did not command themselves (see `RemoteTracker`).
  #[must_use]
  pub fn with_remote_events(mut self, remote_events: UnboundedSender<CastRemoteEvent>) -> Self {
    self.remote_events = Some(remote_events);
    self
  }

  /// Ensure a warm worker exists for `device` (start one if missing).
  ///
  /// Updates host / hostname / port when the registry entry changes.
  /// Does **not** spawn a second worker while one is still registered (including
  /// an exiting tombstone after join timeout).
  pub fn ensure(&self, device: &Device) {
    self.ensure_worker(&device.id, &device.host, &device.hostname, device.port, false);
  }

  /// Start the worker for `id`, or retarget it when its endpoint changed.
  fn ensure_worker(&self, id: &str, host: &str, hostname: &str, port: u16, session_host: bool) {
    let mut guard = self.workers.lock();
    if let Some(handle) = guard.get_mut(id) {
      // Tombstone from remove: worker may still be unwinding; never double-spawn.
      if !ensure_may_spawn_for_slot(handle.exiting) {
        drop(guard);
        return;
      }
      let host_changed = handle.host != host || handle.hostname != hostname || handle.port != port;
      if host_changed {
        host.clone_into(&mut handle.host);
        hostname.clone_into(&mut handle.hostname);
        handle.port = port;
        drop(handle.cmd_tx.send(CastWorkerCmd::UpdateEndpoint {
          host: host.to_owned(),
          hostname: hostname.to_owned(),
          port,
        }));
      }
      drop(guard);
      return;
    }

    let (cmd_tx, cmd_rx) = mpsc::channel();
    let device_id = id.to_owned();
    let worker_host = host.to_owned();
    let worker_hostname = hostname.to_owned();
    let notify = WorkerNotify {
      ownership_lost: self.ownership_lost.clone(),
      media_recovered: self.media_recovered.clone(),
      remote_events: self.remote_events.clone(),
    };
    let relay_slot: SharedRelaySlot = Arc::new(StdMutex::new(None));
    let relay_slot_worker = Arc::clone(&relay_slot);
    let reachable = Arc::new(AtomicBool::new(false));
    let reachable_worker = Arc::clone(&reachable);
    let terminal_failed = Arc::new(AtomicBool::new(false));
    let terminal_failed_worker = Arc::clone(&terminal_failed);
    let ownership_stand_down = Arc::new(AtomicBool::new(false));
    let ownership_stand_down_worker = Arc::clone(&ownership_stand_down);
    let thread_name = format!("cast-warm-{}", short_id(&device_id));
    let join = std::thread::Builder::new()
      .name(thread_name)
      .spawn(move || {
        worker_main(
          device_id,
          worker_host,
          worker_hostname,
          port,
          cmd_rx,
          notify,
          relay_slot_worker,
          reachable_worker,
          terminal_failed_worker,
          ownership_stand_down_worker,
          session_host,
        );
      })
      .ok();

    if join.is_none() {
      drop(guard);
      tracing::error!(%id, "failed to spawn warm Cast worker thread");
      return;
    }

    drop(guard.insert(
      id.to_owned(),
      CastWorkerHandle {
        cmd_tx,
        thread: join,
        host: host.to_owned(),
        hostname: hostname.to_owned(),
        port,
        relay_slot,
        exiting: false,
        reachable,
        terminal_failed,
        ownership_stand_down,
        session_host,
      },
    ));
    drop(guard);
    tracing::debug!(%id, %host, port, session_host, "warm Cast worker started");
  }

  /// Whether the warm Cast worker for `device_id` currently reports a reachable control plane.
  ///
  /// Returns `false` when no worker exists, the worker is exiting, or the last known
  /// state is unreachable. Used by Linux discovery liveness (TTL alone is insufficient).
  #[must_use]
  pub fn is_reachable(&self, device_id: &str) -> bool {
    let guard = self.workers.lock();
    guard
      .get(device_id)
      .is_some_and(|h| !h.exiting && h.reachable.load(Ordering::Relaxed))
  }

  /// Whether Cast control for `device_id` hit a process-lifetime terminal failure.
  ///
  /// Terminal devices must not be advertised as AirPlay sinks and must not be
  /// hammered by volume-seed / reconnect loops.
  #[must_use]
  pub fn is_terminal_failed(&self, device_id: &str) -> bool {
    let guard = self.workers.lock();
    guard.get(device_id).is_some_and(|h| h.terminal_failed.load(Ordering::Relaxed))
  }

  /// Shut down and remove the worker for `device_id` (device left the network).
  ///
  /// Forces relay socket shutdown so a blocked `rust_cast` read unblocks. The map
  /// entry is kept until the worker **actually** exits so [`Self::ensure`] cannot
  /// spawn a duplicate (on join timeout a reaper removes the tombstone later).
  pub fn remove(&self, device_id: &str) {
    let mut workers = self.workers.lock();
    let Some(worker) = workers.get_mut(device_id) else {
      return;
    };
    if worker.exiting {
      // Remove already in flight (or reaper pending); do not clear the tombstone.
      return;
    }
    worker.exiting = true;
    // Unblock any stuck read before/while Shutdown is processed.
    if let Ok(mut slot) = worker.relay_slot.lock()
      && let Some(relay) = slot.take()
    {
      relay.shutdown();
    }
    drop(worker.cmd_tx.send(CastWorkerCmd::Shutdown));
    let join_handle = worker.thread.take();
    drop(workers);

    if let Some(handle) = join_handle {
      join_and_reap_worker(handle, JOIN_TIMEOUT, device_id.to_owned(), Arc::clone(&self.workers));
    } else {
      // No live thread (spawn never produced a handle); drop the tombstone.
      drop(self.workers.lock().remove(device_id));
      tracing::info!(%device_id, "warm Cast worker removed");
    }
  }

  /// Shut down every worker (process exit).
  pub fn shutdown(&self) {
    let ids: Vec<String> = {
      let guard = self.workers.lock();
      guard.keys().cloned().collect()
    };
    for id in ids {
      self.remove(&id);
    }
  }

  /// Snapshot of device ids with a live worker (for maintain / tests).
  #[must_use]
  pub fn device_ids(&self) -> Vec<String> {
    let guard = self.workers.lock();
    guard
      .iter()
      .filter(|(_, handle)| !handle.session_host)
      .map(|(id, _)| id.clone())
      .collect()
  }

  /// Stop the active media session on the warm connection (no-op if no session).
  pub fn stop(&self, device_id: &str) -> Result<()> {
    let tx = self.cmd_tx(device_id)?;
    let (reply_tx, reply_rx) = mpsc::sync_channel(1);
    tx.send(CastWorkerCmd::Stop { reply: reply_tx })
      .map_err(|_send| Error::Cast(format!("warm Cast worker for {device_id} disconnected")))?;
    match reply_rx.recv_timeout(COMMAND_TIMEOUT) {
      Ok(result) => result,
      Err(RecvTimeoutError::Timeout) => Err(Error::Cast(format!("warm Cast stop timed out for {device_id}"))),
      Err(RecvTimeoutError::Disconnected) => Err(Error::Cast(format!("warm Cast stop reply dropped for {device_id}"))),
    }
  }

  fn adopt(
    &self,
    key: &str,
    session: ActiveCastSession,
    session_id: String,
    load: Option<MediaLoadRequest>,
  ) -> Result<()> {
    let tx = self.cmd_tx(key)?;
    let (reply_tx, reply_rx) = mpsc::sync_channel(1);
    tx.send(CastWorkerCmd::Adopt {
      session,
      session_id,
      load,
      reply: reply_tx,
    })
    .map_err(|_send| Error::Cast(format!("warm Cast worker for {key} disconnected")))?;
    match reply_rx.recv_timeout(COMMAND_TIMEOUT) {
      Ok(result) => result,
      Err(RecvTimeoutError::Timeout) => Err(Error::Cast(format!("session adopt timed out for {key}"))),
      Err(RecvTimeoutError::Disconnected) => Err(Error::Cast(format!("session adopt reply dropped for {key}"))),
    }
  }

  fn cmd_tx(&self, device_id: &str) -> Result<mpsc::Sender<CastWorkerCmd>> {
    let guard = self.workers.lock();
    guard
      .get(device_id)
      .map(|h| h.cmd_tx.clone())
      .ok_or_else(|| Error::Cast(format!("no warm Cast worker for {device_id}")))
  }
}

impl CastControl for CastPool {
  /// LOAD media on the warm control plane for `device_id`.
  ///
  /// Blocks up to [`COMMAND_TIMEOUT`]. If the warm TCP is dead, the worker tries
  /// reconnect with backoff before failing the command.
  ///
  /// On timeout the caller marks the op abandoned: a late worker success must STOP
  /// the session and must not leave `active` installed (B1).
  fn load(&self, device_id: &str, request: MediaLoadRequest) -> Result<ActiveCastSession> {
    let tx = self.cmd_tx(device_id)?;
    let (reply_tx, reply_rx) = mpsc::sync_channel(1);
    let abandoned = Arc::new(AtomicBool::new(false));
    tx.send(CastWorkerCmd::Load {
      request,
      reply: reply_tx,
      abandoned: Arc::clone(&abandoned),
    })
    .map_err(|_send| Error::Cast(format!("warm Cast worker for {device_id} disconnected")))?;
    match reply_rx.recv_timeout(COMMAND_TIMEOUT) {
      Ok(result) => result,
      Err(RecvTimeoutError::Timeout) => {
        abandoned.store(true, Ordering::Release);
        Err(Error::Cast(format!(
          "warm Cast load timed out after {}s for {device_id}",
          COMMAND_TIMEOUT.as_secs()
        )))
      },
      Err(RecvTimeoutError::Disconnected) => {
        abandoned.store(true, Ordering::Release);
        Err(Error::Cast(format!("warm Cast load reply dropped for {device_id}")))
      },
    }
  }

  /// Set volume on the warm connection (0.0..=1.0).
  fn set_volume(&self, device_id: &str, level: f32) -> Result<()> {
    let tx = self.cmd_tx(device_id)?;
    let (reply_tx, reply_rx) = mpsc::sync_channel(1);
    tx.send(CastWorkerCmd::SetVolume { level, reply: reply_tx })
      .map_err(|_send| Error::Cast(format!("warm Cast worker for {device_id} disconnected")))?;
    match reply_rx.recv_timeout(VOLUME_TIMEOUT) {
      Ok(result) => result,
      Err(RecvTimeoutError::Timeout) => Err(Error::Cast(format!("warm Cast set_volume timed out for {device_id}"))),
      Err(RecvTimeoutError::Disconnected) => {
        Err(Error::Cast(format!("warm Cast set_volume reply dropped for {device_id}")))
      },
    }
  }

  /// Read current receiver volume on the warm connection (`0.0..=1.0`; muted → `0.0`).
  fn get_volume(&self, device_id: &str) -> Result<f32> {
    let tx = self.cmd_tx(device_id)?;
    let (reply_tx, reply_rx) = mpsc::sync_channel(1);
    tx.send(CastWorkerCmd::GetVolume { reply: reply_tx })
      .map_err(|_send| Error::Cast(format!("warm Cast worker for {device_id} disconnected")))?;
    match reply_rx.recv_timeout(VOLUME_TIMEOUT) {
      Ok(result) => result,
      Err(RecvTimeoutError::Timeout) => Err(Error::Cast(format!("warm Cast get_volume timed out for {device_id}"))),
      Err(RecvTimeoutError::Disconnected) => {
        Err(Error::Cast(format!("warm Cast get_volume reply dropped for {device_id}")))
      },
    }
  }

  /// Pause active media. Errors with "no active session" when none is loaded.
  fn pause(&self, device_id: &str) -> Result<MediaPlayback> {
    let tx = self.cmd_tx(device_id)?;
    let (reply_tx, reply_rx) = mpsc::sync_channel(1);
    tx.send(CastWorkerCmd::Pause {
      queued_at: Instant::now(),
      reply: reply_tx,
    })
    .map_err(|_send| Error::Cast(format!("warm Cast worker for {device_id} disconnected")))?;
    match reply_rx.recv_timeout(COMMAND_TIMEOUT) {
      Ok(result) => result,
      Err(RecvTimeoutError::Timeout) => Err(Error::Cast(format!("warm Cast pause timed out for {device_id}"))),
      Err(RecvTimeoutError::Disconnected) => Err(Error::Cast(format!("warm Cast pause reply dropped for {device_id}"))),
    }
  }

  /// Resume a paused media session. Errors with "no active session" when none is loaded.
  fn play(&self, device_id: &str) -> Result<()> {
    let tx = self.cmd_tx(device_id)?;
    let (reply_tx, reply_rx) = mpsc::sync_channel(1);
    tx.send(CastWorkerCmd::Play { reply: reply_tx })
      .map_err(|_send| Error::Cast(format!("warm Cast worker for {device_id} disconnected")))?;
    match reply_rx.recv_timeout(COMMAND_TIMEOUT) {
      Ok(result) => result,
      Err(RecvTimeoutError::Timeout) => Err(Error::Cast(format!("warm Cast play timed out for {device_id}"))),
      Err(RecvTimeoutError::Disconnected) => Err(Error::Cast(format!("warm Cast play reply dropped for {device_id}"))),
    }
  }

  /// Read the active media session's player state and position.
  fn media_playback(&self, device_id: &str) -> Result<MediaPlayback> {
    let tx = self.cmd_tx(device_id)?;
    let (reply_tx, reply_rx) = mpsc::sync_channel(1);
    tx.send(CastWorkerCmd::MediaStatus { reply: reply_tx })
      .map_err(|_send| Error::Cast(format!("warm Cast worker for {device_id} disconnected")))?;
    match reply_rx.recv_timeout(VOLUME_TIMEOUT) {
      Ok(result) => result,
      Err(RecvTimeoutError::Timeout) => Err(Error::Cast(format!("warm Cast media status timed out for {device_id}"))),
      Err(RecvTimeoutError::Disconnected) => {
        Err(Error::Cast(format!("warm Cast media status reply dropped for {device_id}")))
      },
    }
  }

  /// Best-effort media STOP with a wall-clock timeout; never tears down warm TCP.
  ///
  /// Returns `true` when the worker acknowledged STOP within `timeout`. Returns
  /// `false` on missing worker, send failure, timeout, or disconnect so callers
  /// (full teardown) can retry once.
  fn stop_best_effort(&self, device_id: &str, timeout: Duration) -> bool {
    let Ok(tx) = self.cmd_tx(device_id) else {
      return false;
    };
    let (reply_tx, reply_rx) = mpsc::sync_channel(1);
    if tx.send(CastWorkerCmd::StopBestEffort { reply: reply_tx }).is_err() {
      return false;
    }
    match reply_rx.recv_timeout(timeout) {
      Ok(()) => true,
      Err(RecvTimeoutError::Timeout) => {
        tracing::warn!(
          %device_id,
          timeout_ms = timeout.as_millis(),
          "warm Cast STOP best-effort timed out"
        );
        false
      },
      Err(RecvTimeoutError::Disconnected) => {
        tracing::debug!(%device_id, "warm Cast STOP best-effort: worker gone");
        false
      },
    }
  }

  /// Mark a bridge-initiated session end so ownership probes stand down.
  ///
  /// Fire-and-forget: sets a cross-thread stand-down flag immediately, then queues a
  /// worker command that clears the tracked active load without sending Cast STOP.
  /// Call this on full session teardown (terminal stall, session end) **before** or
  /// with [`Self::stop_best_effort`] so a missing media session is not declared as
  /// "another app took the receiver".
  ///
  /// Do **not** call on replace-path teardowns: a new LOAD may already own (or is
  /// about to own) the active session.
  fn session_ended(&self, device_id: &str) {
    let guard = self.workers.lock();
    let Some(handle) = guard.get(device_id) else {
      return;
    };
    // Instant stand-down: probe must not declare loss while SessionEnded is still queued.
    handle.ownership_stand_down.store(true, Ordering::Release);
    let tx = handle.cmd_tx.clone();
    drop(guard);
    if tx.send(CastWorkerCmd::SessionEnded).is_err() {
      tracing::debug!(%device_id, "session_ended: worker gone");
    }
  }

  /// Replace the now-playing metadata shown for the live media on `device_id`.
  ///
  /// Fire-and-forget. The worker sends a `QUEUE_UPDATE` for the playing queue item (no
  /// re-LOAD, so audio is not interrupted) and remembers the track for later re-LOADs.
  fn update_metadata(&self, device_id: &str, track: CastTrack) {
    match self.cmd_tx(device_id) {
      Ok(tx) => {
        if tx.send(CastWorkerCmd::UpdateMetadata { track }).is_err() {
          tracing::debug!(%device_id, "update_metadata: worker gone");
        }
      },
      Err(err) => tracing::debug!(%device_id, error = %err, "update_metadata: no worker"),
    }
  }

  /// Send `SET_PLAYBACK_DEVICES` for the active session on worker `key` (the session host).
  ///
  /// Blocks up to [`COMMAND_TIMEOUT`] for `PLAYBACK_SESSION_UPDATED`; `INVALID_REQUEST`, no
  /// active session, or no answer is an error.
  fn set_playback_devices(&self, key: &str, request: SetPlaybackDevices) -> Result<PlaybackSessionUpdate> {
    let tx = self.cmd_tx(key)?;
    let (reply_tx, reply_rx) = mpsc::sync_channel(1);
    tx.send(CastWorkerCmd::SetPlaybackDevices { request, reply: reply_tx })
      .map_err(|_send| Error::Cast(format!("warm Cast worker for {key} disconnected")))?;
    match reply_rx.recv_timeout(COMMAND_TIMEOUT.saturating_add(VOLUME_TIMEOUT)) {
      Ok(result) => result,
      Err(RecvTimeoutError::Timeout) => Err(Error::Cast(format!("multizone request timed out for {key}"))),
      Err(RecvTimeoutError::Disconnected) => Err(Error::Cast(format!("multizone reply dropped for {key}"))),
    }
  }

  /// Open (or retarget) a worker for a multizone session host at `host:port`.
  ///
  /// Session hosts are not registry speakers: [`Self::device_ids`] leaves them out, and LOAD
  /// on them joins the running Default Media Receiver instead of launching it.
  fn open_session_host(&self, key: &str, host: &str, port: u16) {
    self.ensure_worker(key, host, "", port, true);
  }

  /// Hand the active media session from worker `from` to worker `to` (no Cast STOP).
  ///
  /// `to` finds the receiver app running `session_id` (default: the session's own id) on its
  /// host, joins its transport, and re-reads the media session there. A move gives the app a
  /// new transport, so the old transport id is never reused. On failure nothing is handed back:
  /// the session left `from`'s host, and controlling it there would address a gone endpoint.
  fn move_session(&self, from: &str, to: &str, session_id: Option<&str>) -> Result<()> {
    let from_tx = self.cmd_tx(from)?;
    let (take_tx, take_rx) = mpsc::sync_channel(1);
    from_tx
      .send(CastWorkerCmd::TakeSession { reply: take_tx })
      .map_err(|_send| Error::Cast(format!("warm Cast worker for {from} disconnected")))?;
    let (session, load) = take_rx
      .recv_timeout(COMMAND_TIMEOUT)
      .map_err(|_recv| Error::Cast(format!("session hand-off from {from} timed out")))?
      .ok_or_else(|| Error::Cast(format!("no active session on {from} to move")))?;
    let adopt_id = session_id.map_or_else(|| session.session_id.clone(), str::to_owned);
    self.adopt(to, session, adopt_id, load)
  }

  /// Close a worker opened with [`Self::open_session_host`].
  fn close_session_host(&self, key: &str) {
    let is_host = self.workers.lock().get(key).is_some_and(|handle| handle.session_host);
    if is_host {
      self.remove(key);
    }
  }
}

/// Mutable state owned exclusively by the worker thread.
#[expect(
  clippy::struct_excessive_bools,
  reason = "worker flags are independent latch bits (reachable/terminal/reload/source-ip log), not a mode machine"
)]
struct WorkerState {
  device_id: String,
  host: String,
  hostname: String,
  port: u16,
  /// Live Cast device; never leaves this thread (`Rc` inside `rust_cast`).
  device: Option<WarmCastDevice>,
  /// Media session after a successful warm LOAD.
  active: Option<ActiveCastSession>,
  /// When `active` was last set by a successful LOAD (ownership grace clock).
  active_since: Option<Instant>,
  /// Consecutive heartbeats where status succeeded but our transport was absent.
  ownership_loss_streak: u8,
  /// Consecutive BUFFERING media probes.
  buffering_streak: u8,
  /// App channel for confirmed ownership loss (`device_id` payload).
  ownership_lost: Option<UnboundedSender<String>>,
  /// App channel for internal media recovery events.
  media_recovered: Option<UnboundedSender<String>>,
  /// App channel for user-initiated play / pause / volume changes.
  remote_events: Option<UnboundedSender<CastRemoteEvent>>,
  /// Attributes Cast status changes to the user vs our own commands.
  remote: RemoteTracker,
  /// Next ownership probe (heartbeat cadence even when the drain runs faster).
  next_probe_at: Instant,
  /// Shared relay shutdown slot (pool `remove` force-closes).
  relay_slot: SharedRelaySlot,
  /// Last successful LOAD request (for internal re-LOAD recovery).
  last_load: Option<MediaLoadRequest>,
  /// When the last LOAD command was applied (rollover guard).
  last_load_at: Option<Instant>,
  /// Already attempted one internal re-LOAD for the current trouble episode.
  reload_attempted: bool,
  /// Deadline for the next outbound PING (independent of command traffic).
  next_ping_at: Instant,
  /// Exponential reconnect attempt counter (reset on success).
  reconnect_attempt: u32,
  /// When the device first became unreachable (for downtime logs).
  unreachable_since: Option<Instant>,
  /// Whether the last known control-plane state was reachable (local + shared flag).
  reachable: bool,
  /// Shared with the pool handle for cheap cross-thread reachability probes.
  reachable_flag: Arc<AtomicBool>,
  /// Process-lifetime terminal failure (shared with pool handle).
  terminal_flag: Arc<AtomicBool>,
  /// Local mirror of [`Self::terminal_flag`] for the worker loop.
  terminal_failed: bool,
  /// Bridge-initiated end: stand ownership probes down before STOP drains.
  ownership_stand_down: Arc<AtomicBool>,
  /// Earliest time to try another reconnect.
  next_reconnect_at: Instant,
  /// Last parse-error string warned (once per distinct message).
  last_parse_warn: Option<String>,
  /// Source IP logged once per successful connect for this worker.
  logged_source_ip: bool,
  /// Session host (multizone group endpoint): LOAD joins the running receiver instead of launching one.
  join_running_app: bool,
}

#[expect(
  clippy::needless_pass_by_value,
  reason = "worker thread owns the command receiver for its full lifetime"
)]
#[expect(
  clippy::too_many_arguments,
  reason = "worker bootstrap packs all channels/slots once at spawn"
)]
#[expect(
  clippy::too_many_lines,
  reason = "worker loop dispatches every CastWorkerCmd; splitting would obscure control flow"
)]
fn worker_main(
  device_id: String,
  host: String,
  hostname: String,
  port: u16,
  cmd_rx: mpsc::Receiver<CastWorkerCmd>,
  notify: WorkerNotify,
  relay_slot: SharedRelaySlot,
  reachable_flag: Arc<AtomicBool>,
  terminal_flag: Arc<AtomicBool>,
  ownership_stand_down: Arc<AtomicBool>,
  join_running_app: bool,
) {
  let now = Instant::now();
  let mut state = WorkerState {
    device_id,
    host,
    hostname,
    port,
    device: None,
    active: None,
    active_since: None,
    ownership_loss_streak: 0,
    buffering_streak: 0,
    ownership_lost: notify.ownership_lost,
    media_recovered: notify.media_recovered,
    remote_events: notify.remote_events,
    remote: RemoteTracker::default(),
    next_probe_at: now,
    relay_slot,
    last_load: None,
    last_load_at: None,
    reload_attempted: false,
    next_ping_at: now,
    reconnect_attempt: 0,
    unreachable_since: None,
    reachable: false,
    reachable_flag,
    terminal_flag,
    terminal_failed: false,
    ownership_stand_down,
    next_reconnect_at: now,
    last_parse_warn: None,
    logged_source_ip: false,
    join_running_app,
  };

  // Connect while idle (before any AirPlay session).
  if let Err(err) = state.ensure_connected(/* force */ true) {
    tracing::debug!(
      device_id = %state.device_id,
      host = %state.host,
      port = state.port,
      error = %err,
      "warm Cast initial connect failed; will retry with backoff"
    );
  }

  let mut pending: Option<CastWorkerCmd> = None;
  loop {
    let cmd = if let Some(cmd) = pending.take() {
      cmd
    } else {
      let wait = state.next_loop_wait();
      match cmd_rx.recv_timeout(wait) {
        Ok(cmd) => cmd,
        Err(RecvTimeoutError::Timeout) => {
          // Idle recovery / probes / reconnect are blocking Cast ops; same hard deadline.
          state.with_hard_deadline(WorkerState::on_idle_tick);
          continue;
        },
        Err(RecvTimeoutError::Disconnected) => {
          tracing::debug!(
            device_id = %state.device_id,
            "warm Cast command channel closed; worker exit"
          );
          state.drop_device();
          break;
        },
      }
    };

    match cmd {
      CastWorkerCmd::SetPlaybackDevices { request, reply } => {
        let result = state.with_hard_deadline(|s| s.handle_set_playback_devices(&request));
        drop(reply.send(result));
      },
      CastWorkerCmd::TakeSession { reply } => {
        drop(reply.send(state.handle_take_session()));
      },
      CastWorkerCmd::Adopt { session, session_id, load, reply } => {
        let result = state.with_hard_deadline(|s| s.handle_adopt(&session, &session_id, load));
        drop(reply.send(result));
      },
      CastWorkerCmd::Shutdown => {
        tracing::debug!(device_id = %state.device_id, "warm Cast worker shutting down");
        state.drop_device();
        break;
      },
      CastWorkerCmd::UpdateEndpoint {
        host: new_host,
        hostname: new_hostname,
        port: new_port,
      } => {
        let changed = state.host != new_host || state.hostname != new_hostname || state.port != new_port;
        state.host = new_host;
        state.hostname = new_hostname;
        state.port = new_port;
        if changed {
          tracing::info!(
            device_id = %state.device_id,
            host = %state.host,
            port = state.port,
            "warm Cast endpoint updated; reconnecting"
          );
          state.drop_device();
          state.reconnect_attempt = 0;
          state.next_reconnect_at = Instant::now();
          if let Err(err) = state.ensure_connected(true) {
            tracing::debug!(
              device_id = %state.device_id,
              error = %err,
              "warm Cast reconnect after endpoint update failed"
            );
          }
        }
      },
      CastWorkerCmd::Load { request, reply, abandoned } => {
        let load_result = state.with_hard_deadline(|s| s.handle_load(&request, &abandoned));
        // If the caller already abandoned, never deliver Ok (and ensure no active).
        let deliver = if abandoned.load(Ordering::Acquire) {
          if state.active.is_some() {
            drop(state.with_hard_deadline(WorkerState::handle_stop));
          }
          Err(Error::Cast(format!(
            "warm Cast load abandoned after caller timeout for {}",
            state.device_id
          )))
        } else {
          load_result
        };
        if reply.send(deliver).is_err() {
          // Reply channel gone: late success must not leave an unacknowledged session.
          if state.active.is_some() {
            drop(state.with_hard_deadline(WorkerState::handle_stop));
          }
        }
      },
      CastWorkerCmd::SetVolume { level, reply } => {
        let (final_level, replies, next_pending) = coalesce_set_volume(level, reply, &cmd_rx);
        pending = next_pending;
        let result = state.with_hard_deadline(|s| s.handle_set_volume(final_level));
        let err_msg = result.as_ref().err().map(ToString::to_string);
        for r in replies {
          let send_result = err_msg.as_ref().map_or(Ok(()), |msg| Err(Error::Cast(msg.clone())));
          drop(r.send(send_result));
        }
      },
      CastWorkerCmd::GetVolume { reply } => {
        let result = state.with_hard_deadline(WorkerState::handle_get_volume);
        drop(reply.send(result));
      },
      CastWorkerCmd::Stop { reply } => {
        let result = state.with_hard_deadline(WorkerState::handle_stop);
        drop(reply.send(result));
      },
      CastWorkerCmd::Pause { queued_at, reply } => {
        let result = state.with_hard_deadline(|s| s.handle_pause(queued_at));
        drop(reply.send(result));
      },
      CastWorkerCmd::Play { reply } => {
        let result = state.with_hard_deadline(WorkerState::handle_play);
        drop(reply.send(result));
      },
      CastWorkerCmd::MediaStatus { reply } => {
        let result = state.with_hard_deadline(|s| s.handle_media_status());
        drop(reply.send(result));
      },
      CastWorkerCmd::StopBestEffort { reply } => {
        if let Err(err) = state.with_hard_deadline(WorkerState::handle_stop) {
          tracing::debug!(
            device_id = %state.device_id,
            error = %err,
            "warm Cast STOP best-effort failed"
          );
        }
        // STOP may fail after take; ensure ownership state is fully cleared either way.
        state.clear_active_session();
        let _send = reply.send(());
      },
      CastWorkerCmd::SessionEnded => {
        state.clear_active_session();
        state.ownership_stand_down.store(true, Ordering::Release);
        tracing::debug!(
          device_id = %state.device_id,
          "Cast session ended by bridge; ownership probe stand-down"
        );
      },
      CastWorkerCmd::UpdateMetadata { track } => {
        // Latest wins: a newer queued update supersedes this one.
        let mut latest = track;
        loop {
          match cmd_rx.try_recv() {
            Ok(CastWorkerCmd::UpdateMetadata { track: newer }) => latest = newer,
            Ok(other) => {
              pending = Some(other);
              break;
            },
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
          }
        }
        state.with_hard_deadline(|s| s.handle_update_metadata(latest));
      },
    }
  }
}

/// Drain consecutive queued `SetVolume` commands; return final level and all reply channels.
type CoalescedVolume = (f32, Vec<SyncSender<Result<()>>>, Option<CastWorkerCmd>);

fn coalesce_set_volume(
  mut level: f32,
  first_reply: SyncSender<Result<()>>,
  cmd_rx: &mpsc::Receiver<CastWorkerCmd>,
) -> CoalescedVolume {
  let mut replies = vec![first_reply];
  loop {
    match cmd_rx.try_recv() {
      Ok(CastWorkerCmd::SetVolume { level: next, reply: next_reply }) => {
        level = next;
        replies.push(next_reply);
      },
      Ok(other) => return (level, replies, Some(other)),
      Err(TryRecvError::Empty | TryRecvError::Disconnected) => return (level, replies, None),
    }
  }
}

impl WorkerState {
  fn next_loop_wait(&self) -> Duration {
    let now = Instant::now();
    let mut wait = HEARTBEAT_INTERVAL;
    if self.device.is_some() {
      wait = wait.min(self.next_ping_at.saturating_duration_since(now));
      if self.active.is_some() {
        wait = wait.min(ACTIVE_DRAIN_INTERVAL);
      }
    } else {
      wait = wait.min(self.next_reconnect_at.saturating_duration_since(now));
    }
    // Never sleep forever; floor at 50 ms so deadline checks stay responsive.
    if wait.is_zero() {
      Duration::from_millis(50)
    } else {
      wait
    }
  }

  fn on_idle_tick(&mut self) {
    if self.device.is_none() {
      if self.terminal_failed {
        return;
      }
      if Instant::now() >= self.next_reconnect_at
        && let Err(err) = self.ensure_connected(false)
      {
        tracing::debug!(
          device_id = %self.device_id,
          host = %self.host,
          error = %err,
          attempt = self.reconnect_attempt,
          "warm Cast reconnect attempt failed"
        );
      }
      return;
    }

    self.drain_unsolicited();

    if Instant::now() >= self.next_ping_at {
      self.heartbeat_ping();
    }

    if Instant::now() >= self.next_probe_at {
      self.next_probe_at = Instant::now() + HEARTBEAT_INTERVAL;
      self.check_ownership();
    }
  }

  fn drop_device(&mut self) {
    self.clear_active_session();
    self.device = None;
    if let Ok(mut slot) = self.relay_slot.lock()
      && let Some(relay) = slot.take()
    {
      relay.shutdown();
    }
  }

  fn clear_active_session(&mut self) {
    self.active = None;
    self.active_since = None;
    self.ownership_loss_streak = 0;
    self.buffering_streak = 0;
    self.reload_attempted = false;
    self.remote.clear_player();
  }

  fn set_active_session(&mut self, session: ActiveCastSession) {
    self.active = Some(session);
    self.active_since = Some(Instant::now());
    self.ownership_loss_streak = 0;
    self.buffering_streak = 0;
    self.reload_attempted = false;
    self.remote.expect_player(PlayerIntent::Playing, Instant::now());
    // New LOAD re-enables ownership probes (bridge end flag must not stick).
    self.ownership_stand_down.store(false, Ordering::Release);
  }

  /// Attribute a media status entry for our session; report a user play / pause.
  fn observe_media_entry(&mut self, entry: &rust_cast::channels::media::StatusEntry) {
    if self
      .active
      .as_ref()
      .is_none_or(|s| s.media_session_id != entry.media_session_id)
    {
      return;
    }
    if let Some(kind) = self.remote.observe_player(entry.player_state, Instant::now()) {
      self.emit_remote(kind);
    }
  }

  /// Attribute a receiver volume; report a user volume / mute change.
  fn observe_receiver_volume(&mut self, volume: rust_cast::channels::receiver::Volume) {
    let (Some(level), muted) = (volume.level, volume.muted.unwrap_or(false)) else {
      return;
    };
    if let Some(kind) = self.remote.observe_volume(level, muted, Instant::now()) {
      self.emit_remote(kind);
    }
  }

  fn emit_remote(&self, kind: CastRemoteKind) {
    tracing::info!(device_id = %self.device_id, ?kind, "Cast change not commanded by us (user / other sender)");
    if let Some(tx) = &self.remote_events
      && tx.send(CastRemoteEvent { device_id: self.device_id.clone(), kind }).is_err()
    {
      tracing::debug!(device_id = %self.device_id, "remote-events channel closed");
    }
  }

  /// Send the new now-playing metadata as a `QUEUE_UPDATE` of the playing item.
  fn handle_update_metadata(&mut self, track: CastTrack) {
    let Some(load) = self.last_load.as_mut() else {
      return;
    };
    if load.track == track {
      return;
    }
    load.track = track;
    let Some(session) = &self.active else {
      return;
    };
    let Some(item_id) = session.queue_item_id else {
      tracing::debug!(
        device_id = %self.device_id,
        "Cast LOAD reported no queue item id; metadata applies on the next LOAD"
      );
      return;
    };
    let (Some(device), Some(request)) = (self.device.as_ref(), self.last_load.as_ref()) else {
      return;
    };
    let media_session_id = session.media_session_id;
    let request_id = device.message_manager.generate_request_id().get();
    let body = request.queue_update_payload(request_id, media_session_id, item_id);
    let message = rust_cast::message_manager::CastMessage {
      namespace: MEDIA_NAMESPACE.to_owned(),
      source: "sender-0".to_owned(),
      destination: session.transport_id.clone(),
      payload: rust_cast::message_manager::CastMessagePayload::String(body.to_string()),
    };
    // The MEDIA_STATUS reply is left for the idle drain (it also feeds play/pause attribution).
    match device.message_manager.send(message) {
      Ok(()) => {
        tracing::debug!(device_id = %self.device_id, media_session_id, item_id, "Cast metadata QUEUE_UPDATE sent");
      },
      Err(err) => tracing::debug!(device_id = %self.device_id, error = %err, "Cast metadata QUEUE_UPDATE failed"),
    }
  }

  /// Run `op` under a hard deadline: if still running after [`WORKER_OP_DEADLINE`],
  /// shut down the relay so blocking `rust_cast` I/O unblocks, then clear `device`
  /// so the next command/idle tick rebuilds the connection.
  fn with_hard_deadline<T>(&mut self, op: impl FnOnce(&mut Self) -> T) -> T {
    let relay_slot = Arc::clone(&self.relay_slot);
    let fired = Arc::new(AtomicBool::new(false));
    let fired_watch = Arc::clone(&fired);
    let (cancel_tx, cancel_rx) = mpsc::channel::<()>();
    let device_id = self.device_id.clone();
    let watchdog = std::thread::Builder::new()
      .name(format!("cast-deadline-{}", short_id(&device_id)))
      .spawn(move || {
        // Only fire on wall-clock timeout. Dropping `cancel_tx` yields Disconnected (success path).
        if matches!(cancel_rx.recv_timeout(WORKER_OP_DEADLINE), Err(RecvTimeoutError::Timeout)) {
          tracing::warn!(
            %device_id,
            deadline_s = WORKER_OP_DEADLINE.as_secs(),
            "warm Cast op exceeded hard deadline; shutting down relay"
          );
          fired_watch.store(true, Ordering::SeqCst);
          if let Ok(mut slot) = relay_slot.lock()
            && let Some(relay) = slot.take()
          {
            relay.shutdown();
          }
        }
      })
      .ok();

    let result = op(self);
    drop(cancel_tx);
    if let Some(handle) = watchdog {
      drop(handle.join());
    }
    if fired.load(Ordering::SeqCst) {
      // Relay already shut by watchdog; drop dead client so reconnect rebuilds.
      let _cleared = apply_hard_deadline_device_clear(&mut self.device);
    }
    result
  }

  /// Establish or re-establish the warm control plane (respects backoff unless `force`).
  fn ensure_connected(&mut self, force: bool) -> Result<()> {
    if self.device.is_some() {
      return Ok(());
    }
    if self.terminal_failed {
      return Err(Error::Cast(format!(
        "terminal Cast failure for {} (not reconnecting)",
        self.device_id
      )));
    }
    let now = Instant::now();
    if !force && now < self.next_reconnect_at {
      return Err(Error::Cast(format!("reconnect backoff active for {}", self.device_id)));
    }

    self.refresh_host();
    crate::net::wake_cast_host(&self.host);

    match connect_cast_device(&self.host, self.port, &self.relay_slot) {
      Ok(device) => {
        if let Err(connect_err) = device.connection.connect("receiver-0") {
          // Drop device (closes TLS) and shut the relay still held in the slot.
          drop(device);
          self.shutdown_relay_slot();
          let err = Error::Cast(format!("warm connection channel: {connect_err}"));
          self.note_unreachable(&err);
          return Err(err);
        }
        if let Err(ping_err) = device.heartbeat.ping() {
          drop(device);
          self.shutdown_relay_slot();
          let err = Error::Cast(format!("warm heartbeat: {ping_err}"));
          self.note_unreachable(&err);
          return Err(err);
        }

        self.device = Some(device);
        self.next_ping_at = Instant::now() + HEARTBEAT_INTERVAL;
        self.note_reachable();
        if self.logged_source_ip {
          tracing::info!(
            host = %self.host,
            port = self.port,
            device_id = %self.device_id,
            "warm Cast connected"
          );
        } else {
          let advertise = crate::net::advertise_host_for_peer(&self.host);
          tracing::info!(
            device_id = %self.device_id,
            host = %self.host,
            port = self.port,
            advertise_ip = %advertise,
            "warm Cast connected (advertise/source IP)"
          );
          self.logged_source_ip = true;
        }
        Ok(())
      },
      Err(err) => {
        self.note_unreachable(&err);
        Err(err)
      },
    }
  }

  /// Take and shut any relay currently installed in the shared slot.
  fn shutdown_relay_slot(&self) {
    if let Ok(mut slot) = self.relay_slot.lock()
      && let Some(relay) = slot.take()
    {
      relay.shutdown();
    }
  }

  fn note_reachable(&mut self) {
    let was_unreachable = !self.reachable;
    let attempts = self.reconnect_attempt;
    let downtime = self.unreachable_since.map(|t| t.elapsed());
    let recovering = downtime.is_some() || attempts > 0;
    self.reachable = true;
    self.reachable_flag.store(true, Ordering::Relaxed);
    self.reconnect_attempt = 0;
    self.unreachable_since = None;
    self.next_reconnect_at = Instant::now();
    if was_unreachable {
      if recovering {
        tracing::info!(
          device_id = %self.device_id,
          host = %self.host,
          attempts,
          downtime_ms = downtime.map(|d| d.as_millis()),
          "Cast control plane reachable again"
        );
      } else {
        tracing::info!(
          device_id = %self.device_id,
          host = %self.host,
          "Cast control plane reachable"
        );
      }
    }
  }

  fn note_unreachable(&mut self, err: &Error) {
    if classify_cast_control_error(err) == CastFailureClass::Terminal {
      self.mark_terminal_failed(err);
      return;
    }
    if self.reachable || self.unreachable_since.is_none() {
      tracing::info!(
        device_id = %self.device_id,
        host = %self.host,
        error = %err,
        "Cast control plane unreachable"
      );
      self.unreachable_since = Some(Instant::now());
    }
    self.reachable = false;
    self.reachable_flag.store(false, Ordering::Relaxed);
    self.reconnect_attempt = self.reconnect_attempt.saturating_add(1);
    let seed = self
      .device_id
      .bytes()
      .fold(0_u64, |acc, b| acc.wrapping_mul(31).wrapping_add(u64::from(b)));
    let delay = reconnect_backoff_delay(self.reconnect_attempt.saturating_sub(1), seed);
    self.next_reconnect_at = Instant::now() + delay;
    tracing::debug!(
      device_id = %self.device_id,
      attempt = self.reconnect_attempt,
      backoff_ms = delay.as_millis(),
      error = %err,
      "Cast reconnect scheduled"
    );
  }

  /// Latch a process-lifetime terminal Cast failure (once) and stop reconnects.
  fn mark_terminal_failed(&mut self, err: &Error) {
    self.reachable = false;
    self.reachable_flag.store(false, Ordering::Relaxed);
    self.next_reconnect_at = Instant::now() + TERMINAL_RECONNECT_HOLD;
    if self.terminal_failed {
      return;
    }
    self.terminal_failed = true;
    self.terminal_flag.store(true, Ordering::Release);
    if self.unreachable_since.is_none() {
      self.unreachable_since = Some(Instant::now());
    }
    tracing::warn!(
      device_id = %self.device_id,
      host = %self.host,
      error = %err,
      "Cast control plane terminal failure; will not advertise AirPlay or reconnect"
    );
  }

  fn refresh_host(&mut self) {
    if self.hostname.is_empty() {
      return;
    }
    if let Some(ip) = crate::net::resolve_host_ipv4(&self.hostname)
      && ip != self.host
    {
      tracing::info!(
        old = %self.host,
        new = %ip,
        hostname = %self.hostname,
        "warm Cast refreshed host IP"
      );
      self.host = ip;
      self.logged_source_ip = false;
    }
  }

  /// Deadline-based PING: one failure marks suspect and tries one reconnect (no kick).
  fn heartbeat_ping(&mut self) {
    self.next_ping_at = Instant::now() + HEARTBEAT_INTERVAL;
    let Some(device) = self.device.as_ref() else {
      return;
    };
    if let Err(ping_err) = device.heartbeat.ping() {
      tracing::debug!(
        device_id = %self.device_id,
        host = %self.host,
        error = %ping_err,
        "warm Cast PING failed; reconnecting once (no ownership kick)"
      );
      self.device = None;
      if let Ok(mut slot) = self.relay_slot.lock()
        && let Some(relay) = slot.take()
      {
        relay.shutdown();
      }
      if let Err(reconnect_err) = self.ensure_connected(true) {
        tracing::debug!(
          device_id = %self.device_id,
          error = %reconnect_err,
          "warm Cast reconnect after PING failure failed"
        );
        // Only after reconnect fails, count toward loss if we had a session.
        if self.active.is_some() {
          self.ownership_loss_streak = self.ownership_loss_streak.saturating_add(1);
          if self.ownership_loss_streak >= OWNERSHIP_LOSS_CONFIRMATIONS
            && let Some(session) = self.active.clone()
          {
            self.declare_ownership_lost(&session.transport_id, session.media_session_id);
          }
        }
      }
    }
  }

  /// Drain `rust_cast`'s internal buffer: answer PINGs, note `MEDIA_STATUS`, discard rest.
  ///
  /// Uses a short `SO_RCVTIMEO` on the **TLS underlying `TcpStream`** (dual-owned clone)
  /// so an empty socket cannot block for the full command I/O timeout. RAII restores
  /// the command timeout even if the drain exits early.
  fn drain_unsolicited(&mut self) {
    let (parse_warn, media_entry, receiver_volume) = {
      let Some(device) = self.device.as_ref() else {
        return;
      };
      // Binds timeout for the duration of this block (including all receive calls).
      let _timeout_guard = device.begin_short_read(DRAIN_READ_TIMEOUT);
      let mut parse_warn: Option<String> = None;
      // Latest status wins: earlier ones in the same drain are superseded.
      let our_session = self.active.as_ref().map(|s| s.media_session_id);
      let mut media_entry = None;
      let mut receiver_volume = None;

      for _ in 0..UNSOLICITED_DRAIN_LIMIT {
        match device.receive() {
          Ok(rust_cast::ChannelMessage::Heartbeat(hb)) => {
            use rust_cast::channels::heartbeat::HeartbeatResponse;
            if matches!(hb, HeartbeatResponse::Ping)
              && let Err(err) = device.heartbeat.pong()
            {
              tracing::debug!(device_id = %self.device_id, error = %err, "Cast pong failed");
              break;
            }
          },
          Ok(rust_cast::ChannelMessage::Media(rust_cast::channels::media::MediaResponse::Status(status))) => {
            tracing::debug!(device_id = %self.device_id, ?status, "Cast unsolicited media status");
            if let Some(entry) = status.entries.into_iter().find(|e| Some(e.media_session_id) == our_session) {
              media_entry = Some(entry);
            }
          },
          Ok(rust_cast::ChannelMessage::Receiver(rust_cast::channels::receiver::ReceiverResponse::Status(status))) => {
            tracing::debug!(device_id = %self.device_id, volume = ?status.volume, "Cast unsolicited receiver status");
            receiver_volume = Some(status.volume);
          },
          Ok(other) => {
            tracing::debug!(device_id = %self.device_id, msg = ?other, "Cast unsolicited message drained");
          },
          Err(err) => {
            match classify_cast_probe_error(&err) {
              ProbeFailureKind::Transport => {
                // Short-timeout idle is expected when nothing is pending/buffered.
                let msg = err.to_string();
                if msg.contains("timed out")
                  || msg.contains("os error 35")
                  || msg.contains("os error 60")
                  || msg.contains("WouldBlock")
                  || msg.contains("Resource temporarily unavailable")
                  || msg.contains("Interrupted")
                {
                  break;
                }
                tracing::debug!(
                  device_id = %self.device_id,
                  error = %err,
                  "Cast receive transport error while draining"
                );
                break;
              },
              ProbeFailureKind::Parse => {
                parse_warn = Some(err.to_string());
                break;
              },
            }
          },
        }
      }
      (parse_warn, media_entry, receiver_volume)
    };
    if let Some(msg) = parse_warn {
      self.warn_parse_once(&msg);
    }
    if let Some(entry) = media_entry {
      self.observe_media_entry(&entry);
    }
    if let Some(volume) = receiver_volume {
      self.observe_receiver_volume(volume);
    }
  }

  fn warn_parse_once(&mut self, message: &str) {
    if self.last_parse_warn.as_deref() == Some(message) {
      return;
    }
    tracing::warn!(
      device_id = %self.device_id,
      error = %message,
      "Cast status parse error (inconclusive; not ownership loss)"
    );
    self.last_parse_warn = Some(message.to_owned());
  }

  /// Confirm we still own Cast media while a warm LOAD session is active.
  fn check_ownership(&mut self) {
    // Bridge-initiated end (`session_ended`) or no tracked load: stand down so missing
    // media is not declared as "another app took the receiver".
    if ownership_probe_stands_down(self.active.is_none(), self.ownership_stand_down.load(Ordering::Acquire)) {
      self.ownership_loss_streak = 0;
      self.buffering_streak = 0;
      return;
    }
    let Some(session) = self.active.as_ref() else {
      return;
    };
    let Some(since) = self.active_since else {
      return;
    };
    let transport_id = session.transport_id.clone();
    let media_session_id = session.media_session_id;
    if since.elapsed() < OWNERSHIP_GRACE {
      return;
    }
    if self.device.is_none() {
      return;
    }

    let transport_listed = self.probe_transport_listed(&transport_id);
    let media = self.probe_media_session(&transport_id, media_session_id);

    if media.buffering {
      self.buffering_streak = self.buffering_streak.saturating_add(1);
    } else {
      self.buffering_streak = 0;
    }

    let load_within_guard = self.last_load_at.is_some_and(|t| t.elapsed() < RECENT_LOAD_GUARD);

    let action = ownership_action(OwnershipInputs {
      transport_listed,
      media_session_present: media.session_present,
      idle_reason: media.idle_reason,
      buffering: media.buffering,
      buffering_streak: self.buffering_streak,
      media_failure: media.failure,
      reload_attempted: self.reload_attempted,
      load_within_guard,
    });

    match action {
      OwnershipAction::Owned => {
        self.ownership_loss_streak = 0;
        self.reload_attempted = false;
      },
      OwnershipAction::Inconclusive => {},
      OwnershipAction::SuspectReconnect => {
        tracing::debug!(
          device_id = %self.device_id,
          "Cast ownership probe transport failure; reconnecting"
        );
        self.device = None;
        if let Ok(mut slot) = self.relay_slot.lock()
          && let Some(relay) = slot.take()
        {
          relay.shutdown();
        }
        drop(self.ensure_connected(true));
      },
      OwnershipAction::AttemptReload => {
        let recovered = self.try_internal_reload();
        self.reload_attempted = reload_attempted_after_internal_reload(recovered);
        if recovered {
          self.ownership_loss_streak = 0;
          self.buffering_streak = 0;
        } else {
          self.note_lost_signal(&transport_id, media_session_id);
        }
      },
      OwnershipAction::LostSignal => {
        self.note_lost_signal(&transport_id, media_session_id);
      },
    }
  }

  fn note_lost_signal(&mut self, transport_id: &str, media_session_id: i32) {
    self.ownership_loss_streak = self.ownership_loss_streak.saturating_add(1);
    if self.ownership_loss_streak < OWNERSHIP_LOSS_CONFIRMATIONS {
      tracing::info!(
        device_id = %self.device_id,
        transport_id = %transport_id,
        media_session_id,
        streak = self.ownership_loss_streak,
        "Cast ownership look stolen; waiting for confirmation"
      );
      return;
    }
    self.declare_ownership_lost(transport_id, media_session_id);
  }

  fn try_internal_reload(&mut self) -> bool {
    let Some(request) = self.last_load.clone() else {
      tracing::debug!(device_id = %self.device_id, "Cast recover re-LOAD skipped: no last load params");
      return false;
    };
    tracing::info!(
      device_id = %self.device_id,
      url = %request.content_url,
      "Cast recoverable IDLE/buffering; attempting internal re-LOAD"
    );
    let not_abandoned = AtomicBool::new(false);
    match self.handle_load(&request, &not_abandoned) {
      Ok(_session) => {
        tracing::info!(device_id = %self.device_id, "Cast media recovered via internal re-LOAD");
        if let Some(tx) = &self.media_recovered
          && tx.send(self.device_id.clone()).is_err()
        {
          tracing::debug!(device_id = %self.device_id, "media_recovered channel closed");
        }
        true
      },
      Err(err) => {
        tracing::warn!(
          device_id = %self.device_id,
          error = %err,
          "Cast internal re-LOAD recovery failed"
        );
        false
      },
    }
  }

  fn probe_transport_listed(&mut self, transport_id: &str) -> bool {
    let Some(device) = self.device.as_ref() else {
      return false;
    };
    match device.receiver.get_status() {
      Ok(status) => {
        let ids: Vec<&str> = status.applications.iter().map(|app| app.transport_id.as_str()).collect();
        let listed = cast_transport_still_owned(&ids, transport_id);
        tracing::debug!(
          device_id = %self.device_id,
          %transport_id,
          apps = ?ids,
          listed,
          "Cast ownership receiver status"
        );
        self.observe_receiver_volume(status.volume);
        listed
      },
      Err(err) => {
        match classify_cast_probe_error(&err) {
          ProbeFailureKind::Parse => {
            self.warn_parse_once(&err.to_string());
          },
          ProbeFailureKind::Transport => {
            tracing::debug!(
              device_id = %self.device_id,
              error = %err,
              "Cast ownership receiver get_status transport failure"
            );
          },
        }
        false
      },
    }
  }

  fn probe_media_session(&mut self, transport_id: &str, media_session_id: i32) -> MediaProbe {
    let Some(device) = self.device.as_ref() else {
      return MediaProbe {
        session_present: None,
        idle_reason: None,
        buffering: false,
        failure: Some(ProbeFailureKind::Transport),
      };
    };
    // Media channel requires CONNECT to the app transport (same as PAUSE/PLAY).
    if let Err(err) = device.connection.connect(transport_id) {
      tracing::debug!(
        device_id = %self.device_id,
        %transport_id,
        error = %err,
        "Cast ownership media connect failed"
      );
    }

    match device.media.get_status(transport_id, Some(media_session_id)) {
      Ok(status) => {
        let entry = status.entries.iter().find(|e| e.media_session_id == media_session_id);
        if let Some(e) = entry {
          use rust_cast::channels::media::{IdleReason, PlayerState};
          let idle_reason = if matches!(e.player_state, PlayerState::Idle) {
            match e.idle_reason {
              Some(IdleReason::Interrupted) => Some(IdleReasonKind::Interrupted),
              Some(IdleReason::Error | IdleReason::Finished | IdleReason::Cancelled) => {
                Some(IdleReasonKind::Recoverable)
              },
              None => Some(IdleReasonKind::Recoverable),
            }
          } else {
            None
          };
          let buffering = matches!(e.player_state, PlayerState::Buffering);
          tracing::debug!(
            device_id = %self.device_id,
            media_session_id,
            player_state = %e.player_state,
            idle_reason = ?e.idle_reason,
            buffering,
            "Cast ownership media status"
          );
          self.observe_media_entry(e);
          MediaProbe {
            session_present: Some(true),
            idle_reason,
            buffering,
            failure: None,
          }
        } else {
          tracing::debug!(
            device_id = %self.device_id,
            media_session_id,
            entries = status.entries.len(),
            "Cast ownership media status has no matching session"
          );
          MediaProbe {
            session_present: Some(false),
            idle_reason: None,
            buffering: false,
            failure: None,
          }
        }
      },
      Err(err) => {
        let kind = classify_cast_probe_error(&err);
        if kind == ProbeFailureKind::Parse {
          self.warn_parse_once(&err.to_string());
        } else {
          tracing::debug!(
            device_id = %self.device_id,
            media_session_id,
            error = %err,
            "Cast ownership media get_status transport failure"
          );
        }
        MediaProbe {
          session_present: None,
          idle_reason: None,
          buffering: false,
          failure: Some(kind),
        }
      },
    }
  }

  fn declare_ownership_lost(&mut self, transport_id: &str, media_session_id: i32) {
    tracing::info!(
      device_id = %self.device_id,
      transport_id = %transport_id,
      media_session_id,
      "Cast ownership lost (another app took the receiver)"
    );
    self.clear_active_session();
    if let Some(tx) = &self.ownership_lost
      && tx.send(self.device_id.clone()).is_err()
    {
      tracing::warn!(device_id = %self.device_id, "ownership-lost channel closed; cannot kick AirPlay");
    }
  }

  fn handle_load(&mut self, request: &MediaLoadRequest, abandoned: &AtomicBool) -> Result<ActiveCastSession> {
    self.last_load = Some(request.clone());
    self.last_load_at = Some(Instant::now());
    let media = request.to_media();
    match self.load_once(&media) {
      Ok(session) => self.commit_load_session(session, &media.content_id, abandoned),
      Err(err) => {
        if abandoned.load(Ordering::Acquire) {
          return Err(Error::Cast(format!(
            "warm Cast load abandoned after caller timeout for {}",
            self.device_id
          )));
        }
        tracing::warn!(
          device_id = %self.device_id,
          host = %self.host,
          error = %err,
          "warm Cast load failed; trying one reconnect"
        );
        self.device = None;
        if let Ok(mut slot) = self.relay_slot.lock()
          && let Some(relay) = slot.take()
        {
          relay.shutdown();
        }
        if let Err(re_err) = self.ensure_connected(true) {
          tracing::warn!(
            device_id = %self.device_id,
            error = %re_err,
            "warm Cast reconnect before load retry failed"
          );
          return Err(err);
        }
        if abandoned.load(Ordering::Acquire) {
          return Err(Error::Cast(format!(
            "warm Cast load abandoned after caller timeout for {}",
            self.device_id
          )));
        }
        let session = self.load_once(&media).map_err(|retry_err| {
          tracing::warn!(
            device_id = %self.device_id,
            error = %retry_err,
            "warm Cast load after reconnect failed"
          );
          retry_err
        })?;
        self.commit_load_session(session, &media.content_id, abandoned)
      },
    }
  }

  /// Install `active` only when the caller has not abandoned; otherwise STOP late media.
  fn commit_load_session(
    &mut self,
    session: ActiveCastSession,
    content_url: &str,
    abandoned: &AtomicBool,
  ) -> Result<ActiveCastSession> {
    match load_commit_decision(abandoned.load(Ordering::Acquire)) {
      LoadCommitDecision::AbandonAndStop => {
        self.stop_late_load_session(&session);
        Err(Error::Cast(format!(
          "warm Cast load abandoned after caller timeout for {}",
          self.device_id
        )))
      },
      LoadCommitDecision::InstallActive => {
        self.set_active_session(session.clone());
        // TOCTOU: caller may have timed out between the check and install.
        if abandoned.load(Ordering::Acquire) {
          tracing::info!(
            device_id = %self.device_id,
            transport_id = %session.transport_id,
            media_session_id = session.media_session_id,
            "warm Cast load abandoned after install; stopping late session"
          );
          drop(self.handle_stop());
          return Err(Error::Cast(format!(
            "warm Cast load abandoned after caller timeout for {}",
            self.device_id
          )));
        }
        tracing::info!(
          host = %self.host,
          device_id = %self.device_id,
          transport_id = %session.transport_id,
          media_session_id = session.media_session_id,
          url = %content_url,
          "warm Cast load"
        );
        Ok(session)
      },
    }
  }

  /// Best-effort STOP for a session that never became acknowledged `active`.
  fn stop_late_load_session(&mut self, session: &ActiveCastSession) {
    tracing::info!(
      device_id = %self.device_id,
      transport_id = %session.transport_id,
      media_session_id = session.media_session_id,
      "warm Cast load abandoned after caller timeout; stopping late session"
    );
    // Route through handle_stop so ownership state stays consistent.
    self.active = Some(session.clone());
    if let Err(err) = self.handle_stop() {
      tracing::debug!(
        device_id = %self.device_id,
        error = %err,
        "late abandoned LOAD STOP failed"
      );
    }
  }

  fn load_once(&self, media: &rust_cast::channels::media::Media) -> Result<ActiveCastSession> {
    let device = self
      .device
      .as_ref()
      .ok_or_else(|| Error::Cast("warm Cast device not connected".to_owned()))?;

    device
      .heartbeat
      .ping()
      .map_err(|err| Error::Cast(format!("warm load heartbeat: {err}")))?;

    let running = if self.join_running_app {
      running_media_receiver(device)
    } else {
      None
    };
    let app = match running {
      Some(app) => app,
      None => device
        .receiver
        .launch_app(&rust_cast::channels::receiver::CastDeviceApp::DefaultMediaReceiver)
        .map_err(|err| Error::Cast(format!("warm launch app: {err}")))?,
    };

    device
      .connection
      .connect(app.transport_id.as_str())
      .map_err(|err| Error::Cast(format!("warm app connection: {err}")))?;

    let status = device
      .media
      .load(app.transport_id.as_str(), app.session_id.as_str(), media)
      .map_err(|err| Error::Cast(format!("warm media load: {err}")))?;

    media_session_from_status(app.transport_id, app.session_id, &status)
  }

  /// Send `SET_PLAYBACK_DEVICES` for the active session to `receiver-0` and await the answer.
  fn handle_set_playback_devices(&mut self, request: &SetPlaybackDevices) -> Result<PlaybackSessionUpdate> {
    let session_id = self
      .active
      .as_ref()
      .map(|session| session.session_id.clone())
      .ok_or_else(|| Error::Cast("no active session".to_owned()))?;
    if self.device.is_none() {
      self.ensure_connected(true)?;
    }
    let device = self
      .device
      .as_ref()
      .ok_or_else(|| Error::Cast("warm Cast device not connected".to_owned()))?;
    let request_id = device.message_manager.generate_request_id().get();
    let body = request.payload(request_id, &session_id);
    tracing::info!(device_id = %self.device_id, host = %self.host, port = self.port, %body, "Cast multizone SET_PLAYBACK_DEVICES");
    device
      .message_manager
      .send(rust_cast::message_manager::CastMessage {
        namespace: MULTIZONE_NAMESPACE.to_owned(),
        source: "sender-0".to_owned(),
        destination: "receiver-0".to_owned(),
        payload: rust_cast::message_manager::CastMessagePayload::String(body.to_string()),
      })
      .map_err(|err| Error::Cast(format!("multizone send: {err}")))?;
    let deadline = Instant::now() + COMMAND_TIMEOUT;
    let answer = device.message_manager.receive_find_map(|message| {
      if Instant::now() >= deadline {
        return Err(rust_cast::errors::Error::Internal("multizone answer timed out".to_owned()));
      }
      if message.namespace != MULTIZONE_NAMESPACE {
        return Ok(None);
      }
      let rust_cast::message_manager::CastMessagePayload::String(text) = &message.payload else {
        return Ok(None);
      };
      Ok(parse_set_playback_devices_reply(text, request_id))
    });
    let update = answer.map_err(|err| Error::Cast(format!("multizone answer: {err}")))??;
    tracing::info!(
      device_id = %self.device_id,
      name = ?update.name,
      failed = ?update.failed_devices,
      endpoint = ?update.endpoint,
      "Cast multizone PLAYBACK_SESSION_UPDATED"
    );
    Ok(update)
  }

  /// Release the active session to another worker: no Cast STOP, probes stand down.
  fn handle_take_session(&mut self) -> Option<(ActiveCastSession, Option<MediaLoadRequest>)> {
    let session = self.active.clone()?;
    let load = self.last_load.clone();
    self.clear_active_session();
    self.ownership_stand_down.store(true, Ordering::Release);
    tracing::info!(
      device_id = %self.device_id,
      transport_id = %session.transport_id,
      "Cast media session handed to another worker"
    );
    Some((session, load))
  }

  /// Take over a media session that already plays on this worker's host (moved by multizone).
  ///
  /// Joins the session the way a Cast sender joins by session id: `RECEIVER_STATUS` on this
  /// host, the app running `session_id`, `CONNECT` to that app's transport, then media
  /// `GET_STATUS` for the (possibly renumbered) media session. A move gives the app a new
  /// transport on the new host; a message to the old one makes the receiver close the
  /// connection. A failed read drops the connection so the next command reconnects.
  fn handle_adopt(
    &mut self,
    session: &ActiveCastSession,
    session_id: &str,
    load: Option<MediaLoadRequest>,
  ) -> Result<()> {
    if self.device.is_none() {
      self.ensure_connected(true)?;
    }
    let adopted = match self.read_adopted_session(session, session_id) {
      Ok(adopted) => adopted,
      Err(err) => {
        self.drop_device();
        return Err(err);
      },
    };
    tracing::info!(
      device_id = %self.device_id,
      host = %self.host,
      port = self.port,
      session_id = %adopted.session_id,
      transport_id = %adopted.transport_id,
      previous_transport_id = %session.transport_id,
      media_session_id = adopted.media_session_id,
      "Cast media session adopted on session host"
    );
    self.last_load = load;
    self.last_load_at = Some(Instant::now());
    self.set_active_session(adopted);
    Ok(())
  }

  /// Wire half of [`Self::handle_adopt`]: find, join, and read the moved media session.
  fn read_adopted_session(&self, session: &ActiveCastSession, session_id: &str) -> Result<ActiveCastSession> {
    let device = self
      .device
      .as_ref()
      .ok_or_else(|| Error::Cast("warm Cast device not connected".to_owned()))?;
    let receiver = device
      .receiver
      .get_status()
      .map_err(|err| Error::Cast(format!("adopt receiver status: {err}")))?;
    let app = adoptable_app(&receiver.applications, session_id)?;
    device
      .connection
      .connect(app.transport_id.as_str())
      .map_err(|err| Error::Cast(format!("adopt app connection: {err}")))?;
    let status = device
      .media
      .get_status(app.transport_id.as_str(), None)
      .map_err(|err| Error::Cast(format!("adopt media status: {err}")))?;
    let entry = status
      .entries
      .first()
      .ok_or_else(|| Error::Cast("adopt: host reports no media session".to_owned()))?;
    Ok(ActiveCastSession {
      transport_id: app.transport_id.clone(),
      session_id: app.session_id.clone(),
      media_session_id: entry.media_session_id,
      queue_item_id: entry.current_item_id.map(i32::from).or(session.queue_item_id),
    })
  }

  fn handle_set_volume(&mut self, level: f32) -> Result<()> {
    let clamped = volume_level_clamped(level);
    if self.device.is_none() {
      self.ensure_connected(true)?;
    }
    self.remote.expect_volume_command(Instant::now());
    let first = {
      let device = self
        .device
        .as_ref()
        .ok_or_else(|| Error::Cast("warm Cast device not connected".to_owned()))?;
      device.receiver.set_volume(clamped)
    };
    let first_err = match first {
      Ok(volume) => {
        tracing::debug!(device_id = %self.device_id, ?volume, "warm Cast volume set");
        self.note_own_volume(volume, clamped);
        return Ok(());
      },
      Err(err) => err,
    };
    tracing::warn!(
      device_id = %self.device_id,
      error = %first_err,
      "warm Cast set_volume failed; reconnecting once"
    );
    self.device = None;
    if let Ok(mut slot) = self.relay_slot.lock()
      && let Some(relay) = slot.take()
    {
      relay.shutdown();
    }
    self.ensure_connected(true)?;
    let device = self
      .device
      .as_ref()
      .ok_or_else(|| Error::Cast("warm Cast device not connected".to_owned()))?;
    let volume = device
      .receiver
      .set_volume(clamped)
      .map_err(|e| Error::Cast(format!("warm set volume: {e}")))?;
    tracing::debug!(device_id = %self.device_id, ?volume, "warm Cast volume set after reconnect");
    self.note_own_volume(volume, clamped);
    Ok(())
  }

  /// Baseline for user-change attribution after our own volume command.
  fn note_own_volume(&mut self, volume: rust_cast::channels::receiver::Volume, requested: f32) {
    self
      .remote
      .note_volume(volume.level.unwrap_or(requested), volume.muted.unwrap_or(false));
  }

  /// Read linear volume from `receiver.get_status()` (muted → `0.0`). Retries once on failure.
  fn handle_get_volume(&mut self) -> Result<f32> {
    if self.device.is_none() {
      self.ensure_connected(true)?;
    }
    let first_err = {
      let device = self
        .device
        .as_ref()
        .ok_or_else(|| Error::Cast("warm Cast device not connected".to_owned()))?;
      match device.receiver.get_status() {
        Ok(status) => {
          self
            .remote
            .note_volume(status.volume.level.unwrap_or(0.0), status.volume.muted.unwrap_or(false));
          return Ok(volume_from_cast_status(status.volume));
        },
        Err(err) => err,
      }
    };
    tracing::warn!(
      device_id = %self.device_id,
      error = %first_err,
      "warm Cast get_volume failed; reconnecting once"
    );
    self.device = None;
    if let Ok(mut slot) = self.relay_slot.lock()
      && let Some(relay) = slot.take()
    {
      relay.shutdown();
    }
    self.ensure_connected(true)?;
    let device = self
      .device
      .as_ref()
      .ok_or_else(|| Error::Cast("warm Cast device not connected".to_owned()))?;
    let status = device
      .receiver
      .get_status()
      .map_err(|e| Error::Cast(format!("warm get volume: {e}")))?;
    self
      .remote
      .note_volume(status.volume.level.unwrap_or(0.0), status.volume.muted.unwrap_or(false));
    Ok(volume_from_cast_status(status.volume))
  }

  fn handle_stop(&mut self) -> Result<()> {
    // Capture session for STOP, then fully clear ownership (including reload_attempted)
    // so probes stand down even if the Cast STOP I/O hangs under the hard deadline.
    let stopped = self.active.take();
    self.clear_active_session();
    let Some(session) = stopped else {
      return Ok(());
    };
    let Some(device) = self.device.as_ref() else {
      return Ok(());
    };

    for dest in ["receiver-0", session.transport_id.as_str()] {
      if let Err(err) = device.connection.connect(dest) {
        tracing::debug!(
          device_id = %self.device_id,
          dest,
          error = %err,
          "warm Cast STOP connect failed"
        );
        self.device = None;
        if let Ok(mut slot) = self.relay_slot.lock()
          && let Some(relay) = slot.take()
        {
          relay.shutdown();
        }
        return Err(Error::Cast(format!("warm stop connect {dest}: {err}")));
      }
    }
    // Bound by relay socket read timeout (item 1); avoids infinite receive_status_entry.
    drop(
      device
        .media
        .stop(session.transport_id.as_str(), session.media_session_id)
        .map_err(|err| Error::Cast(format!("warm stop: {err}")))?,
    );
    tracing::debug!(
      device_id = %self.device_id,
      transport_id = %session.transport_id,
      media_session_id = session.media_session_id,
      "warm Cast STOP ok"
    );
    Ok(())
  }

  fn handle_pause(&mut self, queued_at: Instant) -> Result<MediaPlayback> {
    let started = Instant::now();
    let Some((transport_id, media_session_id)) =
      self.active.as_ref().map(|s| (s.transport_id.clone(), s.media_session_id))
    else {
      return Err(Error::Cast("no active session".to_owned()));
    };
    self.remote.expect_player(PlayerIntent::Paused, Instant::now());
    let Some(device) = self.device.as_ref() else {
      return Err(Error::Cast("no active session".to_owned()));
    };
    for dest in ["receiver-0", transport_id.as_str()] {
      if let Err(err) = device.connection.connect(dest) {
        tracing::debug!(device_id = %self.device_id, dest, error = %err, "warm Cast PAUSE connect failed");
        self.device = None;
        if let Ok(mut slot) = self.relay_slot.lock()
          && let Some(relay) = slot.take()
        {
          relay.shutdown();
        }
        return Err(Error::Cast(format!("warm pause connect {dest}: {err}")));
      }
    }
    // Socket timeouts bound receive_status_entry; empty status replies fail the command.
    let entry = device
      .media
      .pause(transport_id.as_str(), media_session_id)
      .map_err(|err| Error::Cast(format!("warm pause: {err}")))?;
    let playback = MediaPlayback::from(&entry);
    tracing::info!(
      device_id = %self.device_id,
      media_session_id,
      queue_wait_ms = started.duration_since(queued_at).as_millis(),
      wire_ms = started.elapsed().as_millis(),
      player_state = ?playback.player_state,
      current_time = ?playback.current_time,
      "warm Cast PAUSE ok"
    );
    Ok(playback)
  }

  /// `MEDIA_STATUS` of the active media session (diagnostics after a PAUSE).
  fn handle_media_status(&self) -> Result<MediaPlayback> {
    let Some((transport_id, media_session_id)) =
      self.active.as_ref().map(|s| (s.transport_id.clone(), s.media_session_id))
    else {
      return Err(Error::Cast("no active session".to_owned()));
    };
    let device = self
      .device
      .as_ref()
      .ok_or_else(|| Error::Cast("warm Cast device not connected".to_owned()))?;
    let status = device
      .media
      .get_status(transport_id.as_str(), Some(media_session_id))
      .map_err(|err| Error::Cast(format!("warm media status: {err}")))?;
    status
      .entries
      .iter()
      .find(|entry| entry.media_session_id == media_session_id)
      .map(MediaPlayback::from)
      .ok_or_else(|| Error::Cast("media status has no entry for our media session".to_owned()))
  }

  fn handle_play(&mut self) -> Result<()> {
    let Some((transport_id, media_session_id)) =
      self.active.as_ref().map(|s| (s.transport_id.clone(), s.media_session_id))
    else {
      return Err(Error::Cast("no active session".to_owned()));
    };
    self.remote.expect_player(PlayerIntent::Playing, Instant::now());
    let Some(device) = self.device.as_ref() else {
      return Err(Error::Cast("no active session".to_owned()));
    };
    for dest in ["receiver-0", transport_id.as_str()] {
      if let Err(err) = device.connection.connect(dest) {
        tracing::debug!(device_id = %self.device_id, dest, error = %err, "warm Cast PLAY connect failed");
        self.device = None;
        if let Ok(mut slot) = self.relay_slot.lock()
          && let Some(relay) = slot.take()
        {
          relay.shutdown();
        }
        return Err(Error::Cast(format!("warm play connect {dest}: {err}")));
      }
    }
    drop(
      device
        .media
        .play(transport_id.as_str(), media_session_id)
        .map_err(|err| Error::Cast(format!("warm play: {err}")))?,
    );
    tracing::debug!(
      device_id = %self.device_id,
      media_session_id,
      "warm Cast PLAY ok"
    );
    Ok(())
  }
}

/// Default Media Receiver app id.
const DEFAULT_MEDIA_RECEIVER_APP_ID: &str = "CC1AD845";

/// The Default Media Receiver instance already running on `device`, if any.
///
/// A multizone group endpoint runs the receiver for the moved session; LOAD there joins it
/// instead of sending a LAUNCH.
fn running_media_receiver(device: &WarmCastDevice) -> Option<rust_cast::channels::receiver::Application> {
  let status = device.receiver.get_status().ok()?;
  status
    .applications
    .into_iter()
    .find(|app| app.app_id == DEFAULT_MEDIA_RECEIVER_APP_ID)
}

/// The receiver app running `session_id` on a host, if it speaks the media namespace.
///
/// Match by session id only (the id `sessionEndpoint` reports, or the one the media had): a
/// moved session keeps its id but gets a new transport, and another sender's app on the same
/// host must never be taken over.
fn adoptable_app<'a>(
  applications: &'a [rust_cast::channels::receiver::Application],
  session_id: &str,
) -> Result<&'a rust_cast::channels::receiver::Application> {
  let app = applications.iter().find(|app| app.session_id == session_id).ok_or_else(|| {
    let running: Vec<(&str, &str)> = applications
      .iter()
      .map(|app| (app.app_id.as_str(), app.session_id.as_str()))
      .collect();
    Error::Cast(format!(
      "adopt: no receiver app runs session {session_id} on the host (apps: {running:?})"
    ))
  })?;
  if !app.namespaces.iter().any(|namespace| namespace == MEDIA_NAMESPACE) {
    return Err(Error::Cast(format!(
      "adopt: app {} running session {session_id} has no media namespace",
      app.app_id
    )));
  }
  Ok(app)
}

struct MediaProbe {
  session_present: Option<bool>,
  idle_reason: Option<IdleReasonKind>,
  buffering: bool,
  failure: Option<ProbeFailureKind>,
}

/// Connect via source-bound TCP + localhost relay; install shutdown into `relay_slot`.
///
/// Builds a [`WarmCastDevice`] with a dual-owned TCP handle so idle drain can set
/// `SO_RCVTIMEO` on the socket that `receive()` actually reads (not the relay peer).
fn connect_cast_device(host: &str, port: u16, relay_slot: &SharedRelaySlot) -> Result<WarmCastDevice> {
  let (relay_host, relay_port, shutdown) = crate::net::spawn_cast_connect_relay(host, port)
    .map_err(|err| Error::Cast(format!("connect {host}:{port}: {err}")))?;
  if let Ok(mut slot) = relay_slot.lock() {
    *slot = Some(shutdown);
  }
  WarmCastDevice::connect_local_relay(relay_port).map_err(|err| {
    // Connect failed after relay up — shut it down.
    if let Ok(mut slot) = relay_slot.lock()
      && let Some(relay) = slot.take()
    {
      relay.shutdown();
    }
    Error::Cast(format!(
      "connect {host}:{port} (via local relay {relay_host}:{relay_port}): {err}"
    ))
  })
}

type CastTlsStream = rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream>;

/// Cast control-plane client with a dual-owned TCP handle for `SO_RCVTIMEO` control.
///
/// Mirrors `rust_cast::CastDevice` (public channel constructors + `MessageManager`) so
/// we can set read timeouts on the same kernel socket the TLS `receive()` path uses.
/// `CastDevice`'s internal stream is private, so the stock connect path cannot do this.
struct WarmCastDevice {
  /// Shared by channels; also used by [`Self::receive`].
  message_manager: std::rc::Rc<rust_cast::message_manager::MessageManager<CastTlsStream>>,
  connection: rust_cast::channels::connection::ConnectionChannel<'static, CastTlsStream>,
  heartbeat: rust_cast::channels::heartbeat::HeartbeatChannel<'static, CastTlsStream>,
  media: rust_cast::channels::media::MediaChannel<'static, CastTlsStream>,
  receiver: rust_cast::channels::receiver::ReceiverChannel<'static, CastTlsStream>,
  /// Clone of the raw TCP socket under the TLS stream; `SO_RCVTIMEO` applies to both.
  tcp_for_timeout: std::net::TcpStream,
}

/// RAII: short `SO_RCVTIMEO` for drain, always restore command I/O timeout on drop.
struct ShortReadTimeout<'a> {
  tcp: &'a std::net::TcpStream,
  restore: Duration,
}

impl Drop for ShortReadTimeout<'_> {
  fn drop(&mut self) {
    if let Err(err) = self.tcp.set_read_timeout(Some(self.restore)) {
      tracing::debug!(error = %err, "restore Cast read timeout failed");
    }
  }
}

impl WarmCastDevice {
  /// TLS connect to the localhost Cast relay with the Cast verifier (any device
  /// certificate, including X.509 v1; handshake signature still checked against its
  /// key — see `cast::tls`), keeping a dual-owned TCP clone.
  fn connect_local_relay(relay_port: u16) -> std::result::Result<Self, String> {
    use rust_cast::channels::connection::ConnectionChannel;
    use rust_cast::channels::heartbeat::HeartbeatChannel;
    use rust_cast::channels::media::MediaChannel;
    use rust_cast::channels::receiver::ReceiverChannel;
    use rust_cast::message_manager::MessageManager;
    use rustls::pki_types::ServerName;
    use rustls::{ClientConnection, StreamOwned};
    use std::net::TcpStream;
    use std::rc::Rc;
    use std::sync::Arc;

    let tcp = TcpStream::connect(("127.0.0.1", relay_port)).map_err(|err| format!("tcp connect: {err}"))?;
    tcp
      .set_read_timeout(Some(crate::net::CAST_RELAY_IO_TIMEOUT))
      .map_err(|err| format!("set_read_timeout: {err}"))?;
    tcp
      .set_write_timeout(Some(crate::net::CAST_RELAY_IO_TIMEOUT))
      .map_err(|err| format!("set_write_timeout: {err}"))?;
    let tcp_for_timeout = tcp.try_clone().map_err(|err| format!("tcp try_clone: {err}"))?;

    let config = super::tls::client_config().map_err(|err| format!("tls config: {err}"))?;
    let server_name = ServerName::try_from("127.0.0.1")
      .map_err(|err| format!("server name: {err}"))?
      .to_owned();
    let conn = ClientConnection::new(Arc::new(config), server_name).map_err(|err| format!("tls client: {err}"))?;
    let ssl = StreamOwned::new(conn, tcp);

    let message_manager = Rc::new(MessageManager::new(ssl));
    let connection = ConnectionChannel::new("sender-0", Rc::clone(&message_manager));
    let heartbeat = HeartbeatChannel::new("sender-0", "receiver-0", Rc::clone(&message_manager));
    let receiver = ReceiverChannel::new("sender-0", "receiver-0", Rc::clone(&message_manager));
    let media = MediaChannel::new("sender-0", Rc::clone(&message_manager));

    Ok(Self {
      message_manager,
      connection,
      heartbeat,
      media,
      receiver,
      tcp_for_timeout,
    })
  }

  /// Temporarily set `SO_RCVTIMEO` for idle drain; restores command timeout on drop.
  fn begin_short_read(&self, timeout: Duration) -> ShortReadTimeout<'_> {
    if let Err(err) = self.tcp_for_timeout.set_read_timeout(Some(timeout)) {
      tracing::debug!(error = %err, "set Cast drain read timeout failed");
    }
    ShortReadTimeout {
      tcp: &self.tcp_for_timeout,
      restore: crate::net::CAST_RELAY_IO_TIMEOUT,
    }
  }

  /// Same dispatch as `rust_cast::CastDevice::receive`.
  fn receive(&self) -> std::result::Result<rust_cast::ChannelMessage, rust_cast::errors::Error> {
    let cast_message = self.message_manager.receive()?;
    if self.connection.can_handle(&cast_message) {
      return Ok(rust_cast::ChannelMessage::Connection(self.connection.parse(&cast_message)?));
    }
    if self.heartbeat.can_handle(&cast_message) {
      return Ok(rust_cast::ChannelMessage::Heartbeat(self.heartbeat.parse(&cast_message)?));
    }
    if self.media.can_handle(&cast_message) {
      return Ok(rust_cast::ChannelMessage::Media(self.media.parse(&cast_message)?));
    }
    if self.receiver.can_handle(&cast_message) {
      return Ok(rust_cast::ChannelMessage::Receiver(self.receiver.parse(&cast_message)?));
    }
    Ok(rust_cast::ChannelMessage::Raw(cast_message))
  }
}

fn short_id(device_id: &str) -> &str {
  let end = device_id.len().min(12);
  device_id.get(..end).unwrap_or(device_id)
}

/// Clear the warm device client after a hard-deadline fire (pure for unit tests).
#[must_use]
pub(crate) fn apply_hard_deadline_device_clear<T>(device: &mut Option<T>) -> bool {
  if device.is_some() {
    *device = None;
    true
  } else {
    false
  }
}

/// Whether Cast ownership probes must stand down.
///
/// Returns true when there is no tracked active load, or when the bridge marked the
/// session ended (`session_ended` flag) so a missing media session is not treated as
/// a steal by another app.
#[must_use]
pub(crate) const fn ownership_probe_stands_down(active_is_none: bool, bridge_ended: bool) -> bool {
  active_is_none || bridge_ended
}

/// Whether `remove` may drop the worker map entry after a join attempt.
///
/// On timeout the entry stays as an exiting tombstone until a reaper joins.
#[must_use]
pub(crate) const fn remove_clears_map_entry(join_completed: bool) -> bool {
  join_completed
}

/// Whether `ensure` may spawn a new worker for an existing map slot.
#[must_use]
pub(crate) const fn ensure_may_spawn_for_slot(exiting: bool) -> bool {
  !exiting
}

/// Next `reload_attempted` flag after an internal re-LOAD attempt.
///
/// Spec: one re-LOAD **per trouble episode**. Success ends the episode (`false`);
/// only failure marks the episode so a second Recoverable IDLE can escalate.
#[must_use]
pub(crate) const fn reload_attempted_after_internal_reload(success: bool) -> bool {
  !success
}

/// Join a worker thread without blocking forever if it is stuck in I/O.
///
/// On success, removes `device_id` from `workers`. On timeout, leaves the exiting
/// tombstone in the map and spawns a reaper that removes it after the late join.
fn join_and_reap_worker(
  thread: JoinHandle<()>,
  timeout: Duration,
  device_id: String,
  workers: Arc<Mutex<HashMap<String, CastWorkerHandle>>>,
) {
  let (done_tx, done_rx) = mpsc::sync_channel(1);
  let waiter = std::thread::spawn(move || {
    drop(thread.join());
    let _send = done_tx.send(());
  });
  let join_completed = done_rx.recv_timeout(timeout).is_ok();
  if remove_clears_map_entry(join_completed) {
    drop(waiter.join());
    drop(workers.lock().remove(&device_id));
    tracing::info!(%device_id, "warm Cast worker removed");
  } else {
    tracing::warn!(
      %device_id,
      timeout_ms = timeout.as_millis(),
      "warm Cast worker join timed out; keeping map entry until exit"
    );
    let reaper_name = format!("cast-reap-{}", short_id(&device_id));
    drop(std::thread::Builder::new().name(reaper_name).spawn(move || {
      drop(waiter.join());
      drop(workers.lock().remove(&device_id));
      tracing::info!(%device_id, "warm Cast worker removed after late join");
    }));
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::cast::CastStreamKind;
  use std::time::Instant;

  fn app(
    app_id: &str,
    session_id: &str,
    transport_id: &str,
    media: bool,
  ) -> rust_cast::channels::receiver::Application {
    rust_cast::channels::receiver::Application {
      app_id: app_id.to_owned(),
      session_id: session_id.to_owned(),
      transport_id: transport_id.to_owned(),
      namespaces: if media {
        vec![MEDIA_NAMESPACE.to_owned()]
      } else {
        Vec::new()
      },
      display_name: "Default Media Receiver".to_owned(),
      status_text: String::new(),
    }
  }

  /// A moved session is found by its session id and controlled on the transport the new host
  /// reports, never the old one; other apps and media-less stubs are never adopted.
  #[test]
  fn adoptable_app_matches_session_id_and_needs_media() {
    let apps = vec![
      app("CC1AD845", "other-sender", "t-other", true),
      app("CC1AD845", "session-a", "t-new", true),
    ];
    let found = adoptable_app(&apps, "session-a").expect("session-a runs");
    assert_eq!(found.transport_id, "t-new");

    let missing = adoptable_app(&apps, "session-b").expect_err("no such session");
    assert!(
      missing.to_string().contains("no receiver app runs session session-b"),
      "{missing}"
    );

    let stub = vec![app("531A4F84", "session-a", "t-stub", false)];
    let no_media = adoptable_app(&stub, "session-a").expect_err("stub app has no media");
    assert!(no_media.to_string().contains("no media namespace"), "{no_media}");
  }

  mod remote_tracker {
    use super::super::{CastRemoteKind, OWN_COMMAND_SETTLE, PlayerIntent, RemoteTracker};
    use rust_cast::channels::media::PlayerState;
    use std::time::{Duration, Instant};

    fn after_settle(t: Instant) -> Instant {
      t + OWN_COMMAND_SETTLE + Duration::from_millis(1)
    }

    #[test]
    fn own_pause_and_play_are_not_reported() {
      let t0 = Instant::now();
      let mut tracker = RemoteTracker::default();
      tracker.expect_player(PlayerIntent::Playing, t0);
      assert_eq!(tracker.observe_player(PlayerState::Buffering, t0), None);
      assert_eq!(tracker.observe_player(PlayerState::Playing, after_settle(t0)), None);

      let t1 = after_settle(t0);
      tracker.expect_player(PlayerIntent::Paused, t1);
      assert_eq!(tracker.observe_player(PlayerState::Paused, t1), None, "echo of our PAUSE");
      assert_eq!(tracker.observe_player(PlayerState::Paused, after_settle(t1)), None);
    }

    #[test]
    fn device_pause_then_play_reported_once_each() {
      let t0 = Instant::now();
      let mut tracker = RemoteTracker::default();
      tracker.expect_player(PlayerIntent::Playing, t0);
      let t1 = after_settle(t0);
      assert_eq!(tracker.observe_player(PlayerState::Paused, t1), Some(CastRemoteKind::Paused));
      assert_eq!(tracker.observe_player(PlayerState::Paused, t1), None, "reported once");
      assert_eq!(tracker.observe_player(PlayerState::Playing, t1), Some(CastRemoteKind::Played));
      assert_eq!(tracker.observe_player(PlayerState::Idle, t1), None);
    }

    #[test]
    fn stale_status_inside_settle_window_is_ignored_then_caught() {
      let t0 = Instant::now();
      let mut tracker = RemoteTracker::default();
      tracker.expect_player(PlayerIntent::Playing, t0);
      let t1 = after_settle(t0);
      tracker.expect_player(PlayerIntent::Paused, t1);
      // A PLAYING broadcast sent before our PAUSE took effect.
      assert_eq!(tracker.observe_player(PlayerState::Playing, t1), None);
      // If the device really is playing after the window, the user resumed it.
      assert_eq!(
        tracker.observe_player(PlayerState::Playing, after_settle(t1)),
        Some(CastRemoteKind::Played)
      );
    }

    #[test]
    fn no_player_reports_without_live_session() {
      let t0 = Instant::now();
      let mut tracker = RemoteTracker::default();
      assert_eq!(tracker.observe_player(PlayerState::Paused, t0), None);
      tracker.expect_player(PlayerIntent::Playing, t0);
      tracker.clear_player();
      assert_eq!(tracker.observe_player(PlayerState::Paused, after_settle(t0)), None);
    }

    #[test]
    fn volume_echo_suppressed_and_user_change_reported() {
      let t0 = Instant::now();
      let mut tracker = RemoteTracker::default();
      // First observation is only a baseline.
      assert_eq!(tracker.observe_volume(0.4, false, t0), None);
      // Our own set: baseline moves, broadcasts of it (and stale ones in the window) are quiet.
      tracker.expect_volume_command(t0);
      tracker.note_volume(0.7, false);
      assert_eq!(tracker.observe_volume(0.4, false, t0), None, "stale pre-command status");
      let t1 = after_settle(t0);
      assert_eq!(tracker.observe_volume(0.700_1, false, t1), None, "echo of our level");
      // Device buttons.
      assert_eq!(
        tracker.observe_volume(0.8, false, t1),
        Some(CastRemoteKind::Volume { level: 0.8, muted: false })
      );
      // Mute from the Google Home app.
      assert_eq!(
        tracker.observe_volume(0.8, true, t1),
        Some(CastRemoteKind::Volume { level: 0.8, muted: true })
      );
      assert_eq!(tracker.observe_volume(0.8, true, t1), None);
    }
  }

  fn sample_device(id: &str) -> Device {
    Device {
      id: id.to_owned(),
      name: "Test Nest".to_owned(),
      model: String::new(),
      host: "127.0.0.1".to_owned(),
      hostname: "test.local".to_owned(),
      // Discard port — connect fails fast; worker still starts and accepts Shutdown.
      port: 9,
      last_seen: Instant::now(),
      instance: id.to_owned(),
      pending_leave_deadline: None,
      pending_leave_since: None,
    }
  }

  fn base_inputs() -> OwnershipInputs {
    OwnershipInputs {
      transport_listed: true,
      media_session_present: Some(true),
      idle_reason: None,
      buffering: false,
      buffering_streak: 0,
      media_failure: None,
      reload_attempted: false,
      load_within_guard: false,
    }
  }

  #[test]
  fn drain_read_timeout_is_at_most_100ms() {
    assert!(
      DRAIN_READ_TIMEOUT <= Duration::from_millis(100),
      "idle drain SO_RCVTIMEO must be ≤100ms (got {DRAIN_READ_TIMEOUT:?})"
    );
    assert!(
      DRAIN_READ_TIMEOUT < crate::net::CAST_RELAY_IO_TIMEOUT,
      "drain timeout must be far below the 12s command I/O timeout"
    );
  }

  /// Silent peer: short `SO_RCVTIMEO` on the **client** socket must return fast
  /// (the pre-fix path set the relay peer end, which never bounded `receive()`).
  #[test]
  fn silent_socket_short_read_timeout_completes_fast() {
    use std::io::Read;
    use std::net::{TcpListener, TcpStream};
    use std::time::Instant;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    // Accept but never write — models empty Cast idle drain.
    let _acceptor = std::thread::spawn(move || {
      let (_stream, _) = listener.accept().expect("accept");
      std::thread::sleep(Duration::from_secs(30));
    });
    let mut client = TcpStream::connect(addr).expect("connect");
    client.set_read_timeout(Some(DRAIN_READ_TIMEOUT)).expect("set_read_timeout");
    // Dual-owned clone (same as WarmCastDevice::tcp_for_timeout).
    let timeout_handle = client.try_clone().expect("clone");
    timeout_handle
      .set_read_timeout(Some(DRAIN_READ_TIMEOUT))
      .expect("clone set_read_timeout");

    let start = Instant::now();
    let mut buf = [0_u8; 1];
    let result = client.read(&mut buf);
    let elapsed = start.elapsed();

    assert!(result.is_err(), "silent socket should not yield data, got {result:?}");
    // Bound well under the old 12s relay I/O timeout and the 30s hard deadline.
    assert!(
      elapsed < Duration::from_secs(2),
      "short SO_RCVTIMEO on client socket must complete fast, took {elapsed:?}"
    );

    // RAII restore pattern: after short drain, restore command timeout.
    {
      let _guard = ShortReadTimeout {
        tcp: &timeout_handle,
        restore: crate::net::CAST_RELAY_IO_TIMEOUT,
      };
    }
    let restored = timeout_handle.read_timeout().expect("get timeout");
    assert_eq!(
      restored,
      Some(crate::net::CAST_RELAY_IO_TIMEOUT),
      "ShortReadTimeout Drop must restore CAST_RELAY_IO_TIMEOUT"
    );
  }

  #[test]
  fn heartbeat_interval_is_about_four_seconds() {
    assert_eq!(
      HEARTBEAT_INTERVAL,
      Duration::from_secs(4),
      "PING cadence must stay ~4s after idle-drain fix"
    );
  }

  #[test]
  fn is_reachable_false_without_worker() {
    let pool = CastPool::new(None);
    assert!(
      !pool.is_reachable("missing-device"),
      "no worker must report unreachable (Linux leave backstop)"
    );
  }

  #[test]
  fn is_reachable_false_while_exiting_tombstone() {
    let pool = CastPool::new(None);
    let device = Device::new("tomb", "Tomb", "127.0.0.1", "tomb.local", 8009, "tomb");
    {
      let (cmd_tx, _cmd_rx) = mpsc::channel();
      let mut workers = pool.workers.lock();
      drop(workers.insert(
        device.id.clone(),
        CastWorkerHandle {
          cmd_tx,
          thread: None,
          host: device.host.clone(),
          hostname: device.hostname.clone(),
          port: device.port,
          relay_slot: Arc::new(StdMutex::new(None)),
          exiting: true,
          reachable: Arc::new(AtomicBool::new(true)),
          terminal_failed: Arc::new(AtomicBool::new(false)),
          ownership_stand_down: Arc::new(AtomicBool::new(false)),
          session_host: false,
        },
      ));
    }
    assert!(!pool.is_reachable(&device.id), "exiting tombstone must not count as reachable");
  }

  #[test]
  fn cast_transport_still_owned_true_when_present() {
    let apps = ["receiver-0", "our-transport", "other"];
    assert!(cast_transport_still_owned(&apps, "our-transport"));
  }

  #[test]
  fn cast_transport_still_owned_false_when_absent() {
    let apps = ["youtube-app", "receiver-0"];
    assert!(!cast_transport_still_owned(&apps, "our-transport"));
  }

  #[test]
  fn cast_transport_still_owned_false_when_empty() {
    assert!(!cast_transport_still_owned(&[], "our-transport"));
  }

  #[test]
  fn ownership_interrupted_is_lost_signal() {
    let mut i = base_inputs();
    i.idle_reason = Some(IdleReasonKind::Interrupted);
    assert_eq!(ownership_action(i), OwnershipAction::LostSignal);
  }

  #[test]
  fn ownership_playing_is_owned() {
    assert_eq!(ownership_action(base_inputs()), OwnershipAction::Owned);
  }

  #[test]
  fn ownership_missing_session_is_lost() {
    let mut i = base_inputs();
    i.media_session_present = Some(false);
    assert_eq!(ownership_action(i), OwnershipAction::LostSignal);
  }

  #[test]
  fn ownership_parse_failure_is_inconclusive() {
    let mut i = base_inputs();
    i.media_session_present = None;
    i.media_failure = Some(ProbeFailureKind::Parse);
    assert_eq!(ownership_action(i), OwnershipAction::Inconclusive);
  }

  #[test]
  fn ownership_transport_failure_is_suspect() {
    let mut i = base_inputs();
    i.media_session_present = None;
    i.media_failure = Some(ProbeFailureKind::Transport);
    assert_eq!(ownership_action(i), OwnershipAction::SuspectReconnect);
  }

  #[test]
  fn ownership_recoverable_idle_attempts_reload_once() {
    let mut i = base_inputs();
    i.idle_reason = Some(IdleReasonKind::Recoverable);
    assert_eq!(ownership_action(i), OwnershipAction::AttemptReload);
    i.reload_attempted = true;
    assert_eq!(ownership_action(i), OwnershipAction::LostSignal);
  }

  #[test]
  fn ownership_recoverable_skips_reload_when_load_recent() {
    let mut i = base_inputs();
    i.idle_reason = Some(IdleReasonKind::Recoverable);
    i.load_within_guard = true;
    // Guard means wait for the bridge rollover LOAD — not LostSignal / AirPlay kick.
    assert_eq!(ownership_action(i), OwnershipAction::Inconclusive);
  }

  #[test]
  fn ownership_buffering_stuck_skips_reload_when_load_recent() {
    let mut i = base_inputs();
    i.buffering = true;
    i.buffering_streak = 3;
    i.load_within_guard = true;
    assert_eq!(ownership_action(i), OwnershipAction::Inconclusive);
  }

  #[test]
  fn ownership_buffering_stuck_attempts_reload() {
    let mut i = base_inputs();
    i.buffering = true;
    i.buffering_streak = 2;
    assert_eq!(ownership_action(i), OwnershipAction::Owned);
    i.buffering_streak = 3;
    assert_eq!(ownership_action(i), OwnershipAction::AttemptReload);
  }

  #[test]
  fn reload_attempted_cleared_on_success_set_on_failure() {
    assert!(!reload_attempted_after_internal_reload(true));
    assert!(reload_attempted_after_internal_reload(false));
  }

  #[test]
  fn remove_clears_map_only_when_join_completes() {
    assert!(remove_clears_map_entry(true));
    assert!(!remove_clears_map_entry(false));
  }

  #[test]
  fn ensure_may_not_spawn_while_exiting() {
    assert!(ensure_may_spawn_for_slot(false));
    assert!(!ensure_may_spawn_for_slot(true));
  }

  #[test]
  fn hard_deadline_fire_clears_device_option() {
    let mut device = Some(42_u8);
    assert!(apply_hard_deadline_device_clear(&mut device));
    assert!(device.is_none());
    assert!(!apply_hard_deadline_device_clear(&mut device));
  }

  #[test]
  fn ensure_does_not_spawn_over_exiting_tombstone() {
    let pool = CastPool::new(None);
    let device = sample_device("nest-tombstone");
    {
      let (cmd_tx, _cmd_rx) = mpsc::channel();
      drop(pool.workers.lock().insert(
        device.id.clone(),
        CastWorkerHandle {
          cmd_tx,
          thread: None,
          host: device.host.clone(),
          hostname: device.hostname.clone(),
          port: device.port,
          relay_slot: Arc::new(StdMutex::new(None)),
          exiting: true,
          reachable: Arc::new(AtomicBool::new(false)),
          terminal_failed: Arc::new(AtomicBool::new(false)),
          ownership_stand_down: Arc::new(AtomicBool::new(false)),
          session_host: false,
        },
      ));
    }
    pool.ensure(&device);
    let workers = pool.workers.lock();
    let handle = workers.get(&device.id).expect("tombstone kept");
    assert!(handle.exiting, "ensure must not clear exiting flag");
    assert!(handle.thread.is_none(), "ensure must not spawn a second worker");
    drop(workers);
  }

  #[test]
  fn ownership_transport_not_listed_is_lost() {
    let mut i = base_inputs();
    i.transport_listed = false;
    i.media_session_present = None;
    assert_eq!(ownership_action(i), OwnershipAction::LostSignal);
  }

  #[test]
  fn reconnect_backoff_doubles_and_caps() {
    let d0 = reconnect_backoff_delay(0, 42);
    let d1 = reconnect_backoff_delay(1, 42);
    let d2 = reconnect_backoff_delay(2, 42);
    let d_big = reconnect_backoff_delay(20, 42);
    assert!(d0.as_millis() >= 800 && d0.as_millis() <= 1200, "d0={d0:?}");
    assert!(d1.as_millis() >= 1600 && d1.as_millis() <= 2400, "d1={d1:?}");
    assert!(d2.as_millis() >= 3200 && d2.as_millis() <= 4800, "d2={d2:?}");
    assert!(d_big.as_millis() <= 72_000, "cap with jitter d_big={d_big:?}");
    assert!(d_big.as_millis() >= 48_000, "near cap d_big={d_big:?}");
  }

  #[test]
  fn reconnect_backoff_jitter_varies_with_seed() {
    let a = reconnect_backoff_delay(3, 1);
    let b = reconnect_backoff_delay(3, 99999);
    // Same base (8s) but jitter may differ; at least function is deterministic per seed.
    assert_eq!(reconnect_backoff_delay(3, 1), a);
    assert!(a.as_millis() >= 6400 && a.as_millis() <= 9600);
    assert!(b.as_millis() >= 6400 && b.as_millis() <= 9600);
  }

  #[test]
  fn classify_io_is_transport() {
    let err = rust_cast::errors::Error::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "t"));
    assert_eq!(classify_cast_probe_error(&err), ProbeFailureKind::Transport);
  }

  #[test]
  fn classify_timeout_variant_is_transport() {
    let err = rust_cast::errors::Error::Timeout("waited".into());
    assert_eq!(classify_cast_probe_error(&err), ProbeFailureKind::Transport);
  }

  #[test]
  fn classify_parsing_is_parse() {
    let err = rust_cast::errors::Error::Parsing("bad field".into());
    assert_eq!(classify_cast_probe_error(&err), ProbeFailureKind::Parse);
  }

  #[test]
  fn classify_internal_unknown_state_is_parse() {
    let err = rust_cast::errors::Error::Internal("Unknown player state FOO".into());
    assert_eq!(classify_cast_probe_error(&err), ProbeFailureKind::Parse);
  }

  fn wait_until_worker_gone(pool: &CastPool, device_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
      if !pool.device_ids().contains(&device_id.to_owned()) {
        return;
      }
      std::thread::sleep(Duration::from_millis(25));
    }
    panic!("worker {device_id} still registered after remove/reaper window");
  }

  #[test]
  fn worker_shutdown_joins_without_panic() {
    let pool = CastPool::new(None);
    let device = sample_device("nest-shutdown-test");
    pool.ensure(&device);
    assert!(pool.device_ids().contains(&device.id));
    pool.remove(&device.id);
    // Map entry may remain as an exiting tombstone until the worker (or reaper) finishes.
    wait_until_worker_gone(&pool, &device.id);
  }

  #[test]
  fn load_without_worker_errors() {
    let pool = CastPool::new(None);
    let request = MediaLoadRequest::wav("http://127.0.0.1:9/stream", CastStreamKind::Buffered);
    let err = pool.load("missing-device", request).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("no warm Cast worker"), "expected missing-worker error, got: {msg}");
  }

  #[test]
  fn stop_best_effort_without_worker_is_noop() {
    let pool = CastPool::new(None);
    assert!(!pool.stop_best_effort("no-such", Duration::from_millis(100)));
  }

  #[test]
  fn session_ended_without_worker_is_noop() {
    let pool = CastPool::new(None);
    pool.session_ended("no-such");
  }

  #[test]
  fn ownership_probe_stands_down_without_active_or_after_bridge_end() {
    assert!(ownership_probe_stands_down(true, false), "no tracked session ⇒ stand down");
    assert!(
      ownership_probe_stands_down(false, true),
      "bridge session_ended flag ⇒ stand down even if active still set"
    );
    assert!(ownership_probe_stands_down(true, true), "both clear and flag ⇒ stand down");
    assert!(
      !ownership_probe_stands_down(false, false),
      "active session without bridge end ⇒ probes still run (genuine steal path)"
    );
  }

  /// Sequence: bridge ends session → stand-down gate → missing media is not a steal.
  ///
  /// While still tracking an active load, absent media is `LostSignal`. After
  /// `session_ended` / clear, the probe gate stands down so `ownership_action` is
  /// never consulted and no `ownership_lost` kick is emitted.
  #[test]
  fn bridge_session_end_stands_down_before_missing_media_looks_stolen() {
    let mut stolen = base_inputs();
    stolen.media_session_present = Some(false);
    stolen.transport_listed = false;
    assert_eq!(
      ownership_action(stolen),
      OwnershipAction::LostSignal,
      "while still tracking active, missing media must look like a steal"
    );

    // After session_ended: active cleared and/or bridge_ended flag set.
    assert!(
      ownership_probe_stands_down(/* active_is_none */ true, /* bridge_ended */ false),
      "clear_active_session path stands down"
    );
    assert!(
      ownership_probe_stands_down(/* active_is_none */ false, /* bridge_ended */ true),
      "instant stand-down flag path (cmd still queued) stands down"
    );
  }

  #[test]
  fn session_ended_on_live_worker_does_not_emit_ownership_lost() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let pool = CastPool::new(Some(tx));
    let device = sample_device("session-ended-no-steal");
    pool.ensure(&device);
    // Bridge-initiated end while no media was ever loaded: must not kick.
    pool.session_ended(&device.id);
    // Allow worker to drain SessionEnded.
    std::thread::sleep(Duration::from_millis(100));
    assert!(rx.try_recv().is_err(), "session_ended must not emit ownership_lost");
    pool.shutdown();
    wait_until_worker_gone(&pool, &device.id);
  }

  #[test]
  fn ensure_idempotent_same_endpoint() {
    let pool = CastPool::new(None);
    let device = sample_device("nest-idempotent");
    pool.ensure(&device);
    pool.ensure(&device);
    assert_eq!(pool.device_ids().len(), 1);
    pool.shutdown();
    wait_until_worker_gone(&pool, &device.id);
    assert_eq!(pool.device_ids(), Vec::<String>::new());
  }

  #[test]
  fn remove_unknown_is_ok() {
    let pool = CastPool::new(None);
    pool.remove("never-existed");
  }

  #[test]
  fn volume_from_cast_status_muted_is_zero() {
    let muted = rust_cast::channels::receiver::Volume { level: Some(0.8), muted: Some(true) };
    assert!((volume_from_cast_status(muted) - 0.0).abs() < f32::EPSILON);
  }

  #[test]
  fn volume_from_cast_status_level_clamped() {
    let high = rust_cast::channels::receiver::Volume { level: Some(1.5), muted: Some(false) };
    assert!((volume_from_cast_status(high) - 1.0).abs() < f32::EPSILON);
    let mid = rust_cast::channels::receiver::Volume { level: Some(0.42), muted: None };
    assert!((volume_from_cast_status(mid) - 0.42).abs() < f32::EPSILON);
    let missing = rust_cast::channels::receiver::Volume { level: None, muted: None };
    assert!((volume_from_cast_status(missing) - 0.0).abs() < f32::EPSILON);
  }

  #[test]
  fn media_recovered_builder_is_additive() {
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let pool = CastPool::new(None).with_media_recovered(tx);
    assert!(format!("{pool:?}").contains("media_recovered_watch: true"));
  }

  // --- B1: late LOAD abandon must not install active ---

  #[test]
  fn load_commit_installs_when_not_abandoned() {
    assert_eq!(load_commit_decision(false), LoadCommitDecision::InstallActive);
  }

  #[test]
  fn load_commit_abandons_and_stops_when_caller_timed_out() {
    assert_eq!(load_commit_decision(true), LoadCommitDecision::AbandonAndStop);
  }

  #[test]
  fn late_success_after_abandon_must_not_keep_active() {
    // Models the worker post-load check: abandoned means stop, never treat as success.
    let abandoned = true;
    assert!(matches!(load_commit_decision(abandoned), LoadCommitDecision::AbandonAndStop));
    // Caller timeout sets the flag; a late wire success must take the abandon path.
    let flag = AtomicBool::new(false);
    flag.store(true, Ordering::Release);
    assert_eq!(
      load_commit_decision(flag.load(Ordering::Acquire)),
      LoadCommitDecision::AbandonAndStop
    );
  }

  // --- B4: terminal Cast TLS failures ---

  #[test]
  fn tls_peer_incompatible_is_terminal() {
    let msg = "connect 192.168.1.206:8009 (via local relay 127.0.0.1:9): \
      warm connection channel: peer is incompatible: NoCipherSuitesInCommon";
    assert_eq!(classify_cast_control_message(msg), CastFailureClass::Terminal);
    assert_eq!(
      classify_cast_control_error(&Error::Cast(msg.to_owned())),
      CastFailureClass::Terminal
    );
  }

  #[test]
  fn certificate_errors_are_transient() {
    // Certificate problems must never hide a device for the process lifetime.
    for msg in [
      "warm connection channel: invalid peer certificate: Other(OtherError(UnsupportedCertVersion))",
      "warm connection channel: invalid peer certificate: BadSignature",
    ] {
      assert_eq!(classify_cast_control_message(msg), CastFailureClass::Transient, "{msg}");
    }
  }

  #[test]
  fn transport_timeout_is_transient() {
    assert_eq!(
      classify_cast_control_message("warm Cast load timed out after 6s for abc"),
      CastFailureClass::Transient
    );
    assert_eq!(
      classify_cast_control_message("connect 192.168.1.1:8009: connection refused"),
      CastFailureClass::Transient
    );
  }

  #[test]
  fn is_terminal_failed_false_without_worker() {
    let pool = CastPool::new(None);
    assert!(!pool.is_terminal_failed("no-such-device"));
  }
}
