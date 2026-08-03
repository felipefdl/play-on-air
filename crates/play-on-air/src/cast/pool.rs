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
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex as StdMutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::mpsc::UnboundedSender;

use crate::error::{Error, Result};
use crate::net::CastRelayShutdown;
use crate::registry::Device;

use super::{ActiveCastSession, MediaLoadRequest, media_session_id_from_status, volume_level_clamped};

/// Map Cast `Volume` from `get_status` to linear `0.0..=1.0` (muted → `0.0`).
fn volume_from_cast_status(volume: rust_cast::channels::receiver::Volume) -> f32 {
  if volume.muted.unwrap_or(false) {
    return 0.0;
  }
  volume_level_clamped(volume.level.unwrap_or(0.0))
}

/// Heartbeat / PING cadence on the warm control-plane connection.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(4);

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

const fn recoverable_or_lost(reload_attempted: bool, load_within_guard: bool) -> OwnershipAction {
  if !reload_attempted && !load_within_guard {
    OwnershipAction::AttemptReload
  } else {
    OwnershipAction::LostSignal
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
  Load {
    request: MediaLoadRequest,
    reply: SyncSender<Result<ActiveCastSession>>,
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
  Pause { reply: SyncSender<Result<()>> },
  /// Resume a paused media session.
  Play { reply: SyncSender<Result<()>> },
  /// Best-effort stop; always replies after attempt (or immediately if no session).
  StopBestEffort { reply: SyncSender<()> },
  /// Exit the worker loop and drop the Cast device.
  Shutdown,
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
}

/// Shared pool of warm Cast control-plane workers (`Send + Sync` via `Arc`).
///
/// Workers are started when devices appear (see app maintain loop) and torn down
/// only when the device leaves — not when an AirPlay media session ends.
pub struct CastPool {
  workers: Mutex<HashMap<String, CastWorkerHandle>>,
  /// Notifies the app when a warm LOAD session loses Cast media ownership
  /// (another app took the receiver). Payload is the registry device id.
  ownership_lost: Option<UnboundedSender<String>>,
  /// Notifies when an internal re-LOAD recovered media after recoverable IDLE.
  media_recovered: Option<UnboundedSender<String>>,
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
      workers: Mutex::new(HashMap::new()),
      ownership_lost,
      media_recovered: None,
    }
  }

  /// Attach a channel for internal media recovery events (additive).
  #[must_use]
  pub fn with_media_recovered(mut self, media_recovered: UnboundedSender<String>) -> Self {
    self.media_recovered = Some(media_recovered);
    self
  }

  /// Ensure a warm worker exists for `device` (start one if missing).
  ///
  /// Updates host / hostname / port when the registry entry changes.
  /// Does **not** spawn a second worker while one is still registered (including
  /// during join after shutdown signal).
  pub fn ensure(&self, device: &Device) {
    let mut guard = self.workers.lock();
    if let Some(handle) = guard.get_mut(&device.id) {
      let host_changed = handle.host != device.host || handle.hostname != device.hostname || handle.port != device.port;
      if host_changed {
        handle.host.clone_from(&device.host);
        handle.hostname.clone_from(&device.hostname);
        handle.port = device.port;
        drop(handle.cmd_tx.send(CastWorkerCmd::UpdateEndpoint {
          host: device.host.clone(),
          hostname: device.hostname.clone(),
          port: device.port,
        }));
      }
      drop(guard);
      return;
    }

    let (cmd_tx, cmd_rx) = mpsc::channel();
    let device_id = device.id.clone();
    let host = device.host.clone();
    let hostname = device.hostname.clone();
    let port = device.port;
    let ownership_lost = self.ownership_lost.clone();
    let media_recovered = self.media_recovered.clone();
    let relay_slot: SharedRelaySlot = Arc::new(StdMutex::new(None));
    let relay_slot_worker = Arc::clone(&relay_slot);
    let thread_name = format!("cast-warm-{}", short_id(&device_id));
    let join = std::thread::Builder::new()
      .name(thread_name)
      .spawn(move || {
        worker_main(
          device_id,
          host,
          hostname,
          port,
          cmd_rx,
          ownership_lost,
          media_recovered,
          relay_slot_worker,
        );
      })
      .ok();

    if join.is_none() {
      drop(guard);
      tracing::error!(id = %device.id, "failed to spawn warm Cast worker thread");
      return;
    }

    drop(guard.insert(
      device.id.clone(),
      CastWorkerHandle {
        cmd_tx,
        thread: join,
        host: device.host.clone(),
        hostname: device.hostname.clone(),
        port: device.port,
        relay_slot,
      },
    ));
    drop(guard);
    tracing::debug!(id = %device.id, host = %device.host, port = device.port, "warm Cast worker started");
  }

  /// Shut down and remove the worker for `device_id` (device left the network).
  ///
  /// Forces relay socket shutdown so a blocked `rust_cast` read unblocks; keeps the
  /// map entry until join completes so [`Self::ensure`] cannot spawn a duplicate.
  pub fn remove(&self, device_id: &str) {
    let join_handle = {
      let mut workers = self.workers.lock();
      let Some(worker) = workers.get_mut(device_id) else {
        return;
      };
      // Unblock any stuck read before/while Shutdown is processed.
      if let Ok(mut slot) = worker.relay_slot.lock()
        && let Some(relay) = slot.take()
      {
        relay.shutdown();
      }
      drop(worker.cmd_tx.send(CastWorkerCmd::Shutdown));
      let handle = worker.thread.take();
      drop(workers);
      handle
    };
    if let Some(handle) = join_handle {
      join_with_timeout(handle, JOIN_TIMEOUT, device_id);
    }
    drop(self.workers.lock().remove(device_id));
    tracing::info!(%device_id, "warm Cast worker removed");
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
    guard.keys().cloned().collect()
  }

  /// LOAD media on the warm control plane for `device_id`.
  ///
  /// Blocks up to [`COMMAND_TIMEOUT`]. If the warm TCP is dead, the worker tries
  /// reconnect with backoff before failing the command.
  pub fn load(&self, device_id: &str, request: MediaLoadRequest) -> Result<ActiveCastSession> {
    let tx = self.cmd_tx(device_id)?;
    let (reply_tx, reply_rx) = mpsc::sync_channel(1);
    tx.send(CastWorkerCmd::Load { request, reply: reply_tx })
      .map_err(|_send| Error::Cast(format!("warm Cast worker for {device_id} disconnected")))?;
    match reply_rx.recv_timeout(COMMAND_TIMEOUT) {
      Ok(result) => result,
      Err(RecvTimeoutError::Timeout) => Err(Error::Cast(format!(
        "warm Cast load timed out after {}s for {device_id}",
        COMMAND_TIMEOUT.as_secs()
      ))),
      Err(RecvTimeoutError::Disconnected) => Err(Error::Cast(format!("warm Cast load reply dropped for {device_id}"))),
    }
  }

  /// Set volume on the warm connection (0.0..=1.0).
  pub fn set_volume(&self, device_id: &str, level: f32) -> Result<()> {
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
  pub fn get_volume(&self, device_id: &str) -> Result<f32> {
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

  /// Pause active media. Errors with "no active session" when none is loaded.
  pub fn pause(&self, device_id: &str) -> Result<()> {
    let tx = self.cmd_tx(device_id)?;
    let (reply_tx, reply_rx) = mpsc::sync_channel(1);
    tx.send(CastWorkerCmd::Pause { reply: reply_tx })
      .map_err(|_send| Error::Cast(format!("warm Cast worker for {device_id} disconnected")))?;
    match reply_rx.recv_timeout(COMMAND_TIMEOUT) {
      Ok(result) => result,
      Err(RecvTimeoutError::Timeout) => Err(Error::Cast(format!("warm Cast pause timed out for {device_id}"))),
      Err(RecvTimeoutError::Disconnected) => Err(Error::Cast(format!("warm Cast pause reply dropped for {device_id}"))),
    }
  }

  /// Resume a paused media session. Errors with "no active session" when none is loaded.
  pub fn play(&self, device_id: &str) -> Result<()> {
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

  /// Best-effort media STOP with a wall-clock timeout; never tears down warm TCP.
  pub fn stop_best_effort(&self, device_id: &str, timeout: Duration) {
    let Ok(tx) = self.cmd_tx(device_id) else {
      return;
    };
    let (reply_tx, reply_rx) = mpsc::sync_channel(1);
    if tx.send(CastWorkerCmd::StopBestEffort { reply: reply_tx }).is_err() {
      return;
    }
    match reply_rx.recv_timeout(timeout) {
      Ok(()) => {},
      Err(RecvTimeoutError::Timeout) => {
        tracing::warn!(
          %device_id,
          timeout_ms = timeout.as_millis(),
          "warm Cast STOP best-effort timed out"
        );
      },
      Err(RecvTimeoutError::Disconnected) => {
        tracing::debug!(%device_id, "warm Cast STOP best-effort: worker gone");
      },
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

/// Mutable state owned exclusively by the worker thread.
struct WorkerState {
  device_id: String,
  host: String,
  hostname: String,
  port: u16,
  /// Live Cast device; never leaves this thread (`Rc` inside `rust_cast`).
  device: Option<rust_cast::CastDevice<'static>>,
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
  /// Whether the last known control-plane state was reachable.
  reachable: bool,
  /// Earliest time to try another reconnect.
  next_reconnect_at: Instant,
  /// Last parse-error string warned (once per distinct message).
  last_parse_warn: Option<String>,
  /// Source IP logged once per successful connect for this worker.
  logged_source_ip: bool,
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
  ownership_lost: Option<UnboundedSender<String>>,
  media_recovered: Option<UnboundedSender<String>>,
  relay_slot: SharedRelaySlot,
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
    ownership_lost,
    media_recovered,
    relay_slot,
    last_load: None,
    last_load_at: None,
    reload_attempted: false,
    next_ping_at: now,
    reconnect_attempt: 0,
    unreachable_since: None,
    reachable: false,
    next_reconnect_at: now,
    last_parse_warn: None,
    logged_source_ip: false,
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
          state.on_idle_tick();
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
      CastWorkerCmd::Load { request, reply } => {
        let result = state.with_hard_deadline(|s| s.handle_load(&request));
        drop(reply.send(result));
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
      CastWorkerCmd::Pause { reply } => {
        let result = state.with_hard_deadline(WorkerState::handle_pause);
        drop(reply.send(result));
      },
      CastWorkerCmd::Play { reply } => {
        let result = state.with_hard_deadline(WorkerState::handle_play);
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
        let _send = reply.send(());
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

    self.check_ownership();
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
  }

  fn set_active_session(&mut self, session: ActiveCastSession) {
    self.active = Some(session);
    self.active_since = Some(Instant::now());
    self.ownership_loss_streak = 0;
    self.buffering_streak = 0;
    self.reload_attempted = false;
  }

  /// Run `op` under a hard deadline: if still running after [`WORKER_OP_DEADLINE`],
  /// shut down the relay so blocking `rust_cast` I/O unblocks.
  fn with_hard_deadline<T>(&mut self, op: impl FnOnce(&mut Self) -> T) -> T {
    let relay_slot = Arc::clone(&self.relay_slot);
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
    result
  }

  /// Establish or re-establish the warm control plane (respects backoff unless `force`).
  fn ensure_connected(&mut self, force: bool) -> Result<()> {
    if self.device.is_some() {
      return Ok(());
    }
    let now = Instant::now();
    if !force && now < self.next_reconnect_at {
      return Err(Error::Cast(format!("reconnect backoff active for {}", self.device_id)));
    }

    self.refresh_host();
    crate::net::wake_cast_host(&self.host);

    match connect_cast_device(&self.host, self.port, &self.relay_slot) {
      Ok(device) => {
        if let Err(err) = device.connection.connect("receiver-0") {
          self.note_unreachable(&Error::Cast(format!("warm connection channel: {err}")));
          return Err(Error::Cast(format!("warm connection channel: {err}")));
        }
        if let Err(err) = device.heartbeat.ping() {
          self.note_unreachable(&Error::Cast(format!("warm heartbeat: {err}")));
          return Err(Error::Cast(format!("warm heartbeat: {err}")));
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

  fn note_reachable(&mut self) {
    let was_unreachable = !self.reachable;
    let attempts = self.reconnect_attempt;
    let downtime = self.unreachable_since.map(|t| t.elapsed());
    self.reachable = true;
    self.reconnect_attempt = 0;
    self.unreachable_since = None;
    self.next_reconnect_at = Instant::now();
    if was_unreachable {
      tracing::info!(
        device_id = %self.device_id,
        host = %self.host,
        attempts,
        downtime_ms = downtime.map(|d| d.as_millis()),
        "Cast control plane reachable again"
      );
    }
  }

  fn note_unreachable(&mut self, err: &Error) {
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
  /// Uses a short client read timeout so an empty buffer does not block for the full
  /// relay I/O timeout (messages already in `rust_cast`'s buffer return immediately).
  fn drain_unsolicited(&mut self) {
    if self.device.is_none() {
      return;
    }
    // Short timeout only for this drain; restore afterward.
    if let Ok(slot) = self.relay_slot.lock()
      && let Some(relay) = slot.as_ref()
    {
      relay.set_client_read_timeout(Some(Duration::from_millis(50)));
    }

    for _ in 0..UNSOLICITED_DRAIN_LIMIT {
      let Some(device) = self.device.as_ref() else {
        break;
      };
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
        Ok(rust_cast::ChannelMessage::Media(media_msg)) => {
          tracing::debug!(device_id = %self.device_id, ?media_msg, "Cast unsolicited media message");
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
              self.warn_parse_once(&err.to_string());
              break;
            },
          }
        },
      }
    }

    if let Ok(slot) = self.relay_slot.lock()
      && let Some(relay) = slot.as_ref()
    {
      relay.restore_client_read_timeout();
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
    let (transport_id, media_session_id, since) = match (self.active.as_ref(), self.active_since) {
      (Some(session), Some(since)) => (session.transport_id.clone(), session.media_session_id, since),
      (None, _) => {
        self.ownership_loss_streak = 0;
        self.buffering_streak = 0;
        return;
      },
      (Some(_), None) => return,
    };
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
        if self.try_internal_reload() {
          self.reload_attempted = true;
          self.ownership_loss_streak = 0;
          self.buffering_streak = 0;
        } else {
          self.reload_attempted = true;
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
    match self.handle_load(&request) {
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

  fn handle_load(&mut self, request: &MediaLoadRequest) -> Result<ActiveCastSession> {
    self.last_load = Some(request.clone());
    self.last_load_at = Some(Instant::now());
    let media = request.to_media();
    match self.load_once(&media) {
      Ok(session) => {
        self.set_active_session(session.clone());
        tracing::info!(
          host = %self.host,
          device_id = %self.device_id,
          transport_id = %session.transport_id,
          media_session_id = session.media_session_id,
          url = %media.content_id,
          "warm Cast load"
        );
        Ok(session)
      },
      Err(err) => {
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
        let session = self.load_once(&media).map_err(|retry_err| {
          tracing::warn!(
            device_id = %self.device_id,
            error = %retry_err,
            "warm Cast load after reconnect failed"
          );
          retry_err
        })?;
        self.set_active_session(session.clone());
        tracing::info!(
          host = %self.host,
          device_id = %self.device_id,
          transport_id = %session.transport_id,
          media_session_id = session.media_session_id,
          url = %media.content_id,
          "warm Cast load"
        );
        Ok(session)
      },
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

    let app = device
      .receiver
      .launch_app(&rust_cast::channels::receiver::CastDeviceApp::DefaultMediaReceiver)
      .map_err(|err| Error::Cast(format!("warm launch app: {err}")))?;

    device
      .connection
      .connect(app.transport_id.as_str())
      .map_err(|err| Error::Cast(format!("warm app connection: {err}")))?;

    let status = device
      .media
      .load(app.transport_id.as_str(), app.session_id.as_str(), media)
      .map_err(|err| Error::Cast(format!("warm media load: {err}")))?;

    let media_session_id = media_session_id_from_status(&status)?;
    Ok(ActiveCastSession::new(app.transport_id, media_session_id))
  }

  fn handle_set_volume(&mut self, level: f32) -> Result<()> {
    let clamped = volume_level_clamped(level);
    if self.device.is_none() {
      self.ensure_connected(true)?;
    }
    let first_err = {
      let device = self
        .device
        .as_ref()
        .ok_or_else(|| Error::Cast("warm Cast device not connected".to_owned()))?;
      match device.receiver.set_volume(clamped) {
        Ok(volume) => {
          tracing::debug!(device_id = %self.device_id, ?volume, "warm Cast volume set");
          return Ok(());
        },
        Err(err) => err,
      }
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
    Ok(())
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
        Ok(status) => return Ok(volume_from_cast_status(status.volume)),
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
    Ok(volume_from_cast_status(status.volume))
  }

  fn handle_stop(&mut self) -> Result<()> {
    let Some(session) = self.active.take() else {
      return Ok(());
    };
    self.active_since = None;
    self.ownership_loss_streak = 0;
    self.buffering_streak = 0;
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

  fn handle_pause(&mut self) -> Result<()> {
    let Some((transport_id, media_session_id)) =
      self.active.as_ref().map(|s| (s.transport_id.clone(), s.media_session_id))
    else {
      return Err(Error::Cast("no active session".to_owned()));
    };
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
    drop(
      device
        .media
        .pause(transport_id.as_str(), media_session_id)
        .map_err(|err| Error::Cast(format!("warm pause: {err}")))?,
    );
    tracing::debug!(
      device_id = %self.device_id,
      media_session_id,
      "warm Cast PAUSE ok"
    );
    Ok(())
  }

  fn handle_play(&mut self) -> Result<()> {
    let Some((transport_id, media_session_id)) =
      self.active.as_ref().map(|s| (s.transport_id.clone(), s.media_session_id))
    else {
      return Err(Error::Cast("no active session".to_owned()));
    };
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

struct MediaProbe {
  session_present: Option<bool>,
  idle_reason: Option<IdleReasonKind>,
  buffering: bool,
  failure: Option<ProbeFailureKind>,
}

/// Connect via source-bound TCP + localhost relay; install shutdown into `relay_slot`.
fn connect_cast_device(host: &str, port: u16, relay_slot: &SharedRelaySlot) -> Result<rust_cast::CastDevice<'static>> {
  let (relay_host, relay_port, shutdown) = crate::net::spawn_cast_connect_relay(host, port)
    .map_err(|err| Error::Cast(format!("connect {host}:{port}: {err}")))?;
  if let Ok(mut slot) = relay_slot.lock() {
    *slot = Some(shutdown);
  }
  rust_cast::CastDevice::connect_without_host_verification("127.0.0.1", relay_port).map_err(|err| {
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

fn short_id(device_id: &str) -> &str {
  let end = device_id.len().min(12);
  device_id.get(..end).unwrap_or(device_id)
}

/// Join a worker thread without blocking forever if it is stuck in I/O.
///
/// Callers should already have forced relay shutdown so the worker can exit.
fn join_with_timeout(thread: JoinHandle<()>, timeout: Duration, device_id: &str) {
  let (done_tx, done_rx) = mpsc::sync_channel(1);
  let waiter = std::thread::spawn(move || {
    drop(thread.join());
    let _send = done_tx.send(());
  });
  if done_rx.recv_timeout(timeout).is_ok() {
    drop(waiter.join());
  } else {
    tracing::warn!(
      %device_id,
      timeout_ms = timeout.as_millis(),
      "warm Cast worker join timed out; detaching"
    );
    drop(waiter);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::cast::CastStreamKind;
  use std::time::Instant;

  fn sample_device(id: &str) -> Device {
    Device {
      id: id.to_owned(),
      name: "Test Nest".to_owned(),
      host: "127.0.0.1".to_owned(),
      hostname: "test.local".to_owned(),
      // Discard port — connect fails fast; worker still starts and accepts Shutdown.
      port: 9,
      last_seen: Instant::now(),
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
    assert_eq!(ownership_action(i), OwnershipAction::LostSignal);
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

  #[test]
  fn worker_shutdown_joins_without_panic() {
    let pool = CastPool::new(None);
    let device = sample_device("nest-shutdown-test");
    pool.ensure(&device);
    assert!(pool.device_ids().contains(&device.id));
    pool.remove(&device.id);
    assert!(!pool.device_ids().contains(&device.id));
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
    pool.stop_best_effort("no-such", Duration::from_millis(100));
  }

  #[test]
  fn ensure_idempotent_same_endpoint() {
    let pool = CastPool::new(None);
    let device = sample_device("nest-idempotent");
    pool.ensure(&device);
    pool.ensure(&device);
    assert_eq!(pool.device_ids().len(), 1);
    pool.shutdown();
    assert!(pool.device_ids().is_empty());
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
}
