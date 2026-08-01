//! Warm Cast control-plane pool.
//!
//! Each Chromecast gets one dedicated OS thread owning a live `rust_cast::CastDevice`
//! (not `Send` without the `thread_safe` feature). TCP is established while the device
//! is idle so AirPlay sessions only issue LOAD on the existing session — avoiding the
//! Nest `:8009` black-hole window where **new** connects fail with "No route to host".

use std::collections::HashMap;
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender};
use std::thread::JoinHandle;
use std::time::Duration;

use parking_lot::Mutex;

use crate::error::{Error, Result};
use crate::registry::Device;

use super::{
  volume_level_clamped, ActiveCastSession, MediaLoadRequest, media_session_id_from_status,
};

/// Heartbeat interval on the warm control-plane connection.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(4);

/// Wall-clock wait for a worker reply to `Load` / `SetVolume` / `Stop`.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(20);

/// Best-effort wait when joining a worker after `Shutdown`.
const JOIN_TIMEOUT: Duration = Duration::from_secs(3);

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
  /// Stop active media session (if any); keep TCP warm.
  Stop {
    reply: SyncSender<Result<()>>,
  },
  /// Best-effort stop; always replies after attempt (or immediately if no session).
  StopBestEffort {
    reply: SyncSender<()>,
  },
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
}

/// Shared pool of warm Cast control-plane workers (`Send + Sync` via `Arc`).
///
/// Workers are started when devices appear (see app maintain loop) and torn down
/// only when the device leaves — not when an AirPlay media session ends.
#[derive(Default)]
pub struct CastPool {
  workers: Mutex<HashMap<String, CastWorkerHandle>>,
}

impl std::fmt::Debug for CastPool {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("CastPool")
      .field("workers", &self.workers.lock().len())
      .finish()
  }
}

impl CastPool {
  /// Create an empty pool (no workers yet).
  #[must_use]
  pub fn new() -> Self {
    Self {
      workers: Mutex::new(HashMap::new()),
    }
  }

  /// Ensure a warm worker exists for `device` (start one if missing).
  ///
  /// Updates host / hostname / port when the registry entry changes.
  pub fn ensure(&self, device: &Device) {
    let mut guard = self.workers.lock();
    if let Some(handle) = guard.get_mut(&device.id) {
      let host_changed = handle.host != device.host
        || handle.hostname != device.hostname
        || handle.port != device.port;
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
    let thread_name = format!("cast-warm-{}", short_id(&device_id));
    let join = std::thread::Builder::new()
      .name(thread_name)
      .spawn(move || {
        worker_main(device_id, host, hostname, port, cmd_rx);
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
      },
    ));
    drop(guard);
    tracing::debug!(id = %device.id, host = %device.host, port = device.port, "warm Cast worker started");
  }

  /// Shut down and remove the worker for `device_id` (device left the network).
  pub fn remove(&self, device_id: &str) {
    let removed = {
      let mut guard = self.workers.lock();
      guard.remove(device_id)
    };
    let Some(mut worker) = removed else {
      return;
    };
    drop(worker.cmd_tx.send(CastWorkerCmd::Shutdown));
    if let Some(thread) = worker.thread.take() {
      join_with_timeout(thread, JOIN_TIMEOUT, device_id);
    }
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
  /// **one** reconnect before failing.
  pub fn load(&self, device_id: &str, request: MediaLoadRequest) -> Result<ActiveCastSession> {
    let tx = self.cmd_tx(device_id)?;
    let (reply_tx, reply_rx) = mpsc::sync_channel(1);
    tx.send(CastWorkerCmd::Load {
      request,
      reply: reply_tx,
    })
    .map_err(|_send| Error::Cast(format!("warm Cast worker for {device_id} disconnected")))?;
    match reply_rx.recv_timeout(COMMAND_TIMEOUT) {
      Ok(result) => result,
      Err(RecvTimeoutError::Timeout) => Err(Error::Cast(format!(
        "warm Cast load timed out after {}s for {device_id}",
        COMMAND_TIMEOUT.as_secs()
      ))),
      Err(RecvTimeoutError::Disconnected) => {
        Err(Error::Cast(format!("warm Cast load reply dropped for {device_id}")))
      },
    }
  }

  /// Set volume on the warm connection (0.0..=1.0).
  pub fn set_volume(&self, device_id: &str, level: f32) -> Result<()> {
    let tx = self.cmd_tx(device_id)?;
    let (reply_tx, reply_rx) = mpsc::sync_channel(1);
    tx.send(CastWorkerCmd::SetVolume {
      level,
      reply: reply_tx,
    })
    .map_err(|_send| Error::Cast(format!("warm Cast worker for {device_id} disconnected")))?;
    match reply_rx.recv_timeout(COMMAND_TIMEOUT) {
      Ok(result) => result,
      Err(RecvTimeoutError::Timeout) => Err(Error::Cast(format!(
        "warm Cast set_volume timed out for {device_id}"
      ))),
      Err(RecvTimeoutError::Disconnected) => {
        Err(Error::Cast(format!("warm Cast set_volume reply dropped for {device_id}")))
      },
    }
  }

  /// Stop the active media session on the warm connection (errors if no session).
  pub fn stop(&self, device_id: &str) -> Result<()> {
    let tx = self.cmd_tx(device_id)?;
    let (reply_tx, reply_rx) = mpsc::sync_channel(1);
    tx.send(CastWorkerCmd::Stop { reply: reply_tx })
      .map_err(|_send| Error::Cast(format!("warm Cast worker for {device_id} disconnected")))?;
    match reply_rx.recv_timeout(COMMAND_TIMEOUT) {
      Ok(result) => result,
      Err(RecvTimeoutError::Timeout) => {
        Err(Error::Cast(format!("warm Cast stop timed out for {device_id}")))
      },
      Err(RecvTimeoutError::Disconnected) => {
        Err(Error::Cast(format!("warm Cast stop reply dropped for {device_id}")))
      },
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
}

#[expect(
  clippy::needless_pass_by_value,
  reason = "worker thread owns the command receiver for its full lifetime"
)]
fn worker_main(
  device_id: String,
  host: String,
  hostname: String,
  port: u16,
  cmd_rx: mpsc::Receiver<CastWorkerCmd>,
) {
  let mut state = WorkerState {
    device_id,
    host,
    hostname,
    port,
    device: None,
    active: None,
  };

  // Connect while idle (before any AirPlay session).
  if let Err(err) = state.ensure_connected(/* reconnect_log */ false) {
    tracing::warn!(
      device_id = %state.device_id,
      host = %state.host,
      port = state.port,
      error = %err,
      "warm Cast initial connect failed; will retry on heartbeat"
    );
  }

  loop {
    match cmd_rx.recv_timeout(HEARTBEAT_INTERVAL) {
      Ok(CastWorkerCmd::Shutdown) => {
        tracing::debug!(device_id = %state.device_id, "warm Cast worker shutting down");
        state.drop_device();
        break;
      },
      Ok(CastWorkerCmd::UpdateEndpoint {
        host: new_host,
        hostname: new_hostname,
        port: new_port,
      }) => {
        let changed =
          state.host != new_host || state.hostname != new_hostname || state.port != new_port;
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
          if let Err(err) = state.ensure_connected(true) {
            tracing::warn!(
              device_id = %state.device_id,
              error = %err,
              "warm Cast reconnect after endpoint update failed"
            );
          }
        }
      },
      Ok(CastWorkerCmd::Load { request, reply }) => {
        let result = state.handle_load(&request);
        drop(reply.send(result));
      },
      Ok(CastWorkerCmd::SetVolume { level, reply }) => {
        let result = state.handle_set_volume(level);
        drop(reply.send(result));
      },
      Ok(CastWorkerCmd::Stop { reply }) => {
        let result = state.handle_stop();
        drop(reply.send(result));
      },
      Ok(CastWorkerCmd::StopBestEffort { reply }) => {
        if let Err(err) = state.handle_stop() {
          tracing::debug!(
            device_id = %state.device_id,
            error = %err,
            "warm Cast STOP best-effort failed"
          );
        }
        // `SendError<()>` is `Copy`; ignore without `drop`.
        let _send = reply.send(());
      },
      Err(RecvTimeoutError::Timeout) => {
        state.heartbeat_tick();
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
  }
}

impl WorkerState {
  fn drop_device(&mut self) {
    self.active = None;
    self.device = None;
  }

  /// Establish or re-establish the warm control plane.
  fn ensure_connected(&mut self, is_reconnect: bool) -> Result<()> {
    if self.device.is_some() {
      return Ok(());
    }
    self.refresh_host();
    crate::net::wake_cast_host(&self.host);

    let device = connect_cast_device(&self.host, self.port)?;
    device
      .connection
      .connect("receiver-0")
      .map_err(|err| Error::Cast(format!("warm connection channel: {err}")))?;
    device
      .heartbeat
      .ping()
      .map_err(|err| Error::Cast(format!("warm heartbeat: {err}")))?;

    self.device = Some(device);
    if is_reconnect {
      tracing::info!(
        host = %self.host,
        port = self.port,
        device_id = %self.device_id,
        "warm Cast reconnect ok"
      );
    } else {
      tracing::info!(
        host = %self.host,
        port = self.port,
        device_id = %self.device_id,
        "warm Cast connected"
      );
    }
    Ok(())
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
    }
  }

  fn heartbeat_tick(&mut self) {
    let Some(device) = self.device.as_ref() else {
      if let Err(err) = self.ensure_connected(true) {
        tracing::debug!(
          device_id = %self.device_id,
          host = %self.host,
          error = %err,
          "warm Cast heartbeat reconnect failed"
        );
      }
      return;
    };
    if let Err(ping_err) = device.heartbeat.ping() {
      tracing::warn!(
        device_id = %self.device_id,
        host = %self.host,
        error = %ping_err,
        "warm Cast heartbeat failed; reconnecting"
      );
      self.drop_device();
      if let Err(reconnect_err) = self.ensure_connected(true) {
        tracing::debug!(
          device_id = %self.device_id,
          error = %reconnect_err,
          "warm Cast reconnect after heartbeat failure failed"
        );
      }
    }
  }

  fn handle_load(&mut self, request: &MediaLoadRequest) -> Result<ActiveCastSession> {
    let media = request.to_media();
    match self.load_once(&media) {
      Ok(session) => {
        self.active = Some(session.clone());
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
        self.drop_device();
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
        self.active = Some(session.clone());
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

    // Keep the platform receiver channel alive; re-ping before LOAD.
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
    self.drop_device();
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

  fn handle_stop(&mut self) -> Result<()> {
    let Some(session) = self.active.take() else {
      return Ok(());
    };
    let Some(device) = self.device.as_ref() else {
      // No warm TCP — nothing to STOP on-device; session already cleared.
      return Ok(());
    };

    // CONNECT receiver-0 then app transport (same order as MediaTransportPlan).
    for dest in ["receiver-0", session.transport_id.as_str()] {
      if let Err(err) = device.connection.connect(dest) {
        tracing::debug!(
          device_id = %self.device_id,
          dest,
          error = %err,
          "warm Cast STOP connect failed"
        );
        // Drop dead connection; media is already ending on the bridge side.
        self.drop_device();
        return Err(Error::Cast(format!("warm stop connect {dest}: {err}")));
      }
    }
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
}

/// Connect via source-bound TCP + localhost relay (same path as `CastController::with_device`).
fn connect_cast_device(host: &str, port: u16) -> Result<rust_cast::CastDevice<'static>> {
  let (relay_host, relay_port) = crate::net::spawn_cast_connect_relay(host, port)
    .map_err(|err| Error::Cast(format!("connect {host}:{port}: {err}")))?;
  rust_cast::CastDevice::connect_without_host_verification("127.0.0.1", relay_port).map_err(
    |err| {
      Error::Cast(format!(
        "connect {host}:{port} (via local relay {relay_host}:{relay_port}): {err}"
      ))
    },
  )
}

fn short_id(device_id: &str) -> &str {
  let end = device_id.len().min(12);
  device_id.get(..end).unwrap_or(device_id)
}

/// Join a worker thread without blocking forever if it is stuck in I/O.
fn join_with_timeout(thread: JoinHandle<()>, timeout: Duration, device_id: &str) {
  // `JoinHandle` has no timed join; park a waiter and detach on timeout.
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
    // Detach waiter + original worker.
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

  #[test]
  fn worker_shutdown_joins_without_panic() {
    let pool = CastPool::new();
    let device = sample_device("nest-shutdown-test");
    pool.ensure(&device);
    assert!(pool.device_ids().contains(&device.id));
    pool.remove(&device.id);
    assert!(!pool.device_ids().contains(&device.id));
  }

  #[test]
  fn load_without_worker_errors() {
    let pool = CastPool::new();
    let request = MediaLoadRequest::wav("http://127.0.0.1:9/stream", CastStreamKind::Buffered);
    let err = pool.load("missing-device", request).unwrap_err();
    let msg = err.to_string();
    assert!(
      msg.contains("no warm Cast worker"),
      "expected missing-worker error, got: {msg}"
    );
  }

  #[test]
  fn stop_best_effort_without_worker_is_noop() {
    let pool = CastPool::new();
    pool.stop_best_effort("no-such", Duration::from_millis(100));
  }

  #[test]
  fn ensure_idempotent_same_endpoint() {
    let pool = CastPool::new();
    let device = sample_device("nest-idempotent");
    pool.ensure(&device);
    pool.ensure(&device);
    assert_eq!(pool.device_ids().len(), 1);
    pool.shutdown();
    assert!(pool.device_ids().is_empty());
  }

  #[test]
  fn remove_unknown_is_ok() {
    let pool = CastPool::new();
    pool.remove("never-existed");
  }
}
