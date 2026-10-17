//! Per-connection state and RTSP request handling.

use super::handlers;
use super::remote::{self, MediaRemoteCommand, SenderVersion};
use super::rtsp;
use super::types::*;
use crate::error::RemoteError;
use crate::net::server::{ConnectionHandler, HttpdCallbacks};
use crate::proto::http::{HttpRequest, HttpResponse};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::PoisonError;
use std::sync::atomic::{AtomicU64, Ordering};

/// Minimum AirPlay volume in dB (`GET_PARAMETER` / `SET_PARAMETER`). Mute floor.
pub(crate) const AIRPLAY_VOLUME_DB_MIN: f32 = -144.0;

/// Volume changes smaller than this (dB) are not reported back to the sender.
const VOLUME_REPORT_EPSILON_DB: f32 = 0.05;

/// Shared state passed to each connection.
pub(crate) struct RaopShared {
    pub(crate) hwaddr: Vec<u8>,
    pub(crate) password: String,
    pub(crate) handler: Arc<dyn AudioHandler>,
    pub(crate) pairing_store: Arc<dyn PairingStore>,
    /// Accessory's long-term Ed25519 identity seed (random, persisted via the store).
    pub(crate) identity_seed: [u8; 32],
    pub(crate) output_sample_rate: Option<u32>,
    /// Channel cap for the mixdown path (`None` = pass through).
    pub(crate) output_max_channels: Option<u8>,
    /// Samples advertised in the RTSP `Audio-Latency` header on RECORD.
    pub(crate) audio_latency_samples: u32,
    pub(crate) pin: Option<String>,
    #[cfg(feature = "video")]
    pub(crate) video_handler: Option<Arc<dyn crate::raop::video::VideoHandler>>,
    /// Shared video encryption keys — set by audio SETUP, read by video SETUP.
    #[cfg(feature = "video")]
    pub(crate) video_ekey: Arc<std::sync::RwLock<Option<[u8; 16]>>>,
    #[cfg(feature = "video")]
    pub(crate) video_eiv: Arc<std::sync::RwLock<Option<[u8; 16]>>>,
    pub(crate) pairing_id: String,
    /// Accessory device id (`hwaddr_airplay(hwaddr)`), computed once at build.
    pub(crate) device_id: String,
    pub(crate) airplay_name: String,
    /// Stop-handle for the currently-active audio session, owned by a connection
    /// id. iOS opens parallel connections (Happy Eyeballs) and switches between
    /// them; registering each new session here — and stopping the previous —
    /// keeps only the newest playout feeding the output.
    ///
    /// TEARDOWN of a non-owner connection must not clear this slot (see
    /// [`Self::stop_connection_sessions`]).
    pub(crate) active_audio: std::sync::Mutex<Option<(u64, Box<dyn FnOnce() + Send>)>>,
    /// Detached AP2 tasks (event channel, buffered audio accept, RC data, …).
    /// Dual-tracked with per-connection lists; aborted globally on
    /// [`Self::hard_stop_sessions`] (server stop / Cast-steal kick).
    pub(crate) session_tasks: std::sync::Mutex<Vec<tokio::task::AbortHandle>>,
    /// Monotonic connection ids assigned at [`HttpdCallbacks::conn_init`].
    pub(crate) next_connection_id: AtomicU64,
    #[cfg(feature = "hls")]
    pub(crate) hls_handler: Option<Arc<dyn crate::raop::hls::HlsHandler>>,
    /// Volume reported by `GET_PARAMETER volume` (AirPlay dB: `0.0` = max, `-144.0` = mute).
    ///
    /// Hosts may set this from the physical sink so the iOS slider matches device volume
    /// instead of always advertising max (`0.0`).
    pub(crate) reported_volume_db: std::sync::Mutex<f32>,
    /// Open event channels by owning connection id, oldest first. Receiver-initiated
    /// commands go to the active-audio owner's channel, else the newest.
    pub(crate) event_senders: std::sync::Mutex<Vec<(u64, crate::raop::event_channel::EventSender)>>,
    /// PTP follower acquired by [`crate::RaopServer::start`] (shared per process for fixed ports).
    pub(crate) ptp: std::sync::Mutex<Option<Arc<crate::net::ptp::PtpService>>>,
}

impl RaopShared {
    /// Register a newly-started audio session owned by `owner_id`, stopping the
    /// previous one so only the latest connection's playout feeds the output.
    pub(crate) fn set_active_audio(&self, owner_id: u64, stop: Box<dyn FnOnce() + Send>) {
        let prev = self
            .active_audio
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace((owner_id, stop));
        if let Some((_prev_owner, prev)) = prev {
            prev();
        }
    }

    /// Clock of the running PTP follower, if the server started one.
    pub(crate) fn ptp_clock(&self) -> Option<crate::net::ptp::PtpClock> {
        self.ptp
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|service| service.clock().clone())
    }

    /// Track a detached task on the **global** list for [`Self::hard_stop_sessions`].
    /// Prefer [`handlers::RaopConnection::register_session_task`], which dual-tracks
    /// onto the connection-local list for scoped TEARDOWN.
    pub(crate) fn register_session_task_global(&self, handle: tokio::task::AbortHandle) {
        self.session_tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(handle);
    }

    /// Stop only the sessions/tasks owned by `owner_id`.
    ///
    /// Order is load-bearing: the active-audio stop handle must run **before**
    /// aborting tasks. Buffered playout registers a synchronous stop that sets
    /// `stopped` and wakes the delivery thread and reader before the reader task is aborted.
    ///
    /// Detach-only: no joins or sleeps. Used by TEARDOWN so a stale RC connection
    /// cannot murder a live audio session on another connection.
    pub(crate) fn stop_connection_sessions(&self, owner_id: u64, owned_tasks: &mut Vec<tokio::task::AbortHandle>) {
        let stop = {
            let mut guard = self.active_audio.lock().unwrap_or_else(PoisonError::into_inner);
            match guard.as_ref() {
                Some((id, _)) if *id == owner_id => guard.take().map(|(_id, stop)| stop),
                _ => None,
            }
        };
        if let Some(stop) = stop {
            stop();
        }
        for handle in owned_tasks.drain(..) {
            handle.abort();
        }
        self.unregister_event_sender(owner_id);
    }

    /// Track the event channel opened by connection `owner_id` (replaces an earlier one).
    pub(crate) fn register_event_sender(&self, owner_id: u64, sender: crate::raop::event_channel::EventSender) {
        let mut senders = self.event_senders.lock().unwrap_or_else(PoisonError::into_inner);
        senders.retain(|(id, _)| *id != owner_id);
        senders.push((owner_id, sender));
    }

    /// Forget the event channel of connection `owner_id` (TEARDOWN or disconnect).
    pub(crate) fn unregister_event_sender(&self, owner_id: u64) {
        self.event_senders
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|(id, _)| *id != owner_id);
    }

    /// Event channel for receiver-initiated commands: the one opened by the connection that
    /// owns active audio, else the newest (the remote-control-only connection).
    fn command_event_sender(&self) -> Option<crate::raop::event_channel::EventSender> {
        let audio_owner = self
            .active_audio
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|(id, _)| *id);
        let senders = self.event_senders.lock().unwrap_or_else(PoisonError::into_inner);
        audio_owner
            .and_then(|owner| senders.iter().find(|(id, _)| *id == owner))
            .or_else(|| senders.last())
            .map(|(_, sender)| sender.clone())
    }

    /// Ask the sender to perform `command` (`sendMediaRemoteCommand`).
    pub(crate) fn send_media_remote_command(&self, command: MediaRemoteCommand) -> Result<(), RemoteError> {
        let sender = self.command_event_sender().ok_or(RemoteError::NoEventChannel)?;
        sender.send_command(&remote::media_remote_command_body(command))?;
        tracing::info!(code = command.code(), "Sent media remote command to sender");
        Ok(())
    }

    /// Record a local volume change and report it to the sender with `dvlc`.
    ///
    /// Updates the `GET_PARAMETER volume` value. Sends nothing when the value equals the
    /// last reported one (so a sender's own `SET_PARAMETER volume` is never echoed back).
    pub(crate) fn report_volume(&self, slider: f32, muted: bool) -> Result<(), RemoteError> {
        let volume_db = if muted {
            AIRPLAY_VOLUME_DB_MIN
        } else {
            remote::slider_to_db(slider)
        };
        {
            let mut reported = self.reported_volume_db.lock().unwrap_or_else(PoisonError::into_inner);
            if (*reported - volume_db).abs() < VOLUME_REPORT_EPSILON_DB {
                return Ok(());
            }
            *reported = volume_db;
        }
        let sender = self.command_event_sender().ok_or(RemoteError::NoEventChannel)?;
        if let Some(version) = sender.sender_version()
            && version < remote::DVLC_MIN_SENDER_VERSION
        {
            return Err(RemoteError::SenderTooOld(version.to_string()));
        }
        sender.send_command(&remote::volume_report_body(slider, muted))?;
        tracing::debug!(slider, muted, volume_db, "Reported volume to sender (dvlc)");
        Ok(())
    }

    /// Stop active audio and abort **all** registered AP2 session tasks.
    ///
    /// Reserved for [`crate::raop::server::RaopServer::stop`] / Cast-steal kick.
    /// TEARDOWN must use [`Self::stop_connection_sessions`] instead.
    pub(crate) fn hard_stop_sessions(&self) {
        let stop = self.active_audio.lock().unwrap_or_else(PoisonError::into_inner).take();
        if let Some((_owner, stop)) = stop {
            stop();
        }
        let mut tasks = self.session_tasks.lock().unwrap_or_else(PoisonError::into_inner);
        for handle in tasks.drain(..) {
            handle.abort();
        }
        drop(tasks);
        self.event_senders
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

impl HttpdCallbacks for RaopShared {
    fn conn_init(self: Arc<Self>, local: SocketAddr, remote: SocketAddr) -> Option<Box<dyn ConnectionHandler>> {
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        let conn = handlers::RaopConnection::new(self, connection_id, local, remote);
        let remote_str = remote.ip().to_string();
        conn.shared.handler.on_client_connected(&remote_str);
        Some(Box::new(RaopConnectionHandler {
            conn,
            remote_addr: remote_str,
            connected_at: std::time::Instant::now(),
            cipher: None,
            pending_secret: None,
        }))
    }
}

struct RaopConnectionHandler {
    conn: handlers::RaopConnection,
    remote_addr: String,
    /// Connection-start instant, used to log per-request elapsed time for
    /// connect-latency diagnostics (AP2 PTP-sync wait).
    connected_at: std::time::Instant,
    cipher: Option<crate::crypto::chacha_transport::EncryptedChannel>,
    pending_secret: Option<Vec<u8>>,
}

impl Drop for RaopConnectionHandler {
    fn drop(&mut self) {
        self.conn.shared.unregister_event_sender(self.conn.connection_id);
        self.conn.shared.handler.on_client_disconnected(&self.remote_addr);
    }
}

impl ConnectionHandler for RaopConnectionHandler {
    fn conn_request(&mut self, request: &HttpRequest) -> HttpResponse {
        // Connect-latency timeline: one line per RTSP request, elapsed since the
        // connection opened. `/feedback` is a ~2s keep-alive heartbeat, so it drops
        // to `debug` to keep the connect sequence readable; everything else at info.
        let elapsed_ms = self.connected_at.elapsed().as_millis() as u64;
        let method = request.method().unwrap_or("");
        let url = request.url().unwrap_or("");
        if url == "/feedback" {
            tracing::debug!(elapsed_ms, method, url, "RTSP request");
        } else {
            tracing::info!(elapsed_ms, method, url, "RTSP request");
        }
        if self.conn.sender_version.is_none()
            && let Some(version) = request.header("User-Agent").and_then(SenderVersion::from_user_agent)
        {
            tracing::debug!(%version, "AirPlay sender version");
            self.conn.sender_version = Some(version);
        }
        let resp = rtsp::dispatch(&mut self.conn, request);

        // Queue encryption activation for AFTER this response is sent
        if self.cipher.is_none()
            && let Some(secret) = &self.conn.ap2_shared_secret
        {
            self.pending_secret = Some(secret.clone());
        }

        resp
    }

    fn is_encrypted(&self) -> bool {
        self.cipher.is_some()
    }

    fn after_response(&mut self) {
        if self.cipher.is_none()
            && let Some(secret) = self.pending_secret.take()
        {
            tracing::debug!(secret_len = secret.len(), "Activating cipher from pending_secret");
            match crate::crypto::chacha_transport::EncryptedChannel::control(&secret) {
                Ok(ch) => {
                    tracing::info!("Encrypted RTSP transport activated");
                    self.cipher = Some(ch);
                }
                Err(e) => tracing::warn!("Failed to create cipher: {e}"),
            }
        }
    }

    fn decrypt_incoming(&mut self, data: &[u8]) -> Option<(Vec<u8>, usize)> {
        if let Some(ch) = &mut self.cipher {
            return ch.decrypt_ctx.decrypt(data).ok();
        }
        Some((data.to_vec(), data.len()))
    }

    fn encrypt_outgoing(&mut self, data: &[u8]) -> Vec<u8> {
        if let Some(ch) = &mut self.cipher {
            // Once the channel is encrypted the peer expects ciphertext; never
            // fall back to emitting plaintext (which would leak the response and
            // desync the stream). On the practically-impossible AEAD encrypt
            // failure, return no bytes so the connection tears down instead.
            return ch.encrypt_ctx.encrypt(data).unwrap_or_else(|e| {
                tracing::warn!("Outgoing encryption failed; dropping response: {e}");
                Vec::new()
            });
        }
        data.to_vec()
    }
}

/// Minimal [`RaopShared`] for unit tests (in-memory pairing store, no PIN).
#[cfg(test)]
pub(crate) fn test_shared(handler: Arc<dyn AudioHandler>) -> Arc<RaopShared> {
    Arc::new(RaopShared {
        hwaddr: vec![0u8; 6],
        password: String::new(),
        handler,
        pairing_store: Arc::new(crate::raop::types::MemoryPairingStore::default()),
        identity_seed: [0u8; 32],
        output_sample_rate: None,
        output_max_channels: None,
        audio_latency_samples: crate::raop::server::DEFAULT_AUDIO_LATENCY_SAMPLES,
        pin: None,
        #[cfg(feature = "video")]
        video_handler: None,
        #[cfg(feature = "video")]
        video_ekey: Arc::new(std::sync::RwLock::new(None)),
        #[cfg(feature = "video")]
        video_eiv: Arc::new(std::sync::RwLock::new(None)),
        pairing_id: String::new(),
        device_id: String::new(),
        airplay_name: String::new(),
        active_audio: std::sync::Mutex::new(None),
        session_tasks: std::sync::Mutex::new(Vec::new()),
        next_connection_id: AtomicU64::new(1),
        #[cfg(feature = "hls")]
        hls_handler: None,
        reported_volume_db: std::sync::Mutex::new(0.0),
        event_senders: std::sync::Mutex::new(Vec::new()),
        ptp: std::sync::Mutex::new(None),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raop::{AudioFormat, AudioHandler, AudioSession};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
    use std::time::{Duration, Instant};

    struct NoopHandler;

    impl AudioHandler for NoopHandler {
        fn audio_init(&self, _format: AudioFormat) -> Box<dyn AudioSession> {
            struct S;
            impl AudioSession for S {
                fn audio_process(&mut self, _samples: &[f32]) {}
            }
            Box::new(S)
        }
    }

    fn test_shared() -> Arc<RaopShared> {
        super::test_shared(Arc::new(NoopHandler))
    }

    /// TEARDOWN of connection A must not stop audio owned by connection B.
    #[test]
    fn teardown_scoped_stop_preserves_other_connection_audio() {
        let shared = test_shared();
        let owner_a = 1u64;
        let owner_b = 2u64;

        let a_stopped = Arc::new(AtomicBool::new(false));
        let b_stopped = Arc::new(AtomicBool::new(false));
        let b_deliveries = Arc::new(AtomicUsize::new(0));

        // Conn B owns active audio and keeps "delivering" until its stop runs.
        let b_flag = Arc::clone(&b_stopped);
        shared.set_active_audio(
            owner_b,
            Box::new(move || {
                b_flag.store(true, AtomicOrdering::SeqCst);
            }),
        );

        let stop_b = Arc::clone(&b_stopped);
        let ticks = Arc::clone(&b_deliveries);
        let delivery = std::thread::spawn(move || {
            while !stop_b.load(AtomicOrdering::SeqCst) {
                ticks.fetch_add(1, AtomicOrdering::SeqCst);
                std::thread::sleep(Duration::from_millis(5));
            }
        });

        // Conn A: only local tasks (RC/event style), no active_audio ownership.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let mut tasks_a: Vec<tokio::task::AbortHandle> = rt.block_on(async {
            let a_task = tokio::spawn(async {
                loop {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            });
            let a_abort = a_task.abort_handle();
            shared.register_session_task_global(a_abort.clone());
            vec![a_abort]
        });

        // Conn B dual-tracks a session task (not aborted by A).
        let mut tasks_b: Vec<tokio::task::AbortHandle> = rt.block_on(async {
            let b_task = tokio::spawn(async {
                loop {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            });
            let b_abort = b_task.abort_handle();
            shared.register_session_task_global(b_abort.clone());
            vec![b_abort]
        });

        std::thread::sleep(Duration::from_millis(25));
        let before = b_deliveries.load(AtomicOrdering::SeqCst);
        assert!(before > 0, "B must be delivering before A teardown");

        // TEARDOWN-scoped stop on A — must not touch B's audio.
        let start = Instant::now();
        shared.stop_connection_sessions(owner_a, &mut tasks_a);
        let elapsed = start.elapsed();

        assert!(
            !b_stopped.load(AtomicOrdering::SeqCst),
            "conn B audio stop must not run when A tears down"
        );
        assert!(
            shared
                .active_audio
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_ref()
                .is_some_and(|(id, _)| *id == owner_b),
            "active_audio must still be owned by B"
        );
        assert!(tasks_a.is_empty(), "A local task list must be drained");
        assert!(
            elapsed < Duration::from_millis(200),
            "scoped TEARDOWN stop must be sync/detach-only, took {elapsed:?}"
        );

        // B keeps delivering after A's teardown.
        std::thread::sleep(Duration::from_millis(25));
        let after = b_deliveries.load(AtomicOrdering::SeqCst);
        assert!(
            after > before,
            "B must keep delivering after A teardown (before={before}, after={after})"
        );

        // Cleanup B via its own scoped stop.
        shared.stop_connection_sessions(owner_b, &mut tasks_b);
        assert!(b_stopped.load(AtomicOrdering::SeqCst));
        delivery.join().expect("delivery thread");
        let _ = a_stopped;
    }

    /// Scoped stop with live playout-style stop callback returns promptly (no join).
    #[test]
    fn teardown_scoped_stop_is_prompt_with_live_playout() {
        let shared = test_shared();
        let owner = 7u64;
        let stopped = Arc::new(AtomicBool::new(false));
        let deliveries = Arc::new(AtomicUsize::new(0));

        // Background "playout" that keeps ticking until sync stop.
        let stop_flag = Arc::clone(&stopped);
        let tick = Arc::clone(&deliveries);
        let playout = std::thread::spawn(move || {
            while !stop_flag.load(AtomicOrdering::SeqCst) {
                tick.fetch_add(1, AtomicOrdering::SeqCst);
                std::thread::sleep(Duration::from_millis(5));
            }
        });

        let flag = Arc::clone(&stopped);
        shared.set_active_audio(
            owner,
            Box::new(move || {
                flag.store(true, AtomicOrdering::SeqCst);
            }),
        );

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let _guard = rt.enter();
        let task = tokio::spawn(async {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
        let abort = task.abort_handle();
        shared.register_session_task_global(abort.clone());
        let mut owned = vec![abort];

        // Let playout deliver a few frames first.
        std::thread::sleep(Duration::from_millis(30));
        assert!(deliveries.load(AtomicOrdering::SeqCst) > 0);

        let start = Instant::now();
        shared.stop_connection_sessions(owner, &mut owned);
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_millis(100),
            "TEARDOWN-scoped stop must return promptly (no multi-second hang), took {elapsed:?}"
        );
        assert!(stopped.load(AtomicOrdering::SeqCst));

        playout.join().expect("playout thread");
        assert!(
            deliveries.load(AtomicOrdering::SeqCst) > 0,
            "playout must have delivered before stop"
        );
    }

    /// hard_stop still kills every owner (server stop / Cast-steal).
    #[test]
    fn hard_stop_kills_all_owners() {
        let shared = test_shared();
        let a = Arc::new(AtomicBool::new(false));
        let b = Arc::new(AtomicBool::new(false));
        // Only the latest active_audio slot is kept; hard_stop must clear it.
        let a_flag = Arc::clone(&a);
        let b_flag = Arc::clone(&b);
        shared.set_active_audio(1, Box::new(move || a_flag.store(true, AtomicOrdering::SeqCst)));
        shared.set_active_audio(2, Box::new(move || b_flag.store(true, AtomicOrdering::SeqCst)));
        // First owner was replaced and already stopped by set_active_audio.
        assert!(a.load(AtomicOrdering::SeqCst));
        assert!(!b.load(AtomicOrdering::SeqCst));

        shared.hard_stop_sessions();
        assert!(b.load(AtomicOrdering::SeqCst));
        assert!(
            shared
                .active_audio
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_none()
        );
    }

    fn decode_command(message: &[u8]) -> (String, plist::Dictionary) {
        let split = message.windows(4).position(|w| w == b"\r\n\r\n").expect("header end") + 4;
        let head = String::from_utf8(message[..split].to_vec()).expect("ascii head");
        let body: plist::Value = plist::from_bytes(&message[split..]).expect("plist body");
        (head, body.into_dictionary().expect("dict"))
    }

    fn register(
        shared: &RaopShared,
        owner: u64,
        version: Option<SenderVersion>,
    ) -> tokio::sync::mpsc::UnboundedReceiver<Vec<u8>> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        shared.register_event_sender(owner, crate::raop::event_channel::EventSender::new(tx, version));
        rx
    }

    #[test]
    fn media_remote_command_goes_to_active_audio_owner_with_rising_cseq() {
        let shared = test_shared();
        let mut audio_rx = register(&shared, 1, None);
        let mut rc_rx = register(&shared, 2, None);
        shared.set_active_audio(1, Box::new(|| {}));

        shared.send_media_remote_command(MediaRemoteCommand::Pause).unwrap();
        shared.send_media_remote_command(MediaRemoteCommand::Play).unwrap();

        let (head, body) = decode_command(&audio_rx.try_recv().unwrap());
        assert!(head.starts_with("POST /command RTSP/1.0\r\n"), "{head}");
        assert!(head.contains("\r\nCSeq: 1\r\n"), "{head}");
        assert_eq!(body.get("value").and_then(plist::Value::as_string), Some("paus"));
        let (head, body) = decode_command(&audio_rx.try_recv().unwrap());
        assert!(head.contains("\r\nCSeq: 2\r\n"), "{head}");
        assert_eq!(body.get("value").and_then(plist::Value::as_string), Some("play"));
        assert!(
            rc_rx.try_recv().is_err(),
            "RC channel must not get audio-owner commands"
        );
    }

    #[test]
    fn media_remote_command_without_audio_uses_newest_and_errors_when_none() {
        let shared = test_shared();
        assert!(matches!(
            shared.send_media_remote_command(MediaRemoteCommand::Pause),
            Err(RemoteError::NoEventChannel)
        ));
        let mut old_rx = register(&shared, 1, None);
        let mut new_rx = register(&shared, 2, None);
        shared.send_media_remote_command(MediaRemoteCommand::Pause).unwrap();
        assert!(new_rx.try_recv().is_ok());
        assert!(old_rx.try_recv().is_err());

        // Teardown of connection 2 unregisters its channel.
        shared.stop_connection_sessions(2, &mut Vec::new());
        shared.send_media_remote_command(MediaRemoteCommand::Pause).unwrap();
        assert!(old_rx.try_recv().is_ok());
    }

    #[test]
    fn report_volume_sends_dvlc_once_and_never_echoes_sender_volume() {
        let shared = test_shared();
        let mut rx = register(&shared, 1, Some(SenderVersion([860, 7, 1])));

        shared.report_volume(0.5, false).unwrap();
        let (_, body) = decode_command(&rx.try_recv().unwrap());
        assert_eq!(body.get("value").and_then(plist::Value::as_string), Some("dvlc"));
        assert_eq!(body.get("volume").and_then(plist::Value::as_real), Some(0.5));
        assert_eq!(body.get("isMuted").and_then(plist::Value::as_boolean), Some(false));
        assert!((*shared.reported_volume_db.lock().unwrap() - -15.0).abs() < 0.001);

        // Same value again: no report.
        shared.report_volume(0.5, false).unwrap();
        assert!(rx.try_recv().is_err());

        // Sender set -6 dB (SET_PARAMETER); the device then settles on the same level.
        *shared.reported_volume_db.lock().unwrap() = -6.0;
        shared.report_volume(0.8, false).unwrap();
        assert!(rx.try_recv().is_err(), "echo of the sender's own volume");

        // Mute reports -144 with isMuted.
        shared.report_volume(0.8, true).unwrap();
        let (_, body) = decode_command(&rx.try_recv().unwrap());
        assert_eq!(body.get("isMuted").and_then(plist::Value::as_boolean), Some(true));
        assert!((*shared.reported_volume_db.lock().unwrap() - AIRPLAY_VOLUME_DB_MIN).abs() < f32::EPSILON);
    }

    #[test]
    fn report_volume_refuses_pre_dvlc_sender() {
        let shared = test_shared();
        let mut rx = register(&shared, 1, Some(SenderVersion([354, 54, 5])));
        assert!(matches!(
            shared.report_volume(0.3, false),
            Err(RemoteError::SenderTooOld(_))
        ));
        assert!(rx.try_recv().is_err());
        // GET_PARAMETER still reflects the device volume.
        assert!((*shared.reported_volume_db.lock().unwrap() - -21.0).abs() < 0.001);
    }
}
