//! Per-connection state and RTSP request handling.

use super::MAX_NONCE_LEN;
use super::handlers_ap1 as handlers;
use super::rtsp;
use super::types::*;
use crate::crypto::fairplay::FairPlay;
use crate::crypto::pairing::Pairing;
use crate::crypto::rsa::RsaKey;
use crate::net::server::{ConnectionHandler, HttpdCallbacks};
use crate::proto::digest;
use crate::proto::http::{HttpRequest, HttpResponse};
use std::net::SocketAddr;
use std::sync::Arc;
#[cfg(feature = "ap2")]
use std::sync::PoisonError;
#[cfg(feature = "ap2")]
use std::sync::atomic::{AtomicU64, Ordering};

/// Minimum AirPlay volume in dB (`GET_PARAMETER` / `SET_PARAMETER`). Mute floor.
pub(crate) const AIRPLAY_VOLUME_DB_MIN: f32 = -144.0;

/// Shared state passed to each connection.
pub(crate) struct RaopShared {
    pub(crate) rsakey: Arc<RsaKey>,
    pub(crate) pairing: Arc<Pairing>,
    pub(crate) hwaddr: Vec<u8>,
    pub(crate) password: String,
    pub(crate) handler: Arc<dyn AudioHandler>,
    #[cfg(feature = "ap2")]
    pub(crate) pairing_store: Arc<dyn PairingStore>,
    /// Accessory's long-term Ed25519 identity seed (random, persisted via the store).
    #[cfg(feature = "ap2")]
    pub(crate) identity_seed: [u8; 32],
    pub(crate) output_sample_rate: Option<u32>,
    /// Only consulted by the AP2 mixdown path; dead in AP1-only builds.
    #[cfg_attr(not(feature = "ap2"), allow(dead_code))]
    pub(crate) output_max_channels: Option<u8>,
    /// Samples advertised in the RTSP `Audio-Latency` header on RECORD.
    pub(crate) audio_latency_samples: u32,
    #[cfg(feature = "ap2")]
    pub(crate) pin: Option<String>,
    #[cfg(feature = "video")]
    pub(crate) video_handler: Option<Arc<dyn crate::raop::video::VideoHandler>>,
    /// Shared video encryption keys — set by audio SETUP, read by video SETUP.
    #[cfg(feature = "video")]
    pub(crate) video_ekey: Arc<std::sync::RwLock<Option<[u8; 16]>>>,
    #[cfg(feature = "video")]
    pub(crate) video_eiv: Arc<std::sync::RwLock<Option<[u8; 16]>>>,
    #[cfg(feature = "ap2")]
    pub(crate) pairing_id: String,
    /// Accessory device id (`hwaddr_airplay(hwaddr)`), computed once at build.
    #[cfg(feature = "ap2")]
    pub(crate) device_id: String,
    #[cfg(feature = "ap2")]
    pub(crate) airplay_name: String,
    /// Stop-handle for the currently-active audio session, owned by a connection
    /// id. iOS opens parallel connections (Happy Eyeballs) and switches between
    /// them; registering each new session here — and stopping the previous —
    /// keeps only the newest playout feeding the output.
    ///
    /// TEARDOWN of a non-owner connection must not clear this slot (see
    /// [`Self::stop_connection_sessions`]).
    #[cfg(feature = "ap2")]
    pub(crate) active_audio: std::sync::Mutex<Option<(u64, Box<dyn FnOnce() + Send>)>>,
    /// Detached AP2 tasks (event channel, buffered audio accept, RC data, …).
    /// Dual-tracked with per-connection lists; aborted globally on
    /// [`Self::hard_stop_sessions`] (server stop / Cast-steal kick).
    #[cfg(feature = "ap2")]
    pub(crate) session_tasks: std::sync::Mutex<Vec<tokio::task::AbortHandle>>,
    /// Monotonic connection ids assigned at [`HttpdCallbacks::conn_init`].
    #[cfg(feature = "ap2")]
    pub(crate) next_connection_id: AtomicU64,
    #[cfg(feature = "hls")]
    pub(crate) hls_handler: Option<Arc<dyn crate::raop::hls::HlsHandler>>,
    /// Volume reported by `GET_PARAMETER volume` (AirPlay dB: `0.0` = max, `-144.0` = mute).
    ///
    /// Hosts may set this from the physical sink so the iOS slider matches device volume
    /// instead of always advertising max (`0.0`).
    pub(crate) reported_volume_db: std::sync::Mutex<f32>,
}

#[cfg(feature = "ap2")]
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
    /// `stopped` and notifies the delivery condvar; if the async command task is
    /// aborted first, `PlayoutCommand::Stop` is never polled and delivery wedges.
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
    }
}

impl HttpdCallbacks for RaopShared {
    fn conn_init(self: Arc<Self>, local: SocketAddr, remote: SocketAddr) -> Option<Box<dyn ConnectionHandler>> {
        let local_bytes = match local.ip() {
            std::net::IpAddr::V4(ip) => ip.octets().to_vec(),
            std::net::IpAddr::V6(ip) => ip.octets().to_vec(),
        };
        let remote_bytes = match remote.ip() {
            std::net::IpAddr::V4(ip) => ip.octets().to_vec(),
            std::net::IpAddr::V6(ip) => ip.octets().to_vec(),
        };

        #[cfg(feature = "ap2")]
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);

        let conn = handlers::RaopConnection {
            raop_rtp: None,
            fairplay: FairPlay::new(),
            pairing: self.pairing.create_session(),
            local_addr: local_bytes,
            remote_addr: remote_bytes,
            remote_socket: remote,
            nonce: digest::generate_nonce(MAX_NONCE_LEN),
            #[cfg(feature = "ap2")]
            connection_id,
            #[cfg(feature = "ap2")]
            session_tasks: Vec::new(),
            #[cfg(feature = "ap2")]
            srp_server: None,
            #[cfg(feature = "ap2")]
            pair_verify: None,
            #[cfg(feature = "ap2")]
            ap2_shared_secret: None,
            #[cfg(feature = "ap2")]
            pair_verify_secret: None,
            #[cfg(feature = "ap2")]
            is_ap2: false,
            #[cfg(feature = "ap2")]
            playout_cmd: None,
            #[cfg(feature = "ap2")]
            event_sender: None,
            #[cfg(feature = "video")]
            ekey: None,
            #[cfg(feature = "video")]
            eiv: None,
            #[cfg(feature = "hls")]
            hls_state: crate::raop::hls::HlsState::new(),
            shared: self.clone(),
        };
        let remote_str = remote.ip().to_string();
        conn.shared.handler.on_client_connected(&remote_str);
        Some(Box::new(RaopConnectionHandler {
            conn,
            remote_addr: remote_str,
            connected_at: std::time::Instant::now(),
            #[cfg(feature = "ap2")]
            cipher: None,
            #[cfg(feature = "ap2")]
            pending_secret: None,
        }))
    }
}

struct RaopConnectionHandler {
    conn: handlers::RaopConnection,
    remote_addr: String,
    /// Connection-start instant, used to log per-request elapsed time for
    /// connect-latency diagnostics (AP2 PTP-sync wait vs AP1 fast path).
    connected_at: std::time::Instant,
    #[cfg(feature = "ap2")]
    cipher: Option<crate::crypto::chacha_transport::EncryptedChannel>,
    #[cfg(feature = "ap2")]
    pending_secret: Option<Vec<u8>>,
}

impl Drop for RaopConnectionHandler {
    fn drop(&mut self) {
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
        let resp = rtsp::dispatch(&mut self.conn, request);

        // Queue encryption activation for AFTER this response is sent
        #[cfg(feature = "ap2")]
        if self.cipher.is_none()
            && let Some(secret) = &self.conn.ap2_shared_secret
        {
            self.pending_secret = Some(secret.clone());
        }

        resp
    }

    fn is_encrypted(&self) -> bool {
        #[cfg(feature = "ap2")]
        {
            self.cipher.is_some()
        }
        #[cfg(not(feature = "ap2"))]
        {
            false
        }
    }

    fn after_response(&mut self) {
        #[cfg(feature = "ap2")]
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
        #[cfg(feature = "ap2")]
        if let Some(ch) = &mut self.cipher {
            return ch.decrypt_ctx.decrypt(data).ok();
        }
        Some((data.to_vec(), data.len()))
    }

    fn encrypt_outgoing(&mut self, data: &[u8]) -> Vec<u8> {
        #[cfg(feature = "ap2")]
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

// On drop, RTP session is cleaned up automatically (RaopRtp dropped → shutdown sent)

#[cfg(all(test, feature = "ap2"))]
mod tests {
    use super::*;
    use crate::crypto::pairing::Pairing;
    use crate::crypto::rsa::RsaKey;
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
        Arc::new(RaopShared {
            rsakey: Arc::new(RsaKey::from_pem(include_str!("../../airport.key")).unwrap()),
            pairing: Arc::new(Pairing::generate().unwrap()),
            hwaddr: vec![0u8; 6],
            password: String::new(),
            handler: Arc::new(NoopHandler),
            pairing_store: Arc::new(crate::raop::types::MemoryPairingStore::default()),
            identity_seed: [0u8; 32],
            output_sample_rate: None,
            output_max_channels: None,
            audio_latency_samples: crate::raop::server::DEFAULT_AUDIO_LATENCY_SAMPLES,
            pin: None,
            pairing_id: String::new(),
            device_id: String::new(),
            airplay_name: String::new(),
            active_audio: std::sync::Mutex::new(None),
            session_tasks: std::sync::Mutex::new(Vec::new()),
            next_connection_id: AtomicU64::new(1),
            reported_volume_db: std::sync::Mutex::new(0.0),
        })
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
}
