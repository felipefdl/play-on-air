//! Per-connection RTSP state and the handlers shared by every AirPlay 2 session
//! (FairPlay, OPTIONS, GET/SET_PARAMETER). AP2-specific handlers live in
//! [`super::handlers_ap2`].

use std::net::SocketAddr;
use std::sync::Arc;

use crate::crypto::fairplay::FairPlay;
use crate::crypto::pairing_homekit::{PairVerifyServer, SrpServer};
use crate::error::ShairplayError;
use crate::proto::digest;
use crate::proto::http::{HttpRequest, HttpResponse};
use crate::raop::MAX_NONCE_LEN;
use crate::raop::connection::RaopShared;
#[cfg(feature = "video")]
use crate::raop::rtp::RaopRtp;

/// Per-connection state for RTSP handler dispatch. Equivalent to raop_conn_t.
pub(crate) struct RaopConnection {
    /// Legacy RTP audio session (UxPlay-style screen mirroring only).
    #[cfg(feature = "video")]
    pub(crate) raop_rtp: Option<RaopRtp>,
    pub(crate) fairplay: FairPlay,
    pub(crate) local_addr: Vec<u8>,
    /// Peer address (legacy video RTP timing/data sockets).
    #[cfg(feature = "video")]
    pub(crate) remote_socket: SocketAddr,
    pub(crate) nonce: String,
    /// Cheap shared handle to server-wide config (identity, keys, handler, settings).
    /// Replaces the ~17 fields that were previously deep-copied into every connection.
    pub(crate) shared: Arc<RaopShared>,
    /// Monotonic id for this RTSP connection; owns active_audio / session tasks.
    pub(crate) connection_id: u64,
    /// Connection-local abort handles for TEARDOWN-scoped stop (dual-tracked globally).
    pub(crate) session_tasks: Vec<tokio::task::AbortHandle>,
    pub(crate) srp_server: Option<SrpServer>,
    pub(crate) pair_verify: Option<PairVerifyServer>,
    pub(crate) ap2_shared_secret: Option<Vec<u8>>,
    /// X25519 shared secret from pair-verify (32 bytes). Used for video key derivation.
    pub(crate) pair_verify_secret: Option<[u8; 32]>,
    pub(crate) is_ap2: bool,
    pub(crate) playout_cmd: Option<tokio::sync::mpsc::UnboundedSender<crate::raop::buffered_audio::PlayoutCommand>>,
    pub(crate) event_sender: Option<crate::raop::event_channel::EventSender>,
    #[cfg(feature = "video")]
    pub(crate) ekey: Option<[u8; 16]>,
    #[cfg(feature = "video")]
    pub(crate) eiv: Option<[u8; 16]>,
    #[cfg(feature = "hls")]
    pub(crate) hls_state: std::sync::Arc<std::sync::Mutex<crate::raop::hls::HlsState>>,
}

impl RaopConnection {
    /// Fresh per-connection state for a client that connected from `remote` to `local`.
    pub(crate) fn new(
        shared: Arc<RaopShared>,
        connection_id: u64,
        local: SocketAddr,
        #[cfg_attr(
            not(feature = "video"),
            expect(unused_variables, reason = "only legacy video RTP uses the peer address")
        )]
        remote: SocketAddr,
    ) -> Self {
        let local_addr = match local.ip() {
            std::net::IpAddr::V4(ip) => ip.octets().to_vec(),
            std::net::IpAddr::V6(ip) => ip.octets().to_vec(),
        };
        Self {
            #[cfg(feature = "video")]
            raop_rtp: None,
            fairplay: FairPlay::new(),
            local_addr,
            #[cfg(feature = "video")]
            remote_socket: remote,
            nonce: digest::generate_nonce(MAX_NONCE_LEN),
            shared,
            connection_id,
            session_tasks: Vec::new(),
            srp_server: None,
            pair_verify: None,
            ap2_shared_secret: None,
            pair_verify_secret: None,
            is_ap2: false,
            playout_cmd: None,
            event_sender: None,
            #[cfg(feature = "video")]
            ekey: None,
            #[cfg(feature = "video")]
            eiv: None,
            #[cfg(feature = "hls")]
            hls_state: crate::raop::hls::HlsState::new(),
        }
    }

    /// Dual-track a detached task: connection-local (TEARDOWN) and global (server stop).
    pub(crate) fn register_session_task(&mut self, handle: tokio::task::AbortHandle) {
        self.shared.register_session_task_global(handle.clone());
        self.session_tasks.push(handle);
    }

    /// Register active audio owned by this connection.
    pub(crate) fn set_active_audio(&mut self, stop: Box<dyn FnOnce() + Send>) {
        self.shared.set_active_audio(self.connection_id, stop);
    }

    /// TEARDOWN-scoped stop: active audio only if we own it, plus our local tasks.
    /// Detach-only (no joins). Legacy video RTP is stopped by the TEARDOWN handler separately.
    pub(crate) fn stop_connection_sessions(&mut self) {
        self.shared
            .stop_connection_sessions(self.connection_id, &mut self.session_tasks);
    }
}

/// Returns the connection's local IP address.
pub(crate) fn local_ip_from(conn: &RaopConnection) -> std::net::IpAddr {
    ip_from_bytes(&conn.local_addr)
}

pub(crate) fn ip_from_bytes(bytes: &[u8]) -> std::net::IpAddr {
    if bytes.len() == 16 {
        let ip: [u8; 16] = bytes[..16].try_into().unwrap_or([0; 16]);
        std::net::IpAddr::V6(std::net::Ipv6Addr::from(ip))
    } else {
        let ip: [u8; 4] = bytes[..4].try_into().unwrap_or([0; 4]);
        std::net::IpAddr::V4(std::net::Ipv4Addr::from(ip))
    }
}

/// Returns a bind address for sub-listeners (buffered audio, event channel, etc.).
/// Uses the specific local IP for routable addresses (respects BindConfig).
/// Falls back to unspecified for link-local IPv6.
pub(crate) fn bind_addr_for(conn: &RaopConnection) -> SocketAddr {
    let ip = local_ip_from(conn);
    let bind_ip = match ip {
        std::net::IpAddr::V6(v6) if (v6.segments()[0] & 0xffc0) == 0xfe80 => {
            std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED)
        }
        other => other,
    };
    SocketAddr::new(bind_ip, 0)
}

/// FairPlay DRM handshake (fp-setup M1/M2). The AP2 feature set advertises
/// `Authentication4` (bit 14), so senders run this before SETUP.
pub(crate) fn handle_fp_setup(
    conn: &mut RaopConnection,
    request: &HttpRequest,
    _response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    let data = request.data()?;
    tracing::debug!(data_len = data.len(), "fp-setup");
    match data.len() {
        16 => {
            let req: &[u8; 16] = data.try_into().ok()?;
            let res = match conn.fairplay.setup(req) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("fp-setup M1 failed: {e}");
                    conn.shared.handler.on_error(&ShairplayError::Crypto(e));
                    return None;
                }
            };
            Some(res.to_vec())
        }
        164 => {
            let req: &[u8; 164] = data.try_into().ok()?;
            let res = match conn.fairplay.handshake(req) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("fp-setup M2 failed: {e}");
                    conn.shared.handler.on_error(&ShairplayError::Crypto(e));
                    return None;
                }
            };
            Some(res.to_vec())
        }
        _ => None,
    }
}

/// RTSP OPTIONS: return supported methods.
pub(crate) fn handle_options(
    _conn: &mut RaopConnection,
    _request: &HttpRequest,
    response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    response.add_header(
        "Public",
        "ANNOUNCE, SETUP, RECORD, PAUSE, FLUSH, FLUSHBUFFERED, TEARDOWN, OPTIONS, POST, GET, PUT",
    );
    None
}

/// RTSP GET_PARAMETER: return volume or other parameters.
pub(crate) fn handle_get_parameter(
    conn: &mut RaopConnection,
    request: &HttpRequest,
    response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    let content_type = request.header("Content-Type")?;
    if content_type != "text/parameters" {
        return None;
    }

    let data = request.data()?;
    let text = std::str::from_utf8(data).ok()?;
    if text.contains("volume") {
        let volume = conn.shared.reported_volume_db.lock().map(|guard| *guard).unwrap_or(0.0);
        response.add_header("Content-Type", "text/parameters");
        return Some(format!("volume: {volume:.6}\r\n").into_bytes());
    }
    None
}

/// RTSP SET_PARAMETER: handle volume, metadata, artwork, progress.
pub(crate) fn handle_set_parameter(
    conn: &mut RaopConnection,
    request: &HttpRequest,
    _response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    let content_type = request.header("Content-Type")?;
    let data = request.data()?;
    tracing::debug!(content_type, len = data.len(), "SET_PARAMETER");

    // Volume, progress, cover art, and DMAP metadata are delivered straight to the
    // AudioHandler (never blocking the audio pipeline). Audio-pipeline commands
    // (rate/flush) arrive on their own RTSP methods, not here.
    match content_type {
        "text/parameters" => {
            let text = std::str::from_utf8(data).ok()?;
            if let Some(rest) = text.strip_prefix("volume: ") {
                if let Ok(vol) = rest.trim().parse::<f32>() {
                    if let Ok(mut guard) = conn.shared.reported_volume_db.lock() {
                        *guard = vol.clamp(crate::raop::connection::AIRPLAY_VOLUME_DB_MIN, 0.0);
                    }
                    conn.shared.handler.on_volume(vol);
                }
            } else if let Some(rest) = text.strip_prefix("progress: ") {
                let parts: Vec<&str> = rest.trim().split('/').collect();
                if parts.len() == 3 {
                    conn.shared.handler.on_progress(
                        parts[0].parse().unwrap_or(0),
                        parts[1].parse().unwrap_or(0),
                        parts[2].parse().unwrap_or(0),
                    );
                }
            }
        }
        "image/jpeg" | "image/png" => conn.shared.handler.on_coverart(data),
        "application/x-dmap-tagged" => {
            let meta = crate::proto::dmap::TrackMetadata::from_dmap(data);
            conn.shared.handler.on_metadata(&meta);
        }
        _ => {}
    }
    None
}

// Verifies that handler-facing error notification (`AudioHandler::on_error`) is
// wired to the fp-setup failure path that AP2 senders exercise.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::raop::connection::test_shared;
    use crate::raop::{AudioFormat, AudioHandler, AudioSession};
    use std::sync::Mutex;

    /// An `AudioHandler` that records every error passed to `on_error`.
    #[derive(Default)]
    struct RecordingHandler {
        errors: Mutex<Vec<String>>,
    }

    impl AudioHandler for RecordingHandler {
        fn audio_init(&self, _format: AudioFormat) -> Box<dyn AudioSession> {
            unreachable!("audio_init is not exercised by the fp-setup error path")
        }
        fn on_error(&self, error: &ShairplayError) {
            self.errors.lock().unwrap().push(error.to_string());
        }
    }

    fn test_connection(handler: Arc<RecordingHandler>) -> RaopConnection {
        let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        RaopConnection::new(test_shared(handler), 1, addr, addr)
    }

    fn fp_setup_request(body: &[u8]) -> HttpRequest {
        let mut msg = format!(
            "POST /fp-setup RTSP/1.0\r\nCSeq: 1\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        msg.extend_from_slice(body);
        let mut req = HttpRequest::new();
        req.add_data(&msg).unwrap();
        assert_eq!(req.data().map(|d| d.len()), Some(body.len()), "body should parse");
        req
    }

    /// A malformed fp-setup M1 (version byte != 0x03) must surface the FairPlay
    /// `CryptoError` to the application via `on_error`, exactly once.
    #[test]
    fn fp_setup_failure_notifies_handler() {
        let handler = Arc::new(RecordingHandler::default());
        let mut conn = test_connection(handler.clone());

        // 16-byte M1 with byte[4] = 0x00 (!= 0x03) → FairPlay::setup returns Err.
        let req = fp_setup_request(&[0u8; 16]);
        let mut resp = HttpResponse::new("RTSP/1.0", 200, "OK");

        let out = handle_fp_setup(&mut conn, &req, &mut resp);

        assert!(out.is_none(), "malformed fp-setup should decline");
        let errors = handler.errors.lock().unwrap();
        assert_eq!(errors.len(), 1, "on_error should fire exactly once: {errors:?}");
        assert!(
            errors[0].contains("unsupported version"),
            "on_error should carry the FairPlay error, got: {:?}",
            errors[0]
        );
    }

    /// A well-formed fp-setup M1 must NOT trigger `on_error`.
    #[test]
    fn fp_setup_success_does_not_notify() {
        let handler = Arc::new(RecordingHandler::default());
        let mut conn = test_connection(handler.clone());

        // Valid M1: byte[4] = 0x03 (version), byte[14] = 0 (mode).
        let mut body = [0u8; 16];
        body[4] = 0x03;
        let req = fp_setup_request(&body);
        let mut resp = HttpResponse::new("RTSP/1.0", 200, "OK");

        let out = handle_fp_setup(&mut conn, &req, &mut resp);

        assert!(out.is_some(), "valid fp-setup M1 should produce a reply");
        assert!(
            handler.errors.lock().unwrap().is_empty(),
            "no error expected on success"
        );
    }
}
