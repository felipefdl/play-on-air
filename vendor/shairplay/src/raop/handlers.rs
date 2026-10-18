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
    /// Control handle of this connection's buffered (type 103) stream.
    pub(crate) playout: Option<crate::raop::buffered_audio::PlayoutHandle>,
    /// PTP timing protocol and peer set from SETUP / SETPEERS / SETPEERSX.
    pub(crate) timing: crate::net::ptp::SessionTiming,
    /// Sender client version from `User-Agent` (gates `dvlc` volume reports).
    pub(crate) sender_version: Option<crate::raop::remote::SenderVersion>,
    /// SETUP carried `isMultiSelectAirPlay = true`: sender volume gets the 0.01 slider dead
    /// band and a `dvlc` acknowledgement (`crate::raop::volume`).
    pub(crate) multi_select: bool,
    /// Merged `updateMRNowPlayingInfo` state (`mergePolicy = update` overlays it).
    pub(crate) now_playing: crate::proto::dmap::TrackMetadata,
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
            playout: None,
            timing: crate::net::ptp::SessionTiming::default(),
            sender_version: None,
            multi_select: false,
            now_playing: crate::proto::dmap::TrackMetadata::default(),
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

    /// Make this connection's event channel the target for receiver-initiated commands.
    pub(crate) fn register_event_sender(&self, tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>) {
        let sender = crate::raop::event_channel::EventSender::new(tx, self.sender_version);
        self.shared.register_event_sender(self.connection_id, sender);
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

/// RTSP SET_PARAMETER: volume, progress, artwork, and DMAP now-playing (spec section 5).
///
/// Delivered straight to the [`AudioHandler`](crate::raop::AudioHandler) on the RTSP
/// thread, never through the audio pipeline. Metadata and artwork apply on arrival: the
/// `RTP-Info` apply time Apple honors is not used.
pub(crate) fn handle_set_parameter(
    conn: &mut RaopConnection,
    request: &HttpRequest,
    _response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    let content_type = request.header("Content-Type")?;
    let data = request.data()?;
    tracing::debug!(content_type, len = data.len(), "SET_PARAMETER");

    match content_type {
        "text/parameters" => {
            let text = std::str::from_utf8(data).ok()?;
            for line in text.lines() {
                apply_text_parameter(conn, line);
            }
        }
        "application/x-dmap-tagged" => {
            let meta = crate::proto::dmap::TrackMetadata::from_dmap(data);
            conn.shared.handler.on_metadata(&meta);
            conn.now_playing = meta;
        }
        image if image.starts_with("image/") && !data.is_empty() => conn.shared.handler.on_coverart(image, data),
        _ => {}
    }
    None
}

/// One `name: value` line of a `text/parameters` SET_PARAMETER body.
fn apply_text_parameter(conn: &RaopConnection, line: &str) {
    let Some((name, value)) = line.split_once(':') else {
        return;
    };
    let value = value.trim();
    match name.trim() {
        "volume" => {
            if let Ok(volume_db) = value.parse::<f32>()
                && volume_db.is_finite()
            {
                tracing::debug!(volume_db, multi_select = conn.multi_select, "SET_PARAMETER volume");
                apply_sender_volume(conn, volume_db);
            }
        }
        "progress" => {
            let mut parts = value.split('/').map(|part| part.trim().parse::<u32>());
            if let (Some(Ok(start)), Some(Ok(current)), Some(Ok(end)), None) =
                (parts.next(), parts.next(), parts.next(), parts.next())
            {
                conn.shared.handler.on_progress(start, current, end);
            }
        }
        other => tracing::trace!(name = other, "SET_PARAMETER: unhandled text parameter"),
    }
}

/// Apply sender volume `volume_db` (`SET_PARAMETER volume`).
///
/// Multi-select sessions follow Apple's receiver: a value within 0.01 slider of the current
/// volume is ignored, and an applied one is acknowledged to the sender with `dvlc`. The
/// current volume (`GET_PARAMETER volume`, `/info` `initialVolume`) only ever holds applied
/// values.
fn apply_sender_volume(conn: &RaopConnection, volume_db: f32) {
    let applied = volume_db.clamp(crate::raop::connection::AIRPLAY_VOLUME_DB_MIN, 0.0);
    {
        let mut current = conn
            .shared
            .reported_volume_db
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if conn.multi_select && !crate::raop::volume::noticeably_different(*current, applied) {
            tracing::debug!(
                volume_db,
                current = *current,
                "SET_PARAMETER volume within the dead band; ignored"
            );
            return;
        }
        *current = applied;
    }
    conn.shared.handler.on_volume(volume_db);
    if conn.multi_select {
        conn.shared.ack_sender_volume(applied);
    }
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

    #[derive(Default)]
    struct NowPlayingHandler {
        metadata: Mutex<Vec<crate::proto::dmap::TrackMetadata>>,
        artwork: Mutex<Vec<(String, Vec<u8>)>>,
    }

    impl AudioHandler for NowPlayingHandler {
        fn audio_init(&self, _format: AudioFormat) -> Box<dyn AudioSession> {
            unreachable!("not exercised")
        }
        fn on_metadata(&self, metadata: &crate::proto::dmap::TrackMetadata) {
            self.metadata.lock().unwrap().push(metadata.clone());
        }
        fn on_coverart(&self, mime_type: &str, coverart: &[u8]) {
            self.artwork
                .lock()
                .unwrap()
                .push((mime_type.to_owned(), coverart.to_vec()));
        }
    }

    fn command_request(params: plist::Dictionary) -> HttpRequest {
        let mut dict = plist::Dictionary::new();
        dict.insert("type".into(), plist::Value::String("updateMRNowPlayingInfo".into()));
        dict.insert("params".into(), plist::Value::Dictionary(params));
        let mut body = Vec::new();
        plist::to_writer_binary(&mut body, &dict).unwrap();
        let mut msg = format!(
            "POST /command RTSP/1.0\r\nCSeq: 3\r\nContent-Type: application/x-apple-binary-plist\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        msg.extend_from_slice(&body);
        let mut req = HttpRequest::new();
        req.add_data(&msg).unwrap();
        req
    }

    /// `updateMRNowPlayingInfo`: `update` merges into the previous state, `replace` resets it;
    /// artwork goes to `on_coverart` with its MIME type.
    #[test]
    fn now_playing_command_merges_and_replaces() {
        let handler = Arc::new(NowPlayingHandler::default());
        let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        let mut conn = RaopConnection::new(test_shared(handler.clone()), 1, addr, addr);
        let mut resp = HttpResponse::new("RTSP/1.0", 200, "OK");

        let mut first = plist::Dictionary::new();
        first.insert("title".into(), plist::Value::String("Song".into()));
        first.insert("artist".into(), plist::Value::String("Band".into()));
        first.insert("artworkData".into(), plist::Value::Data(vec![1, 2, 3]));
        first.insert("artworkMIMEType".into(), plist::Value::String("image/png".into()));
        crate::raop::handlers_ap2::handle_command(&mut conn, &command_request(first), &mut resp);

        let mut update = plist::Dictionary::new();
        update.insert("mergePolicy".into(), plist::Value::String("update".into()));
        update.insert("album".into(), plist::Value::String("Record".into()));
        crate::raop::handlers_ap2::handle_command(&mut conn, &command_request(update), &mut resp);

        let mut replace = plist::Dictionary::new();
        replace.insert("mergePolicy".into(), plist::Value::String("replace".into()));
        replace.insert("title".into(), plist::Value::String("Next".into()));
        crate::raop::handlers_ap2::handle_command(&mut conn, &command_request(replace), &mut resp);

        let metadata = handler.metadata.lock().unwrap();
        assert_eq!(metadata.len(), 3);
        assert_eq!(metadata[1].title.as_deref(), Some("Song"));
        assert_eq!(metadata[1].artist.as_deref(), Some("Band"));
        assert_eq!(metadata[1].album.as_deref(), Some("Record"));
        assert_eq!(metadata[2].title.as_deref(), Some("Next"));
        assert_eq!(metadata[2].artist, None, "replace drops earlier keys");
        assert_eq!(
            *handler.artwork.lock().unwrap(),
            vec![("image/png".to_owned(), vec![1, 2, 3])]
        );
    }

    #[derive(Default)]
    struct VolumeHandler {
        volumes: Mutex<Vec<f32>>,
    }

    impl AudioHandler for VolumeHandler {
        fn audio_init(&self, _format: AudioFormat) -> Box<dyn AudioSession> {
            unreachable!("not exercised")
        }
        fn on_volume(&self, volume: f32) {
            self.volumes.lock().unwrap().push(volume);
        }
    }

    /// Connection with a dvlc-capable sender's event channel, as SETUP leaves it.
    fn volume_connection(
        handler: Arc<VolumeHandler>,
        multi_select: bool,
    ) -> (RaopConnection, tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>) {
        let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        let mut conn = RaopConnection::new(test_shared(handler), 1, addr, addr);
        conn.sender_version = Some(crate::raop::remote::SenderVersion([860, 7, 1]));
        conn.multi_select = multi_select;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        conn.register_event_sender(tx);
        (conn, rx)
    }

    fn set_volume(conn: &mut RaopConnection, volume_db: f32) {
        let body = format!("volume: {volume_db:.6}\r\n");
        let msg = format!(
            "SET_PARAMETER rtsp://127.0.0.1/1 RTSP/1.0\r\nCSeq: 9\r\nContent-Type: text/parameters\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        let mut req = HttpRequest::new();
        req.add_data(msg.as_bytes()).unwrap();
        let mut resp = HttpResponse::new("RTSP/1.0", 200, "OK");
        handle_set_parameter(conn, &req, &mut resp);
    }

    /// `dvlc` slider values sent on the event channel so far.
    fn acks(rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>) -> Vec<f64> {
        let mut sliders = Vec::new();
        while let Ok(message) = rx.try_recv() {
            let split = message.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
            let body: plist::Value = plist::from_bytes(&message[split..]).unwrap();
            let body = body.into_dictionary().unwrap();
            assert_eq!(body.get("value").and_then(plist::Value::as_string), Some("dvlc"));
            sliders.push(body.get("volume").and_then(plist::Value::as_real).unwrap());
        }
        sliders
    }

    fn initial_volume(conn: &mut RaopConnection) -> f64 {
        let req = HttpRequest::new();
        let mut resp = HttpResponse::new("RTSP/1.0", 200, "OK");
        let body = crate::raop::handlers_ap2::handle_info(conn, &req, &mut resp).unwrap();
        let info: plist::Value = plist::from_bytes(&body).unwrap();
        info.into_dictionary()
            .unwrap()
            .get("initialVolume")
            .and_then(plist::Value::as_real)
            .unwrap()
    }

    /// Multi-select: values within 0.01 slider of the current volume are ignored, applied ones
    /// are acknowledged with `dvlc`, and `/info` reports the applied volume.
    #[tokio::test]
    async fn multi_select_volume_has_a_dead_band_and_is_acknowledged() {
        let handler = Arc::new(VolumeHandler::default());
        let (mut conn, mut rx) = volume_connection(handler.clone(), true);

        set_volume(&mut conn, -15.0);
        assert_eq!(*handler.volumes.lock().unwrap(), vec![-15.0]);
        assert_eq!(acks(&mut rx), vec![0.5], "first value acknowledged at once");

        // Same value and a 0.2 dB step (under 0.01 slider): ignored, no ack, /info unchanged.
        set_volume(&mut conn, -15.0);
        set_volume(&mut conn, -15.2);
        assert_eq!(*handler.volumes.lock().unwrap(), vec![-15.0]);
        assert!((initial_volume(&mut conn) - -15.0).abs() < 1e-6);

        // A drag inside the throttle window: applied at once, acknowledged once at its end.
        set_volume(&mut conn, -21.0);
        set_volume(&mut conn, -9.0);
        set_volume(&mut conn, -6.0);
        assert_eq!(*handler.volumes.lock().unwrap(), vec![-15.0, -21.0, -9.0, -6.0]);
        assert!((initial_volume(&mut conn) - -6.0).abs() < 1e-6);
        assert_eq!(acks(&mut rx), Vec::<f64>::new(), "throttled");
        tokio::time::sleep(crate::raop::volume::VOLUME_ACK_INTERVAL + std::time::Duration::from_millis(100)).await;
        let trailing = acks(&mut rx);
        assert_eq!(trailing.len(), 1, "one trailing ack: {trailing:?}");
        assert!(
            (trailing[0] - 0.8).abs() < 1e-6,
            "trailing ack carries the last value: {trailing:?}"
        );
    }

    /// A single-receiver session keeps applying every value and never acknowledges it.
    #[tokio::test]
    async fn single_receiver_volume_is_applied_as_sent_without_ack() {
        let handler = Arc::new(VolumeHandler::default());
        let (mut conn, mut rx) = volume_connection(handler.clone(), false);
        set_volume(&mut conn, -15.0);
        set_volume(&mut conn, -15.0);
        set_volume(&mut conn, -15.2);
        assert_eq!(*handler.volumes.lock().unwrap(), vec![-15.0, -15.0, -15.2]);
        tokio::time::sleep(crate::raop::volume::VOLUME_ACK_INTERVAL + std::time::Duration::from_millis(100)).await;
        assert_eq!(acks(&mut rx), Vec::<f64>::new());
        assert!((initial_volume(&mut conn) - -15.2).abs() < 1e-6);
    }
}
