//! AP2 RTSP request handlers — pairing, encrypted SETUP, buffered audio, video.

use crate::codec::alac::AlacFormat;
use crate::crypto::pairing_homekit::{self, PairVerifyServer, SrpServer};
#[cfg(feature = "video")]
use crate::error::CryptoError;
use crate::error::{ProtocolError, ShairplayError};
use crate::net::ptp::AnchorMapping;
use crate::proto::http::{HttpRequest, HttpResponse};
use crate::raop::buffered_audio::{AnchorTime, FlushRequest, SetRateRequest};
#[cfg(feature = "video")]
use crate::raop::rtp::RaopRtp;

use super::handlers::{RaopConnection, bind_addr_for, local_ip_from};

fn bind_tcp(addr: std::net::SocketAddr) -> Option<tokio::net::TcpListener> {
    let listener = std::net::TcpListener::bind(addr).ok()?;
    listener.set_nonblocking(true).ok()?;
    tokio::net::TcpListener::from_std(listener).ok()
}

fn bind_udp(addr: std::net::SocketAddr) -> Option<tokio::net::UdpSocket> {
    let socket = std::net::UdpSocket::bind(addr).ok()?;
    socket.set_nonblocking(true).ok()?;
    tokio::net::UdpSocket::from_std(socket).ok()
}

impl RaopConnection {
    /// Decouple network event stream listener spawning from high-level RTSP handlers.
    pub(crate) fn spawn_event_channel(
        &mut self,
        event_listener: tokio::net::TcpListener,
        event_channel_cipher: crate::crypto::chacha_transport::EncryptedChannel,
        rx: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    ) {
        let handle = tokio::spawn(async move {
            if let Ok((stream, addr)) = event_listener.accept().await {
                tracing::info!(%addr, "RC event channel client connected");
                crate::raop::event_channel::EventChannel::handle_stream(stream, event_channel_cipher, rx).await;
            }
        });
        self.register_session_task(handle.abort_handle());
    }
}

/// AP2 pair-setup: SRP-6a + HomeKit pairing (M1→M5).
pub(crate) fn handle_pair_setup(
    conn: &mut RaopConnection,
    request: &HttpRequest,
    response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    let data = request.data()?;
    response.add_header("Content-Type", "application/octet-stream");

    // AirPlay 2 pair-setup is TLV8 with a State field; legacy (AirPlay 1) pairing is unsupported.
    let tlv = match crate::crypto::tlv::TlvValues::decode(data) {
        Ok(t) if t.get(6).is_some() => t, // Must have State field
        _ => {
            tracing::debug!(
                data_len = data.len(),
                "pair-setup: no TLV state (legacy pairing unsupported)"
            );
            return None;
        }
    };
    let state = *tlv.get(6)?.first()?;

    match state {
        1 => {
            tracing::info!("AP2 pair-setup M1 received");
            let mut srp = match SrpServer::new(conn.shared.pin.as_deref()) {
                Ok(srp) => srp,
                Err(e) => {
                    tracing::warn!("pair-setup M1 init failed: {e}");
                    conn.shared.handler.on_error(&ShairplayError::Crypto(e));
                    return Some(pairing_homekit::pairing_error_response(2));
                }
            };
            if let Err(e) = srp.process_m1(data) {
                tracing::warn!("pair-setup M1 failed: {e}");
                conn.shared.handler.on_error(&ShairplayError::Crypto(e));
                return Some(pairing_homekit::pairing_error_response(2));
            }
            let m2 = srp.build_m2();
            conn.srp_server = Some(srp);
            Some(m2)
        }
        3 => {
            let srp = conn.srp_server.as_mut()?;
            let ok = srp.process_m3(data).ok()?;
            let m4 = srp.build_m4().ok()?;
            if ok && srp.is_transient() {
                conn.ap2_shared_secret = srp.shared_secret().map(|s| s.to_vec());
                conn.is_ap2 = true;
                tracing::info!("AP2 transient pair-setup complete");
            }
            Some(m4)
        }
        5 => {
            let srp = conn.srp_server.as_mut()?;
            match srp.process_m5(data) {
                Ok((client_id, client_pk)) => {
                    let m6 = srp.build_m6(&conn.shared.device_id, &conn.shared.identity_seed).ok()?;
                    conn.shared.pairing_store.put(&client_id, client_pk);
                    tracing::info!(client_id, "AP2 normal pair-setup complete, client key stored");
                    Some(m6)
                }
                Err(e) => {
                    tracing::warn!("pair-setup M5 failed: {e}");
                    conn.shared.handler.on_error(&ShairplayError::Crypto(e));
                    let mut tlv = crate::crypto::tlv::TlvValues::new();
                    tlv.add(6, &[6]); // State=6
                    tlv.add(7, &[2]); // Error=Authentication
                    Some(tlv.encode())
                }
            }
        }
        _ => None,
    }
}

/// AP2 pair-verify: Ed25519 verify + HKDF shared secret derivation.
pub(crate) fn handle_pair_verify(
    conn: &mut RaopConnection,
    request: &HttpRequest,
    response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    let data = request.data()?;
    response.add_header("Content-Type", "application/octet-stream");

    let tlv = match crate::crypto::tlv::TlvValues::decode(data) {
        Ok(t) if t.get(6).is_some() => t,
        _ => {
            tracing::debug!(
                data_len = data.len(),
                "pair-verify: no TLV state (legacy pairing unsupported)"
            );
            return None;
        }
    };
    let state = *tlv.get(6)?.first()?;
    tracing::debug!(state, data_len = data.len(), "pair-verify TLV state");

    match state {
        1 => {
            tracing::info!("AP2 pair-verify M1 received");
            let mut pv = PairVerifyServer::new(&conn.shared.device_id, &conn.shared.identity_seed);
            match pv.process_m1_build_m2(data) {
                Ok(m2) => {
                    tracing::debug!(m2_len = m2.len(), "pair-verify M2 built");
                    // Store ECDH shared secret immediately (needed for video even if M3 never arrives)
                    conn.pair_verify_secret = Some(*pv.ecdh_shared_secret());
                    conn.pair_verify = Some(pv);
                    Some(m2)
                }
                Err(e) => {
                    tracing::warn!("pair-verify M1 failed: {e}");
                    conn.shared.handler.on_error(&ShairplayError::Crypto(e));
                    None
                }
            }
        }
        3 => {
            let pv = conn.pair_verify.as_mut()?;
            let store = conn.shared.pairing_store.clone();
            match pv.process_m3_build_m4(data, Some(&|id| store.get(id))) {
                Ok(m4) => {
                    conn.pair_verify_secret = pv.shared_secret().copied();
                    conn.ap2_shared_secret = pv.shared_secret().map(|s| s.to_vec());
                    conn.is_ap2 = true;
                    tracing::info!("AP2 pair-verify complete, encrypted RTSP active");
                    Some(m4)
                }
                Err(e) => {
                    tracing::warn!("pair-verify M3 failed: {e}");
                    conn.shared.handler.on_error(&ShairplayError::Crypto(e));
                    None
                }
            }
        }
        _ => None,
    }
}

/// AP2 GET /info: return device capabilities as binary plist.
pub(crate) fn handle_info(
    conn: &mut RaopConnection,
    _request: &HttpRequest,
    response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    response.set_plist_body(&build_info_dict(&conn.shared))
}

/// The `/info` dictionary (spec section 4 "/info key set"), also the `updateInfo` value.
///
/// Audio-only subset of Apple's `sysInfo_copyInfoDictInternal` keys. Omitted on purpose:
/// `featuresEx` (no extended bits until APAP framing, bit 72, exists), `psi` and
/// `osBuildVersion` (Apple OS identity; the Sonos SDK receiver omits them),
/// `playbackCapabilities` and the screen/video keys.
fn build_info_dict(shared: &crate::raop::connection::RaopShared) -> plist::Dictionary {
    use crate::raop::config;

    let (_, vk) = crate::crypto::pairing_homekit::identity_keypair(&shared.identity_seed);
    let pk = vk.to_bytes();
    let pk_hex: String = pk.iter().map(|b| format!("{b:02x}")).collect();
    let password_required = !shared.password.is_empty();
    let requires_pin_pairing = shared.pin.is_some();
    let already_paired = shared.pairing_store.has_any_pairing();
    let txt = crate::net::mdns::airplay_txt(
        &shared.hwaddr,
        password_required,
        &pk_hex,
        &shared.pairing_id,
        requires_pin_pairing,
        already_paired,
    );
    let initial_volume = shared.reported_volume_db.lock().map(|guard| *guard).unwrap_or(0.0);
    let format_indices = |formats: &[u8]| {
        plist::Value::Array(
            formats
                .iter()
                .map(|&f| plist::Value::Integer(u64::from(f).into()))
                .collect(),
        )
    };

    let mut dict = plist::Dictionary::new();
    dict.insert("deviceID".into(), plist::Value::String(shared.device_id.clone()));
    dict.insert("macAddress".into(), plist::Value::String(shared.device_id.clone()));
    dict.insert("pi".into(), plist::Value::String(shared.pairing_id.clone()));
    dict.insert("name".into(), plist::Value::String(shared.airplay_name.clone()));
    dict.insert(
        "features".into(),
        plist::Value::Integer(crate::net::features::receiver_features_for_pairing(requires_pin_pairing).into()),
    );
    dict.insert("model".into(), plist::Value::String(config::GLOBAL_MODEL.into()));
    dict.insert(
        "protocolVersion".into(),
        plist::Value::String(config::AP2_PROTOVERS.into()),
    );
    dict.insert("sourceVersion".into(), plist::Value::String(config::AP2_SRCVERS.into()));
    dict.insert(
        "statusFlags".into(),
        plist::Value::Integer(
            u64::from(config::ap2_status_flags(
                password_required,
                requires_pin_pairing,
                already_paired,
            ))
            .into(),
        ),
    );
    dict.insert("pk".into(), plist::Value::Data(pk.to_vec()));
    dict.insert("keepAliveSendStatsAsBody".into(), plist::Value::Boolean(true));
    dict.insert(
        "supportedFormats".into(),
        plist::Value::Dictionary(plist::Dictionary::from_iter([
            (
                "audioStream".to_string(),
                plist::Value::Integer(config::audio_format_mask(&config::REALTIME_AUDIO_FORMATS).into()),
            ),
            (
                "bufferStream".to_string(),
                plist::Value::Integer(config::audio_format_mask(&config::BUFFERED_AUDIO_FORMATS).into()),
            ),
        ])),
    );
    dict.insert(
        "supportedAudioFormatsExtended".into(),
        plist::Value::Dictionary(plist::Dictionary::from_iter([(
            "bufferStream".to_string(),
            format_indices(&config::BUFFERED_AUDIO_FORMATS),
        )])),
    );
    dict.insert(
        "volumeControlType".into(),
        plist::Value::Integer(config::VOLUME_CONTROL_TYPE.into()),
    );
    dict.insert("initialVolume".into(), plist::Value::Real(f64::from(initial_volume)));
    dict.insert(
        "txtAirPlay".into(),
        plist::Value::Data(crate::net::mdns::encode_txt(&txt)),
    );

    // Video: advertise a display so the iPhone offers screen mirroring
    #[cfg(feature = "video")]
    if shared.video_handler.is_some() {
        let display = plist::Dictionary::from_iter([
            (
                "widthPixels".to_string(),
                plist::Value::Integer(config::MIRRORING_WIDTH.into()),
            ),
            (
                "heightPixels".to_string(),
                plist::Value::Integer(config::MIRRORING_HEIGHT.into()),
            ),
            ("uuid".to_string(), plist::Value::String(config::MIRRORING_UUID.into())),
            (
                "maxFPS".to_string(),
                plist::Value::Integer(config::MIRRORING_FPS.into()),
            ),
            (
                "features".to_string(),
                plist::Value::Integer(config::MIRRORING_FEATURES.into()),
            ),
        ]);
        dict.insert(
            "displays".into(),
            plist::Value::Array(vec![plist::Value::Dictionary(display)]),
        );
    }

    dict
}

/// AP2 `/pair-pin-start`: acknowledge that the accessory is ready for PIN entry.
///
/// macOS sends this after seeing PIN-required mDNS/status flags and aborts
/// normal pair-setup if the receiver answers 404.
pub(crate) fn handle_pair_pin_start(
    _conn: &mut RaopConnection,
    _request: &HttpRequest,
    response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    response.add_header("Content-Type", "application/octet-stream");
    None
}

/// HomeKit pairing-admin `Method` values (HAP spec, "Pairing TLV Methods").
const PAIRING_METHOD_ADD: u8 = 3;
const PAIRING_METHOD_REMOVE: u8 = 4;
const PAIRING_METHOD_LIST: u8 = 5;

/// HomeKit TLV error codes used by the pairing-admin endpoints.
const PAIRING_ERROR_UNKNOWN: u8 = 1;
const PAIRING_ERROR_AUTHENTICATION: u8 = 2;

/// Permissions value reported by `/pair-list`: the store keeps keys only, so every
/// controller is listed as an admin (Home adds its controllers through an admin).
const PAIRING_PERMISSION_ADMIN: u8 = 1;

/// Pairing-admin response: `State = 2` plus an optional error.
fn pairing_admin_response(error: Option<u8>) -> Vec<u8> {
    let mut tlv = crate::crypto::tlv::TlvValues::new();
    tlv.add(crate::crypto::tlv::TlvType::State as u8, &[2]);
    if let Some(error) = error {
        tlv.add(crate::crypto::tlv::TlvType::Error as u8, &[error]);
    }
    tlv.encode()
}

/// Validate a pairing-admin request and decode its TLV body.
///
/// Admin requests are only honored on a connection that completed pair-setup or
/// pair-verify, so the body arrived over the encrypted channel. Transient
/// pair-setup counts: the Home app sends its first `/pair-add` on that session.
/// HAP restricts these endpoints to admin controllers; this receiver does not,
/// because transient pairing is already open to the LAN (it plays audio
/// without a PIN). Any LAN client can therefore add or remove controllers;
/// removing the Home controllers drops the speaker from Home until it is added
/// again. The body must carry `State = 1` and, when present, the expected
/// `Method`.
fn pairing_admin_request(
    conn: &RaopConnection,
    request: &HttpRequest,
    method: u8,
    endpoint: &str,
) -> Result<crate::crypto::tlv::TlvValues, u8> {
    use crate::crypto::tlv::{TlvType, TlvValues};

    if !conn.is_ap2 {
        tracing::warn!(endpoint, "pairing admin request on an unverified connection");
        return Err(PAIRING_ERROR_AUTHENTICATION);
    }
    let tlv = request
        .data()
        .and_then(|data| TlvValues::decode(data).ok())
        .ok_or(PAIRING_ERROR_UNKNOWN)?;
    if tlv.get_type(TlvType::State) != Some(&[1]) {
        tracing::warn!(endpoint, "pairing admin request without State=1");
        return Err(PAIRING_ERROR_UNKNOWN);
    }
    if let Some(found) = tlv.get_type(TlvType::Method)
        && found != [method]
    {
        tracing::warn!(endpoint, ?found, "pairing admin request with unexpected method");
        return Err(PAIRING_ERROR_UNKNOWN);
    }
    Ok(tlv)
}

/// The controller identifier (`Identifier` TLV) of a pairing-admin request.
fn pairing_admin_identifier(tlv: &crate::crypto::tlv::TlvValues) -> Option<&str> {
    tlv.get_type(crate::crypto::tlv::TlvType::Identifier)
        .and_then(|id| std::str::from_utf8(id).ok())
        .filter(|id| !id.is_empty())
}

/// HomeKit `/pair-add`: store another controller's long-term public key.
///
/// The Home app sends this right after pairing to register the home's
/// controllers, so they can later reconnect with pair-verify.
pub(crate) fn handle_pair_add(
    conn: &mut RaopConnection,
    request: &HttpRequest,
    response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    use crate::crypto::tlv::TlvType;

    response.add_header("Content-Type", "application/octet-stream");
    let tlv = match pairing_admin_request(conn, request, PAIRING_METHOD_ADD, "pair-add") {
        Ok(tlv) => tlv,
        Err(error) => return Some(pairing_admin_response(Some(error))),
    };
    let identifier = pairing_admin_identifier(&tlv);
    let public_key = tlv
        .get_type(TlvType::PublicKey)
        .and_then(|pk| <[u8; 32]>::try_from(pk).ok());
    let (Some(identifier), Some(public_key)) = (identifier, public_key) else {
        tracing::warn!("pair-add: missing or malformed identifier/public key");
        return Some(pairing_admin_response(Some(PAIRING_ERROR_UNKNOWN)));
    };

    let store = &conn.shared.pairing_store;
    if store.get(identifier).is_some_and(|existing| existing != public_key) {
        // HAP: re-adding a known controller with a different key is an error.
        tracing::warn!(identifier, "pair-add: controller already paired with a different key");
        return Some(pairing_admin_response(Some(PAIRING_ERROR_UNKNOWN)));
    }
    store.put(identifier, public_key);
    tracing::info!(identifier, "pair-add: controller key stored");
    Some(pairing_admin_response(None))
}

/// HomeKit `/pair-remove`: forget a controller (sent when removed from Home).
pub(crate) fn handle_pair_remove(
    conn: &mut RaopConnection,
    request: &HttpRequest,
    response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    response.add_header("Content-Type", "application/octet-stream");
    let tlv = match pairing_admin_request(conn, request, PAIRING_METHOD_REMOVE, "pair-remove") {
        Ok(tlv) => tlv,
        Err(error) => return Some(pairing_admin_response(Some(error))),
    };
    let Some(identifier) = pairing_admin_identifier(&tlv) else {
        tracing::warn!("pair-remove: missing identifier");
        return Some(pairing_admin_response(Some(PAIRING_ERROR_UNKNOWN)));
    };
    conn.shared.pairing_store.remove(identifier);
    tracing::info!(identifier, "pair-remove: controller removed");
    Some(pairing_admin_response(None))
}

/// HomeKit `/pair-list`: report every stored controller.
pub(crate) fn handle_pair_list(
    conn: &mut RaopConnection,
    request: &HttpRequest,
    response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    use crate::crypto::tlv::{TlvType, TlvValues};

    response.add_header("Content-Type", "application/octet-stream");
    if let Err(error) = pairing_admin_request(conn, request, PAIRING_METHOD_LIST, "pair-list") {
        return Some(pairing_admin_response(Some(error)));
    }
    let mut tlv = TlvValues::new();
    tlv.add(TlvType::State as u8, &[2]);
    for (index, (identifier, public_key)) in conn.shared.pairing_store.list().iter().enumerate() {
        if index > 0 {
            tlv.add(TlvType::Separator as u8, &[]);
        }
        tlv.add(TlvType::Identifier as u8, identifier.as_bytes());
        tlv.add(TlvType::PublicKey as u8, public_key);
        tlv.add(TlvType::Permissions as u8, &[PAIRING_PERMISSION_ADMIN]);
    }
    Some(tlv.encode())
}

/// HomeKit `/configure`: the Home app's accessory-setup step after `/pair-add`.
///
/// The request carries the home's settings (binary plist); the reply describes the
/// accessory's HomeKit identity, as shairport-sync answers it: pairing identifier,
/// long-term public key, name, and HomeKit access control enabled at level 0
/// ("everyone on the network"). Only honored on a verified connection.
pub(crate) fn handle_configure(
    conn: &mut RaopConnection,
    request: &HttpRequest,
    response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    if !conn.is_ap2 {
        tracing::warn!("configure on an unverified connection");
        *response = HttpResponse::new("RTSP/1.0", 470, "Connection Authorization Required");
        response.add_header("CSeq", request.header("CSeq").unwrap_or("0"));
        return None;
    }
    let keys: Vec<String> = request
        .data()
        .and_then(|data| plist::from_bytes::<plist::Dictionary>(data).ok())
        .map(|dict| dict.keys().cloned().collect())
        .unwrap_or_default();
    tracing::info!(?keys, "configure request");

    let shared = &conn.shared;
    let (_, verifying_key) = pairing_homekit::identity_keypair(&shared.identity_seed);
    let mut dict = plist::Dictionary::new();
    dict.insert("Identifier".into(), plist::Value::String(shared.pairing_id.clone()));
    dict.insert("Enable_HK_Access_Control".into(), plist::Value::Boolean(true));
    dict.insert(
        "PublicKey".into(),
        plist::Value::Data(verifying_key.to_bytes().to_vec()),
    );
    dict.insert("Device_Name".into(), plist::Value::String(shared.airplay_name.clone()));
    dict.insert("Access_Control_Level".into(), plist::Value::Integer(0_u64.into()));
    response.set_plist_body(&dict)
}

/// Build the `updateInfo` `POST /command` message queued on a freshly-opened
/// event channel. Its `value` is the full `/info` dictionary, as Apple sends it
/// (spec section 4). Identical for the RC-only and normal event channels.
fn build_update_info_message(shared: &crate::raop::connection::RaopShared) -> Option<Vec<u8>> {
    let mut update_info = plist::Dictionary::new();
    update_info.insert("type".into(), plist::Value::String("updateInfo".into()));
    update_info.insert("value".into(), plist::Value::Dictionary(build_info_dict(shared)));

    let mut body = Vec::new();
    plist::to_writer_binary(&mut body, &update_info).ok()?;
    let rtsp = format!(
        "POST /command RTSP/1.0\r\nContent-Length: {}\r\nContent-Type: application/x-apple-binary-plist\r\nCSeq: 0\r\n\r\n",
        body.len()
    );
    let mut msg = rtsp.into_bytes();
    msg.extend_from_slice(&body);
    Some(msg)
}

/// AP2 SETUP: configure streams (type 96/103/110/130), event channel, timing.
pub(crate) fn handle_setup(
    conn: &mut RaopConnection,
    request: &HttpRequest,
    response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    let data = request.data()?;
    let plist_val: plist::Value = plist::from_bytes(data).ok()?;
    let dict = plist_val.as_dictionary()?;
    let keys: Vec<_> = dict.keys().collect();
    let has_streams = dict.get("streams").is_some();
    let is_mirror = dict
        .get("isScreenMirroringSession")
        .and_then(|v| v.as_boolean())
        .unwrap_or(false);
    let has_ekey = dict.get("ekey").is_some();
    let timing = dict.get("timingProtocol").and_then(|v| v.as_string()).unwrap_or("");
    tracing::info!(?keys, has_streams, is_mirror, has_ekey, timing, "SETUP plist");

    let resp_dict = if let Some(streams) = dict.get("streams").and_then(|v| v.as_array()) {
        setup_streams(conn, streams)?
    } else {
        setup_initial(conn, dict)?
    };

    response.set_plist_body(&resp_dict)
}

/// Stream SETUP (`streams` present): dispatch by stream type, then add the shared control port.
fn setup_streams(conn: &mut RaopConnection, streams: &[plist::Value]) -> Option<plist::Dictionary> {
    // Stream SETUP — type 96 (realtime) or type 103 (buffered) or type 110 (video)
    let stream0 = streams.first()?.as_dictionary()?;
    let stream_type = stream0.get("type")?.as_unsigned_integer()?;
    let stream_keys: Vec<_> = stream0.keys().collect();
    tracing::info!(stream_type, ?stream_keys, "Stream SETUP");

    let mut stream_resp = plist::Dictionary::new();
    stream_resp.insert("type".into(), plist::Value::Integer(stream_type.into()));

    match stream_type {
        96 => setup_stream_realtime(conn, stream0, &mut stream_resp)?,
        103 => setup_stream_buffered(conn, stream0, &mut stream_resp)?,
        130 => setup_stream_rc(conn, stream0, &mut stream_resp)?,
        #[cfg(feature = "video")]
        110 => setup_stream_video(conn, stream0, &mut stream_resp)?,
        _ => {
            // Type 120 = Apple Music video (animated album art / music videos). Not implemented.
            tracing::warn!(stream_type, "Unknown AP2 stream type");
        }
    }

    // Control port (shared across streams)
    let ctrl_sock = std::net::UdpSocket::bind(bind_addr_for(conn)).ok()?;
    let ctrl_port = ctrl_sock.local_addr().ok()?.port();
    drop(ctrl_sock);
    stream_resp.insert("controlPort".into(), plist::Value::Integer(ctrl_port.into()));

    let mut resp_dict = plist::Dictionary::new();
    resp_dict.insert(
        "streams".into(),
        plist::Value::Array(vec![plist::Value::Dictionary(stream_resp)]),
    );
    Some(resp_dict)
}

/// Initial SETUP (no `streams`): capture FairPlay keys (video), establish the event
/// channel and timing, and return the response dictionary.
fn setup_initial(conn: &mut RaopConnection, dict: &plist::Dictionary) -> Option<plist::Dictionary> {
    let mut resp_dict = plist::Dictionary::new();
    let timing = dict.get("timingProtocol").and_then(|v| v.as_string()).unwrap_or("None");

    // Capture FairPlay encryption keys for video.
    // The audio connection provides ekey (72 bytes, FairPlay-encrypted) + eiv (16 bytes).
    // The video connection (separate RTSP session) reads them from shared state.
    #[cfg(feature = "video")]
    {
        if let Some(ekey_data) = dict.get("ekey").and_then(|v| v.as_data())
            && ekey_data.len() == 72
            && let Ok(input) = <[u8; 72]>::try_from(ekey_data)
        {
            match conn.fairplay.decrypt(&input) {
                Ok(fp_key) => {
                    // SHA-512 two-step: hash FairPlay key with ECDH shared secret
                    // Stage 2: hash with ECDH only if AP2 pairing was used.
                    // With UxPlay-style features (bit 27 off), no pairing occurs
                    // and the raw FairPlay key is used directly.
                    let derived = if let Some(ref secret) = conn.ap2_shared_secret {
                        use sha2::{Digest, Sha512};
                        let mut hasher = Sha512::new();
                        hasher.update(fp_key);
                        hasher.update(secret);
                        let hash = hasher.finalize();
                        let mut key = [0u8; 16];
                        key.copy_from_slice(&hash[..16]);
                        key
                    } else {
                        fp_key
                    };
                    conn.ekey = Some(derived);
                    // Store in shared state for the video connection
                    if let Ok(mut shared) = conn.shared.video_ekey.write() {
                        *shared = Some(derived);
                        tracing::debug!("Video ekey stored in shared state");
                    }
                }
                Err(e) => {
                    tracing::warn!("FairPlay decrypt failed: {e:?}");
                    conn.shared.handler.on_error(&ShairplayError::Crypto(e));
                }
            }
        }
        if let Some(eiv_data) = dict.get("eiv").and_then(|v| v.as_data())
            && let Ok(iv) = <[u8; 16]>::try_from(eiv_data)
        {
            conn.eiv = Some(iv);
            if let Ok(mut shared) = conn.shared.video_eiv.write() {
                *shared = Some(iv);
                tracing::debug!("Video eiv stored in shared state");
            }
        }
    }

    let is_rc_only = dict
        .get("isRemoteControlOnly")
        .and_then(|v| v.as_boolean())
        .unwrap_or(false);

    if is_rc_only {
        tracing::info!("Remote Control Only connection - establishing event channel");

        let event_port_resp = if let Some(shared_secret) = conn.ap2_shared_secret.as_ref() {
            let event_listener = bind_tcp(bind_addr_for(conn))?;
            let event_port = event_listener.local_addr().ok()?.port();

            if let Ok(event_channel_cipher) = crate::crypto::chacha_transport::EncryptedChannel::events(shared_secret) {
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

                if let Some(msg) = build_update_info_message(&conn.shared) {
                    let _ = tx.send(msg);
                    tracing::debug!("updateInfo queued for RC event channel");
                }

                conn.spawn_event_channel(event_listener, event_channel_cipher, rx);
                conn.register_event_sender(tx);
            }
            event_port as u64
        } else {
            0
        };

        resp_dict.insert("eventPort".into(), plist::Value::Integer(event_port_resp.into()));

        return Some(resp_dict);
    }

    let group = super::types::SessionGroup::from_setup(dict);
    tracing::info!(group_uuid = ?group.group_uuid, multi_select = group.is_multi_select, "SETUP AirPlay group");
    conn.multi_select = group.is_multi_select;
    conn.shared.handler.on_session_group(&group);

    conn.timing.configure_from_setup(dict);
    if timing == "PTP" {
        let mut tpi = plist::Dictionary::new();
        let self_ip = local_ip_from(conn).to_string();
        tracing::debug!(self_ip, "timingPeerInfo address");
        let addrs = vec![plist::Value::String(self_ip.clone())];
        tpi.insert("Addresses".into(), plist::Value::Array(addrs));
        tpi.insert("ID".into(), plist::Value::String(self_ip));
        resp_dict.insert("timingPeerInfo".into(), plist::Value::Dictionary(tpi));
    }

    // Bind event port on same address family as the client connection
    let event_listener = bind_tcp(bind_addr_for(conn))?;
    let event_port = event_listener.local_addr().ok()?.port();
    tracing::info!(event_port, "Event channel opened");

    // Derive event channel encryption keys from shared secret (AP2 only).
    // In legacy mode there's no shared secret — skip the encrypted event channel.
    if let Some(shared_secret) = conn.ap2_shared_secret.as_ref()
        && let Ok(event_channel_cipher) = crate::crypto::chacha_transport::EncryptedChannel::events(shared_secret)
    {
        // Spawn bidirectional event channel
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // Queue updateInfo so it's sent immediately when client connects
        if let Some(msg) = build_update_info_message(&conn.shared) {
            let _ = tx.send(msg);
            tracing::debug!("updateInfo queued for event channel");
        }

        let handle = tokio::spawn(async move {
            if let Ok((stream, addr)) = event_listener.accept().await {
                tracing::info!(%addr, "Event channel client connected");
                crate::raop::event_channel::EventChannel::handle_stream(stream, event_channel_cipher, rx).await;
            }
        });
        conn.register_session_task(handle.abort_handle());
        conn.register_event_sender(tx);
    }

    // In legacy mode, event channel is not encrypted — return port 0 like UxPlay.
    let event_port_resp = if conn.ap2_shared_secret.is_some() {
        event_port as u64
    } else {
        0
    };
    resp_dict.insert("eventPort".into(), plist::Value::Integer(event_port_resp.into()));

    // Legacy mode: bind a standalone NTP timing socket and return its port.
    // The iPhone needs NTP sync before it sends the stream SETUP.
    // RaopRtp is created later in the stream SETUP with real ALAC parameters.
    #[cfg(feature = "video")]
    let timing_port = if !conn.is_ap2 && conn.ekey.is_some() {
        let timing_rport = dict
            .get("timingPort")
            .and_then(|v| v.as_unsigned_integer())
            .unwrap_or(0) as u16;
        let tport = bind_udp(bind_addr_for(conn))
            .and_then(|tsock| {
                let local_port = tsock.local_addr().ok()?.port();
                let mut remote_timing = conn.remote_socket;
                remote_timing.set_port(timing_rport);
                crate::raop::ntp::spawn_ntp_responder(tsock, remote_timing);
                Some(local_port)
            })
            .unwrap_or(0);
        tracing::debug!(tport, timing_rport, "Legacy video: NTP timing socket bound");
        tport
    } else {
        0
    };
    #[cfg(not(feature = "video"))]
    let timing_port: u16 = 0;

    resp_dict.insert("timingPort".into(), plist::Value::Integer((timing_port as u64).into()));

    Some(resp_dict)
}

/// Stream type 96 — realtime ALAC (ChaCha20 per-packet), or legacy AES-CBC ALAC under `video`.
fn setup_stream_realtime(
    conn: &mut RaopConnection,
    stream0: &plist::Dictionary,
    stream_resp: &mut plist::Dictionary,
) -> Option<()> {
    let sr = stream0.get("sr").and_then(|v| v.as_unsigned_integer()).unwrap_or(44100);
    let spf = stream0.get("spf").and_then(|v| v.as_unsigned_integer()).unwrap_or(352);
    let audio_format = stream0
        .get("audioFormat")
        .and_then(|v| v.as_unsigned_integer())
        .unwrap_or(0) as u32;
    let alac_format = AlacFormat::from_audio_format(audio_format).unwrap_or(AlacFormat {
        sample_rate: sr as u32,
        bit_depth: 16,
        channels: 2,
    });
    let shk = stream0.get("shk").and_then(|v| v.as_data()).unwrap_or(&[]);

    if shk.len() == 32 {
        // AP2 realtime ALAC — ChaCha20-Poly1305 per-packet encryption.
        tracing::info!(
            stream_type = 96,
            sample_rate = sr,
            samples_per_frame = spf,
            audio_format,
            alac_format = ?alac_format,
            "AP2 realtime ALAC (ChaCha20)"
        );
        if AlacFormat::from_audio_format(audio_format).is_none() {
            tracing::warn!(
                audio_format,
                sample_rate = sr,
                "Unknown AP2 realtime ALAC audioFormat; falling back to SETUP sr and 16-bit stereo"
            );
        }
        let mut shk_arr = [0u8; 32];
        shk_arr.copy_from_slice(shk);

        let socket = bind_udp(bind_addr_for(conn))?;
        let audio_port = socket.local_addr().ok()?.port();

        let handler = conn.shared.handler.clone();
        let output_config = crate::raop::realtime_audio::OutputConfig {
            source_sample_rate: alac_format.sample_rate,
            samples_per_frame: spf as u32,
            channels: alac_format.channels,
            bit_depth: alac_format.bit_depth,
            sample_rate: conn.shared.output_sample_rate,
            max_channels: conn.shared.output_max_channels,
        };

        let handle = tokio::spawn(crate::raop::realtime_audio::run(
            socket,
            shk_arr,
            handler,
            output_config,
        ));
        let abort = handle.abort_handle();
        conn.register_session_task(abort.clone());
        conn.set_active_audio(Box::new(move || abort.abort()));

        stream_resp.insert("dataPort".into(), plist::Value::Integer(audio_port.into()));
    } else {
        // Legacy ALAC — only available with video feature (UxPlay-style features).
        #[cfg(feature = "video")]
        {
            tracing::info!(stream_type = 96, sample_rate = sr, "Legacy ALAC (AES-CBC via ekey)");

            let aes_key = conn.ekey.unwrap_or([0u8; 16]);
            let aes_iv = conn.eiv.unwrap_or([0u8; 16]);
            let fmtp = format!("96 {spf} 0 16 40 10 14 2 255 0 0 {sr}");
            conn.raop_rtp = RaopRtp::new(
                conn.shared.handler.clone(),
                crate::raop::rtp::RtpConfig {
                    remote: conn.remote_socket.ip().to_string(),
                    local_addr: local_ip_from(conn),
                    rtpmap: "96 AppleLossless".to_string(),
                    fmtp,
                    aes_key,
                    aes_iv,
                    output_sample_rate: conn.shared.output_sample_rate,
                    remote_socket: conn.remote_socket,
                },
            );
            if let Some(rtp) = &mut conn.raop_rtp {
                let control_port = stream0
                    .get("controlPort")
                    .and_then(|v| v.as_unsigned_integer())
                    .unwrap_or(0) as u16;
                let (cport, _tport, dport) = rtp.start(true, control_port, 0).ok()?;
                stream_resp.insert("dataPort".into(), plist::Value::Integer(dport.into()));
                stream_resp.insert("controlPort".into(), plist::Value::Integer(cport.into()));
            }
        }
        #[cfg(not(feature = "video"))]
        {
            tracing::warn!("Type 96 without shk — requires video feature");
            conn.shared
                .handler
                .on_error(&ShairplayError::Protocol(ProtocolError::InvalidRtsp(
                    "realtime (type 96) SETUP requires a shared key or the video feature".into(),
                )));
            return None;
        }
    }
    Some(())
}

/// Stream type 103 — buffered audio over TCP with a timed playout buffer.
fn setup_stream_buffered(
    conn: &mut RaopConnection,
    stream0: &plist::Dictionary,
    stream_resp: &mut plist::Dictionary,
) -> Option<()> {
    let audio_format = stream0
        .get("audioFormat")
        .and_then(|v| v.as_unsigned_integer())
        .unwrap_or(0);
    tracing::info!(stream_type = 103, audio_format, "AP2 buffered audio stream setup");

    let shk = stream0.get("shk").and_then(|v| v.as_data()).unwrap_or(&[]);
    if shk.len() != 32 {
        tracing::warn!(len = shk.len(), "Invalid shk length");
        conn.shared
            .handler
            .on_error(&ShairplayError::Protocol(ProtocolError::InvalidRtsp(format!(
                "buffered (type 103) SETUP: invalid shk length {}",
                shk.len()
            ))));
        return None;
    }
    let mut shk_arr = [0u8; 32];
    shk_arr.copy_from_slice(shk);

    let listener = bind_tcp(bind_addr_for(conn))?;
    let audio_port = listener.local_addr().ok()?.port();
    tracing::info!(audio_port, "Buffered audio TCP port opened");

    let handler = conn.shared.handler.clone();
    let output_config = crate::raop::buffered_audio::OutputConfig {
        sample_rate: conn.shared.output_sample_rate,
        max_channels: conn.shared.output_max_channels,
        latency_samples: conn.shared.audio_latency_samples,
        release_lead: conn.shared.release_lead,
    };

    let proc = crate::raop::buffered_audio::BufferedAudioProcessor { listener };
    let (playout, reader) = proc.start(shk_arr, output_config, handler);
    conn.register_session_task(reader);
    conn.playout = Some(playout.clone());
    // Sync stop so hard_stop / TEARDOWN unblocks the delivery thread before aborting the reader.
    conn.set_active_audio(Box::new(move || {
        playout.stop();
    }));

    stream_resp.insert("dataPort".into(), plist::Value::Integer(audio_port.into()));
    stream_resp.insert(
        "audioBufferSize".into(),
        plist::Value::Integer(crate::raop::buffered_audio::AUDIO_BUFFER_SIZE_BYTES.into()),
    );
    Some(())
}

/// Stream type 130 — remote-control data channel (acknowledged on PTP, opened on RC).
fn setup_stream_rc(
    conn: &mut RaopConnection,
    stream0: &plist::Dictionary,
    stream_resp: &mut plist::Dictionary,
) -> Option<()> {
    tracing::info!("Remote Control stream setup (type 130)");

    // On PTP connections, type 130 is just acknowledged.
    // On RC connections, it sets up an encrypted data channel.
    if let Some(_seed) = stream0.get("seed").and_then(|v| v.as_unsigned_integer()) {
        let data_listener = bind_tcp(bind_addr_for(conn))?;
        let data_port = data_listener.local_addr().ok()?.port();
        tracing::debug!(data_port, "RC data channel opened");

        {
            let handle = tokio::spawn(async move {
                if let Ok((_, addr)) = data_listener.accept().await {
                    tracing::info!(%addr, "RC data channel client connected");
                }
            });
            conn.register_session_task(handle.abort_handle());
        }

        stream_resp.insert("streamID".into(), plist::Value::Integer(1_i64.into()));
        stream_resp.insert("dataPort".into(), plist::Value::Integer(data_port.into()));
    } else {
        stream_resp.insert("streamID".into(), plist::Value::Integer(1_i64.into()));
    }
    Some(())
}

#[cfg(feature = "video")]
/// Stream type 110 — screen-mirroring video. Derives the per-stream AES key/IV
/// (see [`crate::crypto::video_key`]) and spawns the video receiver.
fn setup_stream_video(
    conn: &mut RaopConnection,
    stream0: &plist::Dictionary,
    stream_resp: &mut plist::Dictionary,
) -> Option<()> {
    let stream_connection_id = stream0
        .get("streamConnectionID")
        .and_then(|v| v.as_signed_integer())
        .unwrap_or(0) as u64;
    tracing::info!(stream_type = 110, stream_connection_id, "AP2 video stream setup");

    // Seed is either the audio AES key directly (Stage-3) or
    // eaesKey = SHA-512(fairplay_key ‖ ecdh) (full FairPlay + ECDH path).
    let (ekey, eiv) = if let Some(aeskey_audio) = conn
        .ekey
        .or_else(|| conn.shared.video_ekey.read().ok()?.as_ref().copied())
    {
        tracing::debug!("Video key: Stage 3 derivation from aeskey_audio");
        crate::crypto::video_key::derive_stream_key_iv(&aeskey_audio, stream_connection_id)
    } else if let Some(ecdh) = conn.pair_verify_secret.as_ref() {
        let fp_key = conn.shared.video_ekey.read().ok().and_then(|k| *k);
        if let Some(fp_key) = fp_key {
            let eaes_key = crate::crypto::video_key::derive_eaes_key(&fp_key, ecdh);
            let (key, iv) = crate::crypto::video_key::derive_stream_key_iv(&eaes_key, stream_connection_id);
            tracing::debug!(
                derived_key = %hex::encode(key),
                derived_iv = %hex::encode(iv),
                "Video key: 3-step derivation (FairPlay + ECDH)"
            );
            (key, iv)
        } else {
            // iOS 18+ with HomeKit pairing does not send ekey; derivation is unsolved
            // (see AP2-STATUS.md). Decline the stream rather than installing a zeroed key
            // and feeding the app undecryptable "garbage" NAL units.
            tracing::warn!("Video: no ekey available — iOS 18 HomeKit video decryption unsupported; declining stream");
            conn.shared
                .handler
                .on_error(&ShairplayError::Crypto(CryptoError::FairPlay(
                    "video stream key derivation: no ekey (iOS 18 HomeKit unsupported)".into(),
                )));
            return None;
        }
    } else {
        tracing::warn!("Video stream: no encryption keys available");
        conn.shared
            .handler
            .on_error(&ShairplayError::Crypto(CryptoError::FairPlay(
                "video stream key derivation: no encryption keys available".into(),
            )));
        return None;
    };

    let cipher = crate::crypto::video_cipher::VideoCipher::new(&ekey, &eiv);

    let listener = bind_tcp(bind_addr_for(conn))?;
    let video_port = listener.local_addr().ok()?.port();
    tracing::info!(video_port, "Video stream TCP port opened");

    if let Some(vh) = &conn.shared.video_handler {
        let session = vh.video_init();
        tokio::spawn(crate::raop::video_stream::run(listener, cipher, session));
    }

    stream_resp.insert("dataPort".into(), plist::Value::Integer(video_port.into()));
    Some(())
}

/// AP2 RECORD: start buffered audio playout.
pub(crate) fn handle_record(
    conn: &mut RaopConnection,
    _request: &HttpRequest,
    response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    tracing::debug!("RECORD");
    // Samples at stream rate — builder-configurable (default ~2 s @ 48 kHz).
    response.add_header("Audio-Latency", &conn.shared.audio_latency_samples.to_string());
    None
}

/// Integer plist value as `u64` (signed values are reinterpreted bit for bit, as senders encode
/// 64-bit fractions and clock identities either way).
fn plist_uint(dict: &plist::Dictionary, key: &str) -> Option<u64> {
    let value = dict.get(key)?;
    value
        .as_unsigned_integer()
        .or_else(|| value.as_signed_integer().map(|v| v as u64))
}

/// `rtpTime` plus anchor time of a rate-1 SETRATEANCHORTIME, mapped through the PTP follower
/// when this session's timeline is locked, or deferred to the delivery thread while it locks.
fn parse_anchor(conn: &RaopConnection, dict: &plist::Dictionary) -> Option<(u32, AnchorTime)> {
    let rtp = plist_uint(dict, "rtpTime")? as u32;
    let secs = plist_uint(dict, "networkTimeSecs")?;
    let frac = plist_uint(dict, "networkTimeFrac").unwrap_or(0);
    let timeline_id = plist_uint(dict, "networkTimeTimelineID")?;
    let network_ns = crate::net::ptp::network_time_ns(secs, frac);
    let time = match conn.timing.map_anchor(
        conn.shared.ptp_clock(),
        timeline_id,
        network_ns,
        crate::util::mono_now_ns(),
    ) {
        AnchorMapping::Locked(local) => AnchorTime::Local(local),
        AnchorMapping::Locking(mapper) => AnchorTime::Locking {
            timeline_id,
            ns: network_ns,
            mapper,
        },
        AnchorMapping::Unavailable => AnchorTime::Network {
            timeline_id,
            ns: network_ns,
        },
    };
    tracing::debug!(rtp, timeline_id, network_ns, ?time, "SETRATEANCHORTIME anchor");
    Some((rtp, time))
}

/// AP2 SETRATEANCHORTIME: rate 0 stops (buffer kept), rate 1 anchors the RTP timeline.
///
/// Responds with `firstAvailableSampleTS` when audio is buffered, or `status` on error.
pub(crate) fn handle_set_rate_anchor_time(
    conn: &mut RaopConnection,
    request: &HttpRequest,
    response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    let data = request.data()?;
    let plist_val: plist::Value = plist::from_bytes(data).ok()?;
    let dict = plist_val.as_dictionary()?;
    let rate = plist_uint(dict, "rate").unwrap_or(0);
    let anchor = if rate == 1 { parse_anchor(conn, dict) } else { None };
    let Some(playout) = conn.playout.as_ref() else {
        tracing::debug!(rate, "SETRATEANCHORTIME without a buffered stream");
        return None;
    };
    let mut reply = plist::Dictionary::new();
    match playout.set_rate(SetRateRequest { rate, anchor }) {
        Ok(Some(first)) => {
            reply.insert(
                "firstAvailableSampleTS".into(),
                plist::Value::Integer(u64::from(first).into()),
            );
        }
        Ok(None) => {}
        Err(err) => {
            tracing::warn!(?err, rate, "SETRATEANCHORTIME rejected");
            reply.insert("status".into(), plist::Value::Integer(err.status().into()));
        }
    }
    if reply.is_empty() {
        None
    } else {
        response.set_plist_body(&reply)
    }
}

/// AP2 SETPEERS / SETPEERSX: the body (address strings, or peer dictionaries) replaces the
/// session's PTP peer set.
pub(crate) fn handle_set_peers(
    conn: &mut RaopConnection,
    request: &HttpRequest,
    _response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    let data = request.data()?;
    let plist_val: plist::Value = plist::from_bytes(data).ok()?;
    conn.timing.replace_peers(plist_val.as_array()?);
    None
}

/// AP2 FLUSHBUFFERED: flush-all without `flushFromSeq`, range flush with `flushFromSeq` and
/// `flushFromTS`. `flushUntilSeq` is required.
pub(crate) fn handle_flush_buffered(
    conn: &mut RaopConnection,
    request: &HttpRequest,
    response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    let data = request.data()?;
    let plist_val: plist::Value = plist::from_bytes(data).ok()?;
    let dict = plist_val.as_dictionary()?;
    let field = |key: &str| plist_uint(dict, key).map(|v| v as u32);
    let Some(until_seq) = field("flushUntilSeq") else {
        tracing::warn!("FLUSHBUFFERED without flushUntilSeq rejected");
        let mut reply = plist::Dictionary::new();
        reply.insert(
            "status".into(),
            plist::Value::Integer(crate::raop::buffered_audio::STATUS_PARAM_ERROR.into()),
        );
        return response.set_plist_body(&reply);
    };
    let flush = FlushRequest {
        from_seq: field("flushFromSeq"),
        from_ts: field("flushFromTS"),
        until_seq,
    };
    tracing::debug!(?flush, until_ts = field("flushUntilTS"), "FLUSHBUFFERED");
    if let Some(playout) = conn.playout.as_ref() {
        playout.flush(flush);
    }
    None
}

// --- AP2 POST sub-handlers ---

/// AP2 POST /feedback: empty response (required by protocol).
pub(crate) fn handle_feedback(
    conn: &mut RaopConnection,
    _request: &HttpRequest,
    response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    // Only return stream info when audio is actually playing (matches shairport-sync)
    if conn.playout.is_some() {
        let mut stream_dict = plist::Dictionary::new();
        stream_dict.insert("type".into(), plist::Value::Integer(103_i64.into()));
        stream_dict.insert("sr".into(), plist::Value::Real(44100.0));
        let mut resp_dict = plist::Dictionary::new();
        resp_dict.insert(
            "streams".into(),
            plist::Value::Array(vec![plist::Value::Dictionary(stream_dict)]),
        );
        let mut buf = Vec::new();
        plist::to_writer_binary(&mut buf, &resp_dict).ok()?;
        response.add_header("Content-Type", "application/x-apple-binary-plist");
        return Some(buf);
    }
    let _ = conn;
    None
}

/// AP2 POST /command from the sender (spec section 5, "Sender to receiver").
///
/// `updateMRNowPlayingInfo` is merged into the connection's now-playing state and surfaced
/// through [`AudioHandler::on_metadata`](crate::raop::AudioHandler::on_metadata) and
/// [`AudioHandler::on_coverart`](crate::raop::AudioHandler::on_coverart). Other command types
/// are acknowledged and logged.
pub(crate) fn handle_command(
    conn: &mut RaopConnection,
    request: &HttpRequest,
    _response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    let data = request.data()?;
    let plist_val = plist::from_bytes::<plist::Value>(data).ok()?;
    let dict = plist_val.as_dictionary()?;
    let cmd_type = dict.get("type").and_then(plist::Value::as_string).unwrap_or("unknown");
    tracing::debug!(cmd_type, "POST /command");
    if cmd_type == "updateMRNowPlayingInfo" {
        let params = dict.get("params").and_then(plist::Value::as_dictionary).unwrap_or(dict);
        let update = crate::raop::remote::parse_now_playing(params);
        if update.policy == crate::raop::remote::MergePolicy::Replace {
            conn.now_playing = crate::proto::dmap::TrackMetadata::default();
        }
        conn.now_playing.merge(update.metadata);
        conn.shared.handler.on_metadata(&conn.now_playing);
        if let Some(artwork) = update.artwork {
            conn.shared.handler.on_coverart(&artwork.mime_type, &artwork.data);
        }
    }
    None
}

/// AP2 POST /audioMode: acknowledge audio mode change.
pub(crate) fn handle_audio_mode(
    _conn: &mut RaopConnection,
    request: &HttpRequest,
    _response: &mut HttpResponse,
) -> Option<Vec<u8>> {
    if let Some(data) = request.data()
        && let Ok(plist_val) = plist::from_bytes::<plist::Value>(data)
        && let Some(dict) = plist_val.as_dictionary()
    {
        let mode = dict.get("audioMode").and_then(|v| v.as_string()).unwrap_or("unknown");
        tracing::debug!(mode, "POST /audioMode");
    }
    None
}

#[cfg(test)]
mod pairing_admin_tests {
    use super::*;
    use crate::crypto::tlv::{TlvType, TlvValues};
    use crate::raop::connection::test_shared;
    use crate::raop::{AudioFormat, AudioHandler, AudioSession};
    use std::net::SocketAddr;
    use std::sync::Arc;

    struct NoopHandler;

    impl AudioHandler for NoopHandler {
        fn audio_init(&self, _format: AudioFormat) -> Box<dyn AudioSession> {
            unreachable!("pairing admin tests never start audio")
        }
    }

    const CONTROLLER: &str = "8C2D6A3E-1F5B-4E1C-9C0A-6F0B2E7D4A11";
    const KEY: [u8; 32] = [7; 32];

    fn connection(verified: bool) -> RaopConnection {
        let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        let mut conn = RaopConnection::new(test_shared(Arc::new(NoopHandler)), 1, addr, addr);
        conn.is_ap2 = verified;
        conn
    }

    fn request(path: &str, tlv: &TlvValues) -> HttpRequest {
        let body = tlv.encode();
        let mut msg = format!(
            "POST {path} RTSP/1.0\r\nCSeq: 1\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        msg.extend_from_slice(&body);
        let mut req = HttpRequest::new();
        req.add_data(&msg).unwrap();
        req
    }

    fn add_body(identifier: &str, key: &[u8]) -> TlvValues {
        let mut tlv = TlvValues::new();
        tlv.add(TlvType::State as u8, &[1]);
        tlv.add(TlvType::Method as u8, &[PAIRING_METHOD_ADD]);
        tlv.add(TlvType::Identifier as u8, identifier.as_bytes());
        tlv.add(TlvType::PublicKey as u8, key);
        tlv.add(TlvType::Permissions as u8, &[PAIRING_PERMISSION_ADMIN]);
        tlv
    }

    fn call(
        conn: &mut RaopConnection,
        handler: fn(&mut RaopConnection, &HttpRequest, &mut HttpResponse) -> Option<Vec<u8>>,
        path: &str,
        tlv: &TlvValues,
    ) -> TlvValues {
        let mut response = HttpResponse::new("RTSP/1.0", 200, "OK");
        let body = handler(conn, &request(path, tlv), &mut response).expect("admin endpoints always answer");
        TlvValues::decode(&body).unwrap()
    }

    #[test]
    fn pair_add_stores_controller_key() {
        let mut conn = connection(true);
        let reply = call(&mut conn, handle_pair_add, "/pair-add", &add_body(CONTROLLER, &KEY));
        assert_eq!(reply.get_type(TlvType::State), Some(&[2][..]));
        assert_eq!(reply.get_type(TlvType::Error), None);
        assert_eq!(conn.shared.pairing_store.get(CONTROLLER), Some(KEY));
    }

    #[test]
    fn pair_add_is_idempotent_for_the_same_key() {
        let mut conn = connection(true);
        call(&mut conn, handle_pair_add, "/pair-add", &add_body(CONTROLLER, &KEY));
        let reply = call(&mut conn, handle_pair_add, "/pair-add", &add_body(CONTROLLER, &KEY));
        assert_eq!(reply.get_type(TlvType::Error), None);
    }

    #[test]
    fn pair_add_rejects_a_different_key_for_a_known_controller() {
        let mut conn = connection(true);
        call(&mut conn, handle_pair_add, "/pair-add", &add_body(CONTROLLER, &KEY));
        let reply = call(&mut conn, handle_pair_add, "/pair-add", &add_body(CONTROLLER, &[9; 32]));
        assert_eq!(reply.get_type(TlvType::Error), Some(&[PAIRING_ERROR_UNKNOWN][..]));
        assert_eq!(conn.shared.pairing_store.get(CONTROLLER), Some(KEY));
    }

    #[test]
    fn pair_add_requires_a_verified_connection() {
        let mut conn = connection(false);
        let reply = call(&mut conn, handle_pair_add, "/pair-add", &add_body(CONTROLLER, &KEY));
        assert_eq!(
            reply.get_type(TlvType::Error),
            Some(&[PAIRING_ERROR_AUTHENTICATION][..])
        );
        assert_eq!(conn.shared.pairing_store.get(CONTROLLER), None);
    }

    #[test]
    fn pair_add_rejects_a_malformed_key() {
        let mut conn = connection(true);
        let reply = call(&mut conn, handle_pair_add, "/pair-add", &add_body(CONTROLLER, &[1; 16]));
        assert_eq!(reply.get_type(TlvType::Error), Some(&[PAIRING_ERROR_UNKNOWN][..]));
    }

    #[test]
    fn configure_describes_the_accessory_identity() {
        let mut conn = connection(true);
        let mut body = Vec::new();
        let mut settings = plist::Dictionary::new();
        settings.insert("Name".into(), plist::Value::String("Kitchen".into()));
        plist::to_writer_binary(&mut body, &settings).unwrap();
        let mut msg = format!(
            "POST /configure RTSP/1.0\r\nCSeq: 3\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        msg.extend_from_slice(&body);
        let mut req = HttpRequest::new();
        req.add_data(&msg).unwrap();

        let mut response = HttpResponse::new("RTSP/1.0", 200, "OK");
        let reply = handle_configure(&mut conn, &req, &mut response).expect("configure answers with a plist");
        let dict: plist::Dictionary = plist::from_bytes(&reply).unwrap();
        let (_, vk) = pairing_homekit::identity_keypair(&conn.shared.identity_seed);
        assert_eq!(
            dict.get("PublicKey").and_then(plist::Value::as_data),
            Some(&vk.to_bytes()[..])
        );
        assert_eq!(
            dict.get("Enable_HK_Access_Control").and_then(plist::Value::as_boolean),
            Some(true)
        );
        assert!(dict.contains_key("Identifier"));
        assert!(dict.contains_key("Device_Name"));
    }

    #[test]
    fn configure_requires_a_verified_connection() {
        let mut conn = connection(false);
        let mut response = HttpResponse::new("RTSP/1.0", 200, "OK");
        let reply = handle_configure(&mut conn, &request("/configure", &TlvValues::new()), &mut response);
        assert!(reply.is_none());
        assert_eq!(response.status_code(), 470);
    }

    #[test]
    fn pair_list_reports_stored_controllers_and_pair_remove_forgets_them() {
        let mut conn = connection(true);
        call(&mut conn, handle_pair_add, "/pair-add", &add_body(CONTROLLER, &KEY));

        let mut list = TlvValues::new();
        list.add(TlvType::State as u8, &[1]);
        list.add(TlvType::Method as u8, &[PAIRING_METHOD_LIST]);
        let reply = call(&mut conn, handle_pair_list, "/pair-list", &list);
        assert_eq!(reply.get_type(TlvType::State), Some(&[2][..]));
        assert_eq!(reply.get_type(TlvType::Identifier), Some(CONTROLLER.as_bytes()));
        assert_eq!(reply.get_type(TlvType::PublicKey), Some(&KEY[..]));

        let mut remove = TlvValues::new();
        remove.add(TlvType::State as u8, &[1]);
        remove.add(TlvType::Method as u8, &[PAIRING_METHOD_REMOVE]);
        remove.add(TlvType::Identifier as u8, CONTROLLER.as_bytes());
        let reply = call(&mut conn, handle_pair_remove, "/pair-remove", &remove);
        assert_eq!(reply.get_type(TlvType::Error), None);
        assert_eq!(conn.shared.pairing_store.get(CONTROLLER), None);
    }
}
