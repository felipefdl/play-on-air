//! Reverse control and now-playing messages (receiver ↔ sender `POST /command`).
//!
//! Receiver to sender: `sendMediaRemoteCommand` on the encrypted event channel, either a
//! four-character playback code or the `dvlc` absolute-volume report. Sender to receiver:
//! `updateMRNowPlayingInfo` and friends on the RTSP connection. Shapes follow
//! `docs/ap2-receiver-spec.md` section 5; volume math follows section 6.

use crate::error::RemoteError;
use crate::proto::dmap::TrackMetadata;

/// Bottom of the audible AirPlay volume range in dB (slider 0).
const VOLUME_DB_FLOOR: f32 = -30.0;

/// First sender version that takes `dvlc` volume reports on the event channel.
///
/// Older senders expect a DACP request, which this fork does not implement.
pub(crate) const DVLC_MIN_SENDER_VERSION: SenderVersion = SenderVersion([354, 54, 6]);

/// Playback command the receiver asks the sender to perform.
///
/// Sent as `{type: "sendMediaRemoteCommand", value: <code>}`. Only `value` is sent: the
/// `modernMediaRemoteCommand` numbers are not known, and `value` alone is what pre-modern
/// senders expect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaRemoteCommand {
    /// Start or resume playback (`play`).
    Play,
    /// Pause playback (`paus`).
    Pause,
    /// Toggle play and pause (`plps`).
    PlayPause,
    /// Stop playback (`stop`).
    Stop,
    /// Skip to the next item (`nitm`).
    NextItem,
    /// Go back to the previous item (`pitm`).
    PreviousItem,
}

impl MediaRemoteCommand {
    /// Four-character wire code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Play => "play",
            Self::Pause => "paus",
            Self::PlayPause => "plps",
            Self::Stop => "stop",
            Self::NextItem => "nitm",
            Self::PreviousItem => "pitm",
        }
    }

    /// Whether the receiver must pause its own output before forwarding the command.
    ///
    /// Apple's receiver pauses locally first for `paus`, `stop`, and `plps`, so audio stops
    /// at once instead of after the sender's round trip.
    #[must_use]
    pub const fn pauses_locally(self) -> bool {
        matches!(self, Self::Pause | Self::Stop | Self::PlayPause)
    }
}

/// Sender client version from the `User-Agent: AirPlay/<major>.<minor>.<patch>` header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SenderVersion(pub(crate) [u32; 3]);

impl SenderVersion {
    /// Parse `AirPlay/860.7.1` (missing components count as 0). `None` for other agents.
    pub(crate) fn from_user_agent(user_agent: &str) -> Option<Self> {
        let version = user_agent.trim().strip_prefix("AirPlay/")?;
        let version = version.split_whitespace().next()?;
        let mut parts = [0_u32; 3];
        for (slot, part) in parts.iter_mut().zip(version.split('.')) {
            *slot = part.parse().ok()?;
        }
        Some(Self(parts))
    }
}

impl std::fmt::Display for SenderVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let [major, minor, patch] = self.0;
        write!(f, "{major}.{minor}.{patch}")
    }
}

/// Map an iOS slider position (`0..=1`) to AirPlay dB: linear onto `-30..=0`.
pub(crate) fn slider_to_db(slider: f32) -> f32 {
    let slider = if slider.is_finite() {
        slider.clamp(0.0, 1.0)
    } else {
        0.0
    };
    VOLUME_DB_FLOOR + slider * -VOLUME_DB_FLOOR
}

/// iOS slider position (`0..=1`) of AirPlay `volume_db`: linear over `-30..=0` dB, mute (and
/// anything at or below -30 dB) at 0.
pub(crate) fn db_to_slider(volume_db: f32) -> f32 {
    if !volume_db.is_finite() {
        return 0.0;
    }
    ((volume_db - VOLUME_DB_FLOOR) / -VOLUME_DB_FLOOR).clamp(0.0, 1.0)
}

/// `{type: "sendMediaRemoteCommand", value: <code>}`.
pub(crate) fn media_remote_command_body(command: MediaRemoteCommand) -> plist::Dictionary {
    let mut body = plist::Dictionary::new();
    body.insert("type".into(), plist::Value::String("sendMediaRemoteCommand".into()));
    body.insert("value".into(), plist::Value::String(command.code().into()));
    body
}

/// `dvlc` absolute-volume report: `volume` and `isMuted` both at top level and in `params`.
pub(crate) fn volume_report_body(slider: f32, muted: bool) -> plist::Dictionary {
    let slider = if slider.is_finite() {
        slider.clamp(0.0, 1.0)
    } else {
        0.0
    };
    let mut params = plist::Dictionary::new();
    params.insert("volume".into(), plist::Value::Real(f64::from(slider)));
    params.insert("isMuted".into(), plist::Value::Boolean(muted));
    let mut body = plist::Dictionary::new();
    body.insert("type".into(), plist::Value::String("sendMediaRemoteCommand".into()));
    body.insert("value".into(), plist::Value::String("dvlc".into()));
    body.insert("volume".into(), plist::Value::Real(f64::from(slider)));
    body.insert("isMuted".into(), plist::Value::Boolean(muted));
    body.insert("params".into(), plist::Value::Dictionary(params));
    body
}

/// Frame `body` as `POST /command RTSP/1.0` with a binary plist (plaintext; the event
/// channel encrypts it).
pub(crate) fn encode_command_request(cseq: u32, body: &plist::Dictionary) -> Result<Vec<u8>, RemoteError> {
    let mut plist_bytes = Vec::new();
    plist::to_writer_binary(&mut plist_bytes, body).map_err(|err| RemoteError::Encode(err.to_string()))?;
    let mut message = format!(
        "POST /command RTSP/1.0\r\nContent-Length: {}\r\nContent-Type: application/x-apple-binary-plist\r\nCSeq: {cseq}\r\n\r\n",
        plist_bytes.len()
    )
    .into_bytes();
    message.extend_from_slice(&plist_bytes);
    Ok(message)
}

/// Artwork image delivered by the sender.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Artwork {
    /// MIME type (`image/jpeg`, `image/png`, …).
    pub(crate) mime_type: String,
    /// Image bytes.
    pub(crate) data: Vec<u8>,
}

/// How an `updateMRNowPlayingInfo` combines with the previous now-playing state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergePolicy {
    /// Overlay the keys present.
    Update,
    /// Start from empty.
    Replace,
}

/// Parsed `updateMRNowPlayingInfo` params.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NowPlayingUpdate {
    pub(crate) policy: MergePolicy,
    pub(crate) metadata: TrackMetadata,
    pub(crate) artwork: Option<Artwork>,
}

/// Parse `updateMRNowPlayingInfo` `params` (keys from spec section 5).
///
/// The now-playing dictionary may sit directly in `params` or under `params.nowPlayingInfo`.
pub(crate) fn parse_now_playing(params: &plist::Dictionary) -> NowPlayingUpdate {
    let policy = match params.get("mergePolicy").and_then(plist::Value::as_string) {
        Some("replace") => MergePolicy::Replace,
        _ => MergePolicy::Update,
    };
    let info = params
        .get("nowPlayingInfo")
        .and_then(plist::Value::as_dictionary)
        .unwrap_or(params);
    let string = |key: &str| info.get(key).and_then(plist::Value::as_string).map(str::to_owned);
    let number = |key: &str| {
        info.get(key).and_then(|value| {
            value
                .as_real()
                .or_else(|| value.as_signed_integer().map(|n| n as f64))
                .or_else(|| value.as_unsigned_integer().map(|n| n as f64))
        })
    };
    let small = |key: &str| {
        number(key)
            .filter(|n| (0.0..=f64::from(u16::MAX)).contains(n))
            .map(|n| n as u16)
    };
    let metadata = TrackMetadata {
        title: string("title"),
        artist: string("artist"),
        album: string("album"),
        duration_ms: number("duration")
            .filter(|secs| secs.is_finite() && *secs >= 0.0)
            .map(|secs| (secs * 1000.0).min(f64::from(u32::MAX)) as u32),
        track_number: small("trackNumber"),
        track_count: small("totalTracks"),
        ..TrackMetadata::default()
    };
    let artwork = info
        .get("artworkData")
        .and_then(plist::Value::as_data)
        .filter(|data| !data.is_empty())
        .map(|data| Artwork {
            mime_type: string("artworkMIMEType").unwrap_or_else(|| "image/jpeg".to_owned()),
            data: data.to_vec(),
        });
    NowPlayingUpdate {
        policy,
        metadata,
        artwork,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_body(message: &[u8]) -> (String, plist::Dictionary) {
        let split = message.windows(4).position(|w| w == b"\r\n\r\n").expect("header end") + 4;
        let head = String::from_utf8(message[..split].to_vec()).expect("ascii head");
        let body: plist::Value = plist::from_bytes(&message[split..]).expect("binary plist body");
        (head, body.into_dictionary().expect("dict body"))
    }

    #[test]
    fn pause_command_wire_shape() {
        let message = encode_command_request(7, &media_remote_command_body(MediaRemoteCommand::Pause)).unwrap();
        let (head, body) = decode_body(&message);
        let body_len = message.len() - head.len();
        assert_eq!(
            head,
            format!(
                "POST /command RTSP/1.0\r\nContent-Length: {body_len}\r\nContent-Type: application/x-apple-binary-plist\r\nCSeq: 7\r\n\r\n"
            )
        );
        assert!(message[head.len()..].starts_with(b"bplist00"), "binary plist body");
        assert_eq!(body.len(), 2, "only type + value: {body:?}");
        assert_eq!(
            body.get("type").and_then(plist::Value::as_string),
            Some("sendMediaRemoteCommand")
        );
        assert_eq!(body.get("value").and_then(plist::Value::as_string), Some("paus"));
    }

    #[test]
    fn command_codes_match_spec() {
        let codes: Vec<_> = [
            MediaRemoteCommand::Play,
            MediaRemoteCommand::Pause,
            MediaRemoteCommand::PlayPause,
            MediaRemoteCommand::Stop,
            MediaRemoteCommand::NextItem,
            MediaRemoteCommand::PreviousItem,
        ]
        .iter()
        .map(|c| c.code())
        .collect();
        assert_eq!(codes, ["play", "paus", "plps", "stop", "nitm", "pitm"]);
        assert!(MediaRemoteCommand::Pause.pauses_locally());
        assert!(MediaRemoteCommand::Stop.pauses_locally());
        assert!(MediaRemoteCommand::PlayPause.pauses_locally());
        assert!(!MediaRemoteCommand::Play.pauses_locally());
    }

    #[test]
    fn dvlc_volume_report_shape() {
        let message = encode_command_request(1, &volume_report_body(0.25, true)).unwrap();
        let (_, body) = decode_body(&message);
        assert_eq!(
            body.get("type").and_then(plist::Value::as_string),
            Some("sendMediaRemoteCommand")
        );
        assert_eq!(body.get("value").and_then(plist::Value::as_string), Some("dvlc"));
        assert_eq!(body.get("volume").and_then(plist::Value::as_real), Some(0.25));
        assert_eq!(body.get("isMuted").and_then(plist::Value::as_boolean), Some(true));
        let params = body
            .get("params")
            .and_then(plist::Value::as_dictionary)
            .expect("params");
        assert_eq!(params.get("volume").and_then(plist::Value::as_real), Some(0.25));
        assert_eq!(params.get("isMuted").and_then(plist::Value::as_boolean), Some(true));
        assert_eq!(body.len(), 5, "type, value, volume, isMuted, params: {body:?}");
    }

    #[test]
    fn dvlc_clamps_slider() {
        let body = volume_report_body(1.7, false);
        assert_eq!(body.get("volume").and_then(plist::Value::as_real), Some(1.0));
        let body = volume_report_body(f32::NAN, false);
        assert_eq!(body.get("volume").and_then(plist::Value::as_real), Some(0.0));
    }

    #[test]
    fn slider_to_db_is_linear_over_minus_30() {
        assert!((slider_to_db(0.0) - -30.0).abs() < f32::EPSILON);
        assert!((slider_to_db(0.5) - -15.0).abs() < f32::EPSILON);
        assert!((slider_to_db(1.0) - 0.0).abs() < f32::EPSILON);
        assert!((slider_to_db(-1.0) - -30.0).abs() < f32::EPSILON);
    }

    #[test]
    fn sender_version_parse_and_gate() {
        assert_eq!(
            SenderVersion::from_user_agent("AirPlay/860.7.1"),
            Some(SenderVersion([860, 7, 1]))
        );
        assert_eq!(
            SenderVersion::from_user_agent("AirPlay/354.54"),
            Some(SenderVersion([354, 54, 0]))
        );
        assert_eq!(SenderVersion::from_user_agent("iTunes/12.0"), None);
        assert!(SenderVersion([354, 54, 6]) >= DVLC_MIN_SENDER_VERSION);
        assert!(SenderVersion([354, 54, 5]) < DVLC_MIN_SENDER_VERSION);
        assert!(SenderVersion([400, 0, 0]) >= DVLC_MIN_SENDER_VERSION);
    }

    #[test]
    fn now_playing_parse_update_and_artwork() {
        let mut params = plist::Dictionary::new();
        params.insert("title".into(), plist::Value::String("Song".into()));
        params.insert("artist".into(), plist::Value::String("Band".into()));
        params.insert("album".into(), plist::Value::String("Record".into()));
        params.insert("duration".into(), plist::Value::Real(201.5));
        params.insert("trackNumber".into(), plist::Value::Integer(3.into()));
        params.insert("artworkData".into(), plist::Value::Data(vec![0xFF, 0xD8, 0xFF]));
        params.insert("artworkMIMEType".into(), plist::Value::String("image/png".into()));
        let update = parse_now_playing(&params);
        assert_eq!(update.policy, MergePolicy::Update);
        assert_eq!(update.metadata.title.as_deref(), Some("Song"));
        assert_eq!(update.metadata.artist.as_deref(), Some("Band"));
        assert_eq!(update.metadata.album.as_deref(), Some("Record"));
        assert_eq!(update.metadata.duration_ms, Some(201_500));
        assert_eq!(update.metadata.track_number, Some(3));
        assert_eq!(
            update.artwork,
            Some(Artwork {
                mime_type: "image/png".into(),
                data: vec![0xFF, 0xD8, 0xFF]
            })
        );
    }

    #[test]
    fn now_playing_replace_nested_info() {
        let mut info = plist::Dictionary::new();
        info.insert("title".into(), plist::Value::String("Nested".into()));
        let mut params = plist::Dictionary::new();
        params.insert("mergePolicy".into(), plist::Value::String("replace".into()));
        params.insert("nowPlayingInfo".into(), plist::Value::Dictionary(info));
        let update = parse_now_playing(&params);
        assert_eq!(update.policy, MergePolicy::Replace);
        assert_eq!(update.metadata.title.as_deref(), Some("Nested"));
        assert_eq!(update.artwork, None);
    }
}
