//! mDNS service registration for AirPlay network discovery.
//!
//! Platform-conditional: uses `astro-dnssd` (native Bonjour) on macOS,
//! `mdns-sd` (pure Rust) on Linux and other platforms.

use crate::error::NetworkError;
use crate::util;

// --- `_raop._tcp` TXT record constants ---

/// Codecs: 0=PCM, 1=ALAC.
pub(crate) const RAOP_CN: &str = "0,1";
/// Digest auth supported.
pub(crate) const RAOP_DA: &str = "true";
/// Transport protocols. Apple and the Sonos SDK receiver both advertise `UDP` only.
pub(crate) const RAOP_TP: &str = "UDP";
/// Metadata types: 0=text, 1=artwork, 2=progress.
pub(crate) const RAOP_MD: &str = "0,1,2";
/// Encryption types advertised in `_raop` TXT (0=none, 3/5=FairPlay).
pub(crate) const RAOP_ET: &str = "0,3,5";
/// `_raop` protocol version (65537 = 0x10001).
pub(crate) const RAOP_VN: &str = "65537";

// --- Shared AirPlay 2 TXT record constants ---

/// Device model identifier.
pub(crate) const GLOBAL_MODEL: &str = crate::raop::config::GLOBAL_MODEL;
/// Source version string.
pub(crate) const AP2_SRCVERS: &str = crate::raop::config::AP2_SRCVERS;
/// Receiver settings flags (`rsf`). The Sonos SDK receiver sends `0x0`; Apple's Mac sends `0x8`.
pub(crate) const AP2_RSF: &str = "0x0";
/// Firmware version string.
pub(crate) const AP2_FW_VERSION: &str = "77.40.00";
/// Protocol version string.
pub(crate) const AP2_PROTOVERS: &str = crate::raop::config::AP2_PROTOVERS;

/// mDNS service information for AirPlay network discovery.
#[derive(Debug, Clone)]
pub struct AirPlayServiceInfo {
    /// RAOP service name ("AABBCCDDEEFF@Display Name").
    pub raop_name: String,
    /// AirPlay display name.
    pub airplay_name: String,
    /// RTSP listening port.
    pub port: u16,
    /// TXT records for _raop._tcp.
    pub raop_txt: Vec<(String, String)>,
    /// TXT records for _airplay._tcp.
    pub airplay_txt: Vec<(String, String)>,
}

impl AirPlayServiceInfo {
    /// Create AirPlay 2 service info (`_raop._tcp` + `_airplay._tcp` TXT records).
    /// `pk_hex` is the hex-encoded Ed25519 public key, `pi` is the pairing identifier (UUID).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: &str,
        port: u16,
        hwaddr: &[u8],
        password: bool,
        pk_hex: &str,
        pi: &str,
        requires_pin_pairing: bool,
        already_paired: bool,
    ) -> Self {
        let hw_raop = util::hwaddr_raop(hwaddr);
        let raop_name = format!("{hw_raop}@{name}");

        let ft = features_txt(requires_pin_pairing);
        let status_flags = crate::raop::config::ap2_status_flags(password, requires_pin_pairing, already_paired);

        // Key set follows Apple's Mac and the Sonos SDK receiver (spec section 4).
        // A password is signaled by statusFlags bit 7, not a `pw` key.
        let raop_txt = vec![
            ("cn".into(), RAOP_CN.into()),
            ("da".into(), RAOP_DA.into()),
            ("et".into(), RAOP_ET.into()),
            ("ft".into(), ft),
            ("fv".into(), AP2_FW_VERSION.into()),
            ("sf".into(), format!("0x{status_flags:X}")),
            ("md".into(), RAOP_MD.into()),
            ("am".into(), GLOBAL_MODEL.into()),
            ("pk".into(), pk_hex.into()),
            ("tp".into(), RAOP_TP.into()),
            ("vn".into(), RAOP_VN.into()),
            ("vs".into(), AP2_SRCVERS.into()),
        ];

        let airplay_txt = airplay_txt(hwaddr, password, pk_hex, pi, requires_pin_pairing, already_paired);

        Self {
            raop_name,
            airplay_name: name.to_string(),
            port,
            raop_txt,
            airplay_txt,
        }
    }
}

/// `features` / `ft` TXT value: `0x<low 32 bits>,0x<high 32 bits>`.
fn features_txt(requires_pin_pairing: bool) -> String {
    let features = super::features::receiver_features_for_pairing(requires_pin_pairing);
    let features_lo = features & 0xFFFF_FFFF;
    let features_hi = features >> 32;
    format!("0x{features_lo:X},0x{features_hi:X}")
}

/// `_airplay._tcp` TXT key/value pairs.
///
/// Shared by mDNS registration and the `txtAirPlay` key of `GET /info`, which Apple fills
/// with the same record. `gid` equals `pi`, as the Sonos SDK receiver does. Apple-only keys
/// with no meaning for this receiver (`btaddr`, `osvers`, `vv`, `psi`) are omitted, as the
/// Sonos SDK receiver omits them.
pub(crate) fn airplay_txt(
    hwaddr: &[u8],
    password: bool,
    pk_hex: &str,
    pi: &str,
    requires_pin_pairing: bool,
    already_paired: bool,
) -> Vec<(String, String)> {
    let status_flags = crate::raop::config::ap2_status_flags(password, requires_pin_pairing, already_paired);
    vec![
        ("acl".into(), "0".into()),
        ("deviceid".into(), util::hwaddr_airplay(hwaddr)),
        ("features".into(), features_txt(requires_pin_pairing)),
        ("rsf".into(), AP2_RSF.into()),
        ("flags".into(), format!("0x{status_flags:X}")),
        ("gid".into(), pi.into()),
        ("igl".into(), "0".into()),
        ("gcgl".into(), "0".into()),
        ("model".into(), GLOBAL_MODEL.into()),
        ("protovers".into(), AP2_PROTOVERS.into()),
        ("pi".into(), pi.into()),
        ("pk".into(), pk_hex.into()),
        ("srcvers".into(), AP2_SRCVERS.into()),
        ("fv".into(), AP2_FW_VERSION.into()),
    ]
}

/// Encode TXT key/value pairs as DNS TXT record bytes (length-prefixed `key=value` strings).
///
/// Entries longer than 255 bytes cannot be represented and are skipped.
pub(crate) fn encode_txt(txt: &[(String, String)]) -> Vec<u8> {
    let mut out = Vec::new();
    for (key, value) in txt {
        let len = key.len() + 1 + value.len();
        let Ok(len) = u8::try_from(len) else {
            continue;
        };
        out.push(len);
        out.extend_from_slice(key.as_bytes());
        out.push(b'=');
        out.extend_from_slice(value.as_bytes());
    }
    out
}

/// Convert TXT record pairs to a `HashMap` for mDNS registration.
fn txt_map(txt: &[(String, String)]) -> std::collections::HashMap<String, String> {
    txt.iter().cloned().collect()
}

// --- macOS: native Bonjour via astro-dnssd ---

/// mDNS service registration for AirPlay.
///
/// On macOS, uses native Bonjour via `astro-dnssd`.
/// On Linux and other platforms, uses pure Rust `mdns-sd`.
#[cfg(target_os = "macos")]
pub(crate) struct MdnsService {
    _raop_reg: Option<astro_dnssd::RegisteredDnsService>,
    _airplay_reg: Option<astro_dnssd::RegisteredDnsService>,
}

#[cfg(target_os = "macos")]
impl MdnsService {
    /// Create a new mDNS service manager.
    pub(crate) fn new() -> Result<Self, NetworkError> {
        Ok(Self {
            _raop_reg: None,
            _airplay_reg: None,
        })
    }

    /// Register the _raop._tcp mDNS service.
    pub(crate) fn register_raop(&mut self, info: &AirPlayServiceInfo) -> Result<(), NetworkError> {
        let reg = astro_dnssd::DNSServiceBuilder::new("_raop._tcp", info.port)
            .with_name(&info.raop_name)
            .with_txt_record(txt_map(&info.raop_txt))
            .register()
            .map_err(|e| NetworkError::Mdns(format!("{e:?}")))?;
        tracing::info!(name = %info.raop_name, port = info.port, "mDNS: _raop._tcp registered");
        self._raop_reg = Some(reg);
        Ok(())
    }

    /// Register the _airplay._tcp mDNS service.
    pub(crate) fn register_airplay(&mut self, info: &AirPlayServiceInfo) -> Result<(), NetworkError> {
        let reg = astro_dnssd::DNSServiceBuilder::new("_airplay._tcp", info.port)
            .with_name(&info.airplay_name)
            .with_txt_record(txt_map(&info.airplay_txt))
            .register()
            .map_err(|e| NetworkError::Mdns(format!("{e:?}")))?;
        tracing::info!(name = %info.airplay_name, port = info.port, "mDNS: _airplay._tcp registered");
        self._airplay_reg = Some(reg);
        Ok(())
    }

    /// Unregister the _raop._tcp mDNS service.
    pub(crate) fn unregister_raop(&mut self) {
        self._raop_reg = None;
    }
    /// Unregister the _airplay._tcp mDNS service.
    pub(crate) fn unregister_airplay(&mut self) {
        self._airplay_reg = None;
    }
}

#[cfg(target_os = "macos")]
impl Drop for MdnsService {
    fn drop(&mut self) {
        self.unregister_raop();
        self.unregister_airplay();
    }
}

// --- Linux/other: pure Rust mdns-sd ---

#[cfg(not(target_os = "macos"))]
/// mDNS service registration for AirPlay (Linux: pure Rust mdns-sd).
pub(crate) struct MdnsService {
    daemon: mdns_sd::ServiceDaemon,
    raop_fullname: Option<String>,
    airplay_fullname: Option<String>,
}

#[cfg(not(target_os = "macos"))]
impl MdnsService {
    /// Create a new mDNS service manager.
    pub(crate) fn new() -> Result<Self, NetworkError> {
        let daemon = mdns_sd::ServiceDaemon::new().map_err(|e| NetworkError::Mdns(format!("{e}")))?;
        Ok(Self {
            daemon,
            raop_fullname: None,
            airplay_fullname: None,
        })
    }

    /// Register the _raop._tcp mDNS service.
    pub(crate) fn register_raop(&mut self, info: &AirPlayServiceInfo) -> Result<(), NetworkError> {
        let svc = mdns_sd::ServiceInfo::new(
            "_raop._tcp.local.",
            &info.raop_name,
            &format!("{}.local.", gethostname::gethostname().to_string_lossy()),
            "",
            info.port,
            txt_map(&info.raop_txt),
        )
        .map(|svc| svc.enable_addr_auto())
        .map_err(|e| NetworkError::Mdns(format!("{e}")))?;
        self.raop_fullname = Some(svc.get_fullname().to_string());
        self.daemon
            .register(svc)
            .map_err(|e| NetworkError::Mdns(format!("{e}")))?;
        tracing::info!(name = %info.raop_name, port = info.port, "mDNS: _raop._tcp registered");
        Ok(())
    }

    /// Register the _airplay._tcp mDNS service.
    pub(crate) fn register_airplay(&mut self, info: &AirPlayServiceInfo) -> Result<(), NetworkError> {
        let svc = mdns_sd::ServiceInfo::new(
            "_airplay._tcp.local.",
            &info.airplay_name,
            &format!("{}.local.", gethostname::gethostname().to_string_lossy()),
            "",
            info.port,
            txt_map(&info.airplay_txt),
        )
        .map(|svc| svc.enable_addr_auto())
        .map_err(|e| NetworkError::Mdns(format!("{e}")))?;
        self.airplay_fullname = Some(svc.get_fullname().to_string());
        self.daemon
            .register(svc)
            .map_err(|e| NetworkError::Mdns(format!("{e}")))?;
        tracing::info!(name = %info.airplay_name, port = info.port, "mDNS: _airplay._tcp registered");
        Ok(())
    }

    /// Unregister the _raop._tcp mDNS service.
    pub(crate) fn unregister_raop(&mut self) {
        if let Some(name) = self.raop_fullname.take() {
            let _ = self.daemon.unregister(&name);
        }
    }

    /// Unregister the _airplay._tcp mDNS service.
    pub(crate) fn unregister_airplay(&mut self) {
        if let Some(name) = self.airplay_fullname.take() {
            let _ = self.daemon.unregister(&name);
        }
    }
}

#[cfg(not(target_os = "macos"))]
impl Drop for MdnsService {
    fn drop(&mut self) {
        self.unregister_raop();
        self.unregister_airplay();
        let _ = self.daemon.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ap2_raop_txt_has_required_fields() {
        let info = AirPlayServiceInfo::new(
            "Test Speaker",
            7000,
            &[0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF],
            false,
            "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            "12345678-1234-1234-1234-123456789abc",
            false,
            false,
        );

        let keys: Vec<&str> = info.raop_txt.iter().map(|(k, _)| k.as_str()).collect();
        let find = |key: &str| info.raop_txt.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str());

        // Key set of Apple's Mac / the Sonos SDK receiver (spec section 4): no `pw`, `ov`.
        assert_eq!(
            keys,
            ["cn", "da", "et", "ft", "fv", "sf", "md", "am", "pk", "tp", "vn", "vs"]
        );
        assert_eq!(find("vn"), Some("65537"));
        assert_eq!(find("tp"), Some("UDP"));
        assert_eq!(find("cn"), Some("0,1"));
        assert_eq!(find("da"), Some("true"));
        assert_eq!(find("sf"), Some("0x4"));
    }

    #[test]
    fn ap2_airplay_txt_has_required_fields() {
        let info = AirPlayServiceInfo::new(
            "Test Speaker",
            7000,
            &[0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF],
            false,
            "abcd1234",
            "my-uuid-here",
            false,
            false,
        );

        let keys: Vec<&str> = info.airplay_txt.iter().map(|(k, _)| k.as_str()).collect();
        let find = |key: &str| info.airplay_txt.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str());

        // Sonos SDK receiver key set; no Apple-only `btaddr`, `osvers`, `vv`.
        assert_eq!(
            keys,
            [
                "acl",
                "deviceid",
                "features",
                "rsf",
                "flags",
                "gid",
                "igl",
                "gcgl",
                "model",
                "protovers",
                "pi",
                "pk",
                "srcvers",
                "fv"
            ]
        );
        assert_eq!(find("deviceid"), Some("aa:bb:cc:dd:ee:ff"));
        assert_eq!(find("rsf"), Some("0x0"));
        assert_eq!(find("gid"), find("pi"));
        assert_eq!(find("model"), Some("PlayOnAir1,1"));
        assert_eq!(find("protovers"), Some("1.1"));
    }

    #[test]
    fn encode_txt_length_prefixes_entries() {
        let txt = vec![("a".to_string(), "1".to_string()), ("bc".to_string(), String::new())];
        assert_eq!(encode_txt(&txt), b"\x03a=1\x03bc=");
    }

    #[test]
    fn ap2_raop_name_format() {
        let info = AirPlayServiceInfo::new(
            "My Speaker",
            5000,
            &[0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC],
            false,
            "pk",
            "pi",
            false,
            false,
        );
        assert_eq!(info.raop_name, "123456789ABC@My Speaker");
        assert_eq!(info.airplay_name, "My Speaker");
    }

    #[test]
    #[cfg(not(feature = "video"))]
    fn ap2_pin_pairing_txt_changes_features_and_flags() {
        let info = AirPlayServiceInfo::new(
            "Test Speaker",
            7000,
            &[0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF],
            false,
            "abcd1234",
            "my-uuid-here",
            true,
            false,
        );
        let raop = |key: &str| info.raop_txt.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str());
        let airplay = |key: &str| info.airplay_txt.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str());

        assert_eq!(raop("ft"), Some("0x405FCA00,0x14340"));
        assert_eq!(airplay("features"), Some("0x405FCA00,0x14340"));
        assert_eq!(raop("sf"), Some("0x204"));
        assert_eq!(airplay("flags"), Some("0x204"));
    }

    #[test]
    #[cfg(feature = "video")]
    fn ap2_pin_pairing_txt_changes_flags_with_video_features() {
        let info = AirPlayServiceInfo::new(
            "Test Speaker",
            7000,
            &[0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF],
            false,
            "abcd1234",
            "my-uuid-here",
            true,
            false,
        );
        let raop = |key: &str| info.raop_txt.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str());
        let airplay = |key: &str| info.airplay_txt.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str());

        assert_eq!(raop("ft"), Some("0x527FFEE6,0x0"));
        assert_eq!(airplay("features"), Some("0x527FFEE6,0x0"));
        assert_eq!(raop("sf"), Some("0x204"));
        assert_eq!(airplay("flags"), Some("0x204"));
    }
}
