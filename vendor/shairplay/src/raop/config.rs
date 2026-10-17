// SPDX-License-Identifier: GPL-3.0-only
// Copyright (C) 2026 Fabian Schmieder

//! Single Source of Truth (SSOT) for global AirPlay receiver capabilities and profiles.

/// Global device hardware model advertised to Apple clients.
pub(crate) const GLOBAL_MODEL: &str = "PlayOnAir1,1";

/// RTSP/mDNS protocol version for AirPlay 2.
pub(crate) const AP2_PROTOVERS: &str = "1.1";

/// Software build/source version reported in GET /info and mDNS.
pub(crate) const AP2_SRCVERS: &str = "366.0";

/// Buffered (stream type 103) formats the AAC decoder handles, as Apple audio format
/// indices (bit `n` of a `supportedFormats` mask): 22 = AAC-LC/44100/2, 23 = AAC-LC/48000/2.
///
/// The index is also the top byte of the buffered RTP SSRC (`0x16000000`, `0x17000000`).
/// ALAC (18 to 21) and AAC-ELD (24, 25, 41, 42) are not decoded on this path.
pub(crate) const BUFFERED_AUDIO_FORMATS: [u8; 2] = [22, 23];

/// Realtime (stream type 96) formats the ALAC decoder handles, as Apple audio format
/// indices: 18 to 21 = ALAC 44100/48000 Hz, 16/24-bit, stereo.
pub(crate) const REALTIME_AUDIO_FORMATS: [u8; 4] = [18, 19, 20, 21];

/// `/info` `volumeControlType`: 3 = absolute volume applied by the receiver's own hardware,
/// as the Sonos SDK receiver advertises. The Cast device applies volume here.
pub(crate) const VOLUME_CONTROL_TYPE: i64 = 3;

/// Bitmask with bit `n` set for each audio format index `n` (the `supportedFormats` encoding).
pub(crate) fn audio_format_mask(formats: &[u8]) -> u64 {
    formats.iter().fold(0, |mask, &index| mask | (1u64 << index))
}

/// AP2 status flag (bit 2): audio output is available. Apple's receiver always sets it.
pub(crate) const AP2_STATUS_AUDIO_ATTACHED: u32 = 1 << 2;

/// AP2 status flag (bit 7): a password is required. Apple sets bit 3 (PIN) or bit 7
/// (password), never both; this receiver signals its PIN with bit 9 instead of bit 3.
pub(crate) const AP2_STATUS_PASSWORD_REQUIRED: u32 = 1 << 7;

/// AP2 status flag (bit 9): one-time HomeKit pairing (a PIN) is required. A PIN
/// accessory advertises this whether paired or not — matches real Apple TVs.
pub(crate) const AP2_STATUS_ONE_TIME_PAIRING_REQUIRED: u32 = 1 << 9;

/// AP2 status flag (bit 10): the accessory has completed HomeKit access-control
/// setup (i.e. it is paired). Advertised alongside bit 9 once paired.
pub(crate) const AP2_STATUS_DEVICE_SETUP_FOR_HK_ACCESS_CONTROL: u32 = 1 << 10;

/// Build AP2 statusFlags (`/info` `statusFlags`, TXT `flags` and `sf`).
///
/// Follows Apple's `sysInfo_getStatusFlags` (spec section 4): bit 2 always, bit 7 for a
/// password, and the per-setting bits this receiver has (9, 10). The remaining Apple bits
/// (6, 8, 11, 17 to 20) come from cloud, relay, and HomeKit settings it does not implement.
///
/// A PIN accessory always keeps bit 9 (`OneTimePairingRequired`) and, once paired,
/// *adds* bit 10 (`DeviceSetupForHKAccessControl`) — `0x204` unpaired, `0x604` paired —
/// the structure real Apple TVs advertise (`0x18644`).
///
/// Do NOT instead clear bit 9 to `0x004`: that is a transient (PIN-less)
/// accessory's flag value, so the sender treats us as transient and attempts a
/// transient pair-setup we must reject (stalling the connection). And bit 10
/// *without* bit 9 (`0x404`) makes the sender attempt transient on every
/// connection. Bit 9 says "I'm a PIN device"; bit 10 says "already set up".
pub(crate) fn ap2_status_flags(password_required: bool, requires_pin_pairing: bool, already_paired: bool) -> u32 {
    let mut flags = AP2_STATUS_AUDIO_ATTACHED;
    if password_required {
        flags |= AP2_STATUS_PASSWORD_REQUIRED;
    }
    if requires_pin_pairing {
        flags |= AP2_STATUS_ONE_TIME_PAIRING_REQUIRED;
        if already_paired {
            flags |= AP2_STATUS_DEVICE_SETUP_FOR_HK_ACCESS_CONTROL;
        }
    }
    flags
}

// --- Screen Mirroring (Video) Display Specifications ---
// Only consumed by the `video` screen-mirroring path in handlers_ap2.

/// Width in pixels advertised for the virtual display target.
#[cfg(feature = "video")]
pub(crate) const MIRRORING_WIDTH: i64 = 1920;

/// Height in pixels advertised for the virtual display target.
#[cfg(feature = "video")]
pub(crate) const MIRRORING_HEIGHT: i64 = 1080;

/// Frame rate advertised for the virtual display target.
#[cfg(feature = "video")]
pub(crate) const MIRRORING_FPS: i64 = 60;

/// Static UUID tag for the virtual display target.
#[cfg(feature = "video")]
pub(crate) const MIRRORING_UUID: &str = "shairplay_display";

/// Display features bitmask advertised for screen mirroring.
#[cfg(feature = "video")]
pub(crate) const MIRRORING_FEATURES: i64 = 2;

#[cfg(test)]
mod status_flag_tests {
    use super::*;

    #[test]
    fn status_flags_match_apple_tv_structure() {
        assert_eq!(ap2_status_flags(false, false, false), 0x004, "transient (PIN-less)");
        assert_eq!(
            ap2_status_flags(false, false, true),
            0x004,
            "transient (paired irrelevant)"
        );
        assert_eq!(ap2_status_flags(false, true, false), 0x204, "PIN, unpaired");
        assert_eq!(
            ap2_status_flags(false, true, true),
            0x604,
            "PIN, paired (bits 9+10, like Apple TV)"
        );
        assert_eq!(ap2_status_flags(true, false, false), 0x084, "password");
    }
}
