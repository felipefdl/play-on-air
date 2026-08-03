//! Utility functions — hardware address formatting, hex encoding, wall and mono time.

use std::fmt::Write;
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Process-local epoch for [`mono_now_ns`]. Fixed at first call.
static MONO_EPOCH: OnceLock<Instant> = OnceLock::new();

/// Current wall-clock time in nanoseconds since the UNIX epoch.
///
/// Saturates to 0 if the clock is before the epoch. Use only where absolute
/// wall/PTP time is required (e.g. protocol timestamps). Playout scheduling
/// must use [`mono_now_ns`] so NTP steps cannot jump anchors.
#[cfg(feature = "ap2")]
pub fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Monotonic nanoseconds since an arbitrary process-local epoch.
///
/// Safe for playout anchors and delivery scheduling: does not jump when the
/// wall clock steps (NTP, manual set). Not comparable across processes.
#[cfg(feature = "ap2")]
pub fn mono_now_ns() -> u64 {
    let epoch = MONO_EPOCH.get_or_init(Instant::now);
    epoch.elapsed().as_nanos() as u64
}

/// Format a hardware address for RAOP service name: "AABBCCDDEEFF" (uppercase hex, no separators).
/// Equivalent to utils_hwaddr_raop.
pub fn hwaddr_raop(hwaddr: &[u8]) -> String {
    let mut s = String::with_capacity(hwaddr.len() * 2);
    for &b in hwaddr {
        write!(s, "{b:02X}").unwrap();
    }
    s
}

/// Format a hardware address for AirPlay device ID: "aa:bb:cc:dd:ee:ff" (lowercase hex, colon-separated).
/// Equivalent to utils_hwaddr_airplay.
pub fn hwaddr_airplay(hwaddr: &[u8]) -> String {
    let mut s = String::with_capacity(hwaddr.len() * 3);
    for (i, &b) in hwaddr.iter().enumerate() {
        if i > 0 {
            s.push(':');
        }
        write!(s, "{b:02x}").unwrap();
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hwaddr_raop_c_vector() {
        assert_eq!(hwaddr_raop(&[0x48, 0x5d, 0x60, 0x7c, 0xee, 0x22]), "485D607CEE22");
    }

    #[test]
    fn hwaddr_airplay_c_vector() {
        assert_eq!(
            hwaddr_airplay(&[0x48, 0x5d, 0x60, 0x7c, 0xee, 0x22]),
            "48:5d:60:7c:ee:22"
        );
    }

    #[cfg(feature = "ap2")]
    #[test]
    fn mono_now_ns_is_monotonic() {
        let mut prev = mono_now_ns();
        for _ in 0..1000 {
            let now = mono_now_ns();
            assert!(now >= prev, "mono_now_ns went backwards: {now} < {prev}");
            prev = now;
        }
    }
}
