//! Networking layer — TCP server, mDNS discovery, PTP timing, feature flags.

pub mod features;
pub mod mdns;
pub(crate) mod ptp;
pub(crate) mod server;
