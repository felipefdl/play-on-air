//! Protocol implementations (HTTP/RTSP, DMAP metadata, HTTP Digest auth).
//!
//! Binary plist parsing uses the `plist` crate directly at the call sites
//! (see [`crate::raop`]); there is no in-crate plist wrapper.

pub mod digest;
pub mod dmap;
pub mod http;
