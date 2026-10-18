//! AirPlay 2 server builder and lifecycle.

use super::connection::RaopShared;
use super::remote::MediaRemoteCommand;
use super::types::*;
use crate::error::{ServerError, ShairplayError};
use crate::net::mdns::{AirPlayServiceInfo, MdnsService};
use crate::net::server::{BindConfig, HttpServer};
use std::sync::Arc;

fn random_hwaddr() -> Vec<u8> {
    use rand::RngCore;

    let mut hwaddr = [0u8; super::MAX_HWADDR_LEN];
    rand::thread_rng().fill_bytes(&mut hwaddr);
    // Locally administered, unicast MAC address.
    hwaddr[0] = (hwaddr[0] | 0x02) & !0x01;
    hwaddr.to_vec()
}

fn derive_pi_from_hwaddr(hwaddr: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(hwaddr);
    let hash = hasher.finalize();
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        hash[0],
        hash[1],
        hash[2],
        hash[3],
        hash[4],
        hash[5],
        (hash[6] & 0x0f) | 0x40, // version 4
        hash[7],
        (hash[8] & 0x3f) | 0x80, // variant 1
        hash[9],
        hash[10],
        hash[11],
        hash[12],
        hash[13],
        hash[14],
        hash[15]
    )
}

/// Default `Audio-Latency` RECORD header value in samples (2 s at 48 kHz).
///
/// AirPlay 2 senders treat this as the receiver buffer depth in
/// stream-rate samples. Sample rate is not always known at server build time
/// (passthrough hosts), so this fixed default is honest for 48 kHz media and
/// still a reasonable ~2 s lead at 44.1 kHz. Hosts that know the rate can set
/// [`RaopServerBuilder::audio_latency_samples`] (e.g. `2 * sample_rate`).
pub const DEFAULT_AUDIO_LATENCY_SAMPLES: u32 = 96_000;

/// Builder for [`RaopServer`].
pub struct RaopServerBuilder {
    max_clients: usize,
    hwaddr: Option<Vec<u8>>,
    password: Option<String>,
    name: String,
    bind: BindConfig,
    pairing_store: Option<Arc<dyn PairingStore>>,
    output_sample_rate: Option<u32>,
    output_max_channels: Option<u8>,
    /// Samples advertised in the RTSP `Audio-Latency` header on RECORD.
    audio_latency_samples: u32,
    /// Buffered-audio release lead (`None` = the advertised `Audio-Latency`).
    release_lead: Option<std::time::Duration>,
    pin: Option<String>,
    /// Register `_raop` / `_airplay` mDNS services on start.
    advertise: bool,
    /// UDP `(event, general)` ports of the PTP follower.
    ptp_ports: (u16, u16),
    #[cfg(feature = "video")]
    video_handler: Option<Arc<dyn crate::raop::video::VideoHandler>>,
    #[cfg(feature = "hls")]
    hls_handler: Option<Arc<dyn crate::raop::hls::HlsHandler>>,
}

impl Default for RaopServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RaopServerBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            max_clients: 10,
            hwaddr: None,
            password: None,
            name: "Shairplay".to_string(),
            bind: BindConfig::default(),
            pairing_store: None,
            output_sample_rate: None,
            output_max_channels: None,
            audio_latency_samples: DEFAULT_AUDIO_LATENCY_SAMPLES,
            release_lead: None,
            pin: None,
            advertise: true,
            ptp_ports: (
                crate::net::ptp::DEFAULT_PTP_EVENT_PORT,
                crate::net::ptp::DEFAULT_PTP_GENERAL_PORT,
            ),
            #[cfg(feature = "video")]
            video_handler: None,
            #[cfg(feature = "hls")]
            hls_handler: None,
        }
    }

    /// Set the maximum number of concurrent connections. Default: 10.
    pub fn max_clients(mut self, n: usize) -> Self {
        self.max_clients = n;
        self
    }
    /// Set the 6-byte hardware address for mDNS registration.
    pub fn hwaddr(mut self, addr: impl Into<Vec<u8>>) -> Self {
        self.hwaddr = Some(addr.into());
        self
    }
    /// Set an optional HTTP Digest authentication password.
    pub fn password(mut self, pw: impl Into<String>) -> Self {
        self.password = Some(pw.into());
        self
    }
    /// Set the RTSP listening port. Default: 5000.
    pub fn port(mut self, port: u16) -> Self {
        self.bind.port = port;
        self
    }
    /// Set full bind configuration (address, port, auto-sensing, IPv6).
    pub fn bind(mut self, config: BindConfig) -> Self {
        self.bind = config;
        self
    }
    /// Set the AirPlay display name. Default: "Shairplay".
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set a pairing store for persisting device keys across restarts.
    /// Without this, iPhones must re-pair on every server restart.
    pub fn pairing_store(mut self, store: Arc<dyn PairingStore>) -> Self {
        self.pairing_store = Some(store);
        self
    }

    /// Set the desired output sample rate. The library resamples to this rate.
    /// Default: source native rate (no resampling).
    pub fn output_sample_rate(mut self, rate: u32) -> Self {
        self.output_sample_rate = Some(rate);
        self
    }

    /// Set the maximum output channels. Sources with more channels are mixed down.
    /// Sources with fewer channels are passed through (no upmixing).
    /// Default: pass through native channel count.
    pub fn output_max_channels(mut self, channels: u8) -> Self {
        self.output_max_channels = Some(channels);
        self
    }

    /// Set the RTSP `Audio-Latency` value returned on RECORD (samples at stream rate).
    ///
    /// Default: [`DEFAULT_AUDIO_LATENCY_SAMPLES`] (2 s at 48 kHz). Prefer
    /// `2 * sample_rate` when the host knows the stream rate at build time.
    pub fn audio_latency_samples(mut self, samples: u32) -> Self {
        self.audio_latency_samples = samples;
        self
    }

    /// How long before its presentation time each buffered (type 103) sample is handed to the
    /// host.
    ///
    /// Default: the advertised `Audio-Latency`. A host whose output buffers ahead of real time
    /// (PlayOnAir's Cast speakers) sets a longer lead so audio queued by the sender reaches it
    /// early enough to be burst on a fresh start. The sender still sees `Audio-Latency`.
    pub fn release_lead(mut self, lead: std::time::Duration) -> Self {
        self.release_lead = Some(lead);
        self
    }

    /// Require normal HomeKit pair-setup with this one-time PIN.
    ///
    /// Without a PIN, AP2 uses the shairport-sync-style transient pairing
    /// profile. Setting a PIN changes mDNS/GET /info status flags so clients
    /// perform persistent M1-M6 pair-setup and later use pair-verify.
    pub fn pin(mut self, pin: impl Into<String>) -> Self {
        self.pin = Some(pin.into());
        self
    }

    /// Register the `_raop._tcp` / `_airplay._tcp` mDNS services on [`RaopServer::start`].
    ///
    /// Default: `true`. Pass `false` for in-process tests that drive the RTSP
    /// port directly and must not touch the network beyond localhost.
    pub fn advertise(mut self, advertise: bool) -> Self {
        self.advertise = advertise;
        self
    }

    /// UDP ports of the PTP follower that maps buffered-audio anchors to local time.
    ///
    /// Default: 319 (event) and 320 (general). Servers in one process configured with the
    /// same fixed ports share one follower. `0` binds an ephemeral port (tests); read it back
    /// with [`RaopServer::ptp_ports`]. If binding fails (another PTP stack owns the ports, or
    /// no privilege for ports below 1024), buffered audio still plays on the unlocked anchor
    /// fallback.
    pub fn ptp_ports(mut self, event: u16, general: u16) -> Self {
        self.ptp_ports = (event, general);
        self
    }

    #[cfg(feature = "video")]
    /// Set a video handler for screen mirroring (experimental).
    pub fn video_handler(mut self, handler: Arc<dyn crate::raop::video::VideoHandler>) -> Self {
        self.video_handler = Some(handler);
        self
    }

    #[cfg(feature = "hls")]
    /// Set an HLS handler for YouTube/video URL playback.
    pub fn hls_handler(mut self, handler: Arc<dyn crate::raop::hls::HlsHandler>) -> Self {
        self.hls_handler = Some(handler);
        self
    }

    /// Build the server with the given audio handler.
    pub fn build(self, handler: Arc<dyn AudioHandler>) -> Result<RaopServer, ShairplayError> {
        if self.max_clients == 0 {
            return Err(ServerError::MaxClients(0).into());
        }
        if let Some(password) = self.password.as_ref()
            && password.len() > super::MAX_PASSWORD_LEN
        {
            return Err(ServerError::InvalidPassword(password.len()).into());
        }
        let hwaddr = match self.hwaddr {
            Some(addr) if addr.len() == super::MAX_HWADDR_LEN => addr,
            Some(addr) => return Err(ServerError::InvalidHwAddr(addr.len()).into()),
            None => random_hwaddr(),
        };

        let pairing_id = derive_pi_from_hwaddr(&hwaddr);
        let device_id = crate::util::hwaddr_airplay(&hwaddr);
        let airplay_name = self.name.clone();

        let pairing_store: Arc<dyn PairingStore> = self
            .pairing_store
            .unwrap_or_else(|| Arc::new(MemoryPairingStore::default()));
        // Resolve the accessory's long-term identity once: reuse a persisted seed
        // if the store has one, otherwise generate a random seed and hand it back
        // for persistence. (A store with no identity persistence — e.g. the default
        // in-memory one — yields a fresh identity each start; persist it via
        // `PairingStore::load_identity`/`save_identity` to avoid re-pairing.)
        let identity_seed = pairing_store.load_identity().unwrap_or_else(|| {
            let seed = crate::crypto::pairing_homekit::generate_identity_seed();
            pairing_store.save_identity(seed);
            seed
        });

        let shared = Arc::new(RaopShared {
            hwaddr: hwaddr.clone(),
            password: self.password.unwrap_or_default(),
            handler,
            pairing_store,
            identity_seed,
            output_sample_rate: self.output_sample_rate,
            output_max_channels: self.output_max_channels,
            audio_latency_samples: self.audio_latency_samples,
            release_lead: self.release_lead,
            pin: self.pin,
            #[cfg(feature = "video")]
            video_handler: self.video_handler,
            #[cfg(feature = "video")]
            video_ekey: Arc::new(std::sync::RwLock::new(None)),
            #[cfg(feature = "video")]
            video_eiv: Arc::new(std::sync::RwLock::new(None)),
            pairing_id,
            device_id,
            airplay_name,
            active_audio: std::sync::Mutex::new(None),
            session_tasks: std::sync::Mutex::new(Vec::new()),
            next_connection_id: std::sync::atomic::AtomicU64::new(1),
            #[cfg(feature = "hls")]
            hls_handler: self.hls_handler,
            reported_volume_db: std::sync::Mutex::new(0.0),
            event_senders: std::sync::Mutex::new(Vec::new()),
            ptp: std::sync::Mutex::new(None),
        });

        let mut httpd = HttpServer::new(shared.clone(), self.max_clients);
        httpd.set_bind_config(self.bind.clone());

        Ok(RaopServer {
            shared,
            httpd,
            mdns: None,
            bind: self.bind,
            name: self.name,
            hwaddr,
            advertise: self.advertise,
            ptp_ports: self.ptp_ports,
        })
    }
}

/// Clamp an AirPlay volume to the wire range (`-144.0..=0.0`).
fn clamp_airplay_volume_db(volume_db: f32) -> f32 {
    if !volume_db.is_finite() {
        return 0.0;
    }
    volume_db.clamp(super::connection::AIRPLAY_VOLUME_DB_MIN, 0.0)
}

/// The main AirPlay/RAOP server.
///
/// Listens for RTSP connections, handles pairing and encryption,
/// decodes audio, and delivers f32 PCM samples via [`AudioSession`].
/// Automatically registers mDNS services for network discovery.
pub struct RaopServer {
    shared: Arc<RaopShared>,
    httpd: HttpServer,
    mdns: Option<MdnsService>,
    bind: BindConfig,
    name: String,
    hwaddr: Vec<u8>,
    /// Register mDNS services on start (see [`RaopServerBuilder::advertise`]).
    advertise: bool,
    /// Configured PTP follower ports (see [`RaopServerBuilder::ptp_ports`]).
    ptp_ports: (u16, u16),
}

impl RaopServer {
    /// Create a new server builder.
    pub fn builder() -> RaopServerBuilder {
        RaopServerBuilder::new()
    }

    /// Volume reported by `GET_PARAMETER volume` (AirPlay dB: `0.0` = max, `-144.0` = mute).
    ///
    /// Hosts should set this from the physical sink so clients show the real device level
    /// instead of always advertising max.
    pub fn set_reported_volume_db(&self, volume_db: f32) {
        let clamped = clamp_airplay_volume_db(volume_db);
        if let Ok(mut guard) = self.shared.reported_volume_db.lock() {
            *guard = clamped;
        }
    }

    /// Current reported AirPlay volume in dB (`0.0` if the lock is poisoned).
    pub fn reported_volume_db(&self) -> f32 {
        self.shared.reported_volume_db.lock().map(|guard| *guard).unwrap_or(0.0)
    }

    /// Ask the connected sender to perform a playback command (`play`, `paus`, …).
    ///
    /// Sent as `POST /command` `sendMediaRemoteCommand` on the event channel of the
    /// connection that owns active audio (else the newest sender). For commands where
    /// [`MediaRemoteCommand::pauses_locally`] is true, the host should pause its own output
    /// before calling this, as Apple's receiver does.
    pub fn send_media_remote_command(&self, command: MediaRemoteCommand) -> Result<(), ShairplayError> {
        Ok(self.shared.send_media_remote_command(command)?)
    }

    /// Report a local volume change (iOS slider position `0..=1`, plus mute) to the sender.
    ///
    /// Also sets the `GET_PARAMETER volume` value (slider mapped linearly onto `-30..=0` dB,
    /// `-144` when muted). Sends `dvlc` only when the value differs from the last reported
    /// or sender-set one, so a sender's own volume change is never echoed back. Senders
    /// older than 354.54.6 expect DACP and get [`RemoteError::SenderTooOld`](crate::error::RemoteError::SenderTooOld).
    pub fn report_volume(&self, slider: f32, muted: bool) -> Result<(), ShairplayError> {
        Ok(self.shared.report_volume(slider, muted)?)
    }

    /// Bound PTP follower `(event, general)` ports while running; `None` before
    /// [`Self::start`], after [`Self::stop`], or when binding failed.
    pub fn ptp_ports(&self) -> Option<(u16, u16)> {
        self.shared
            .ptp
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|service| service.ports())
    }

    /// Start the server: bind ports, register mDNS services (unless disabled with
    /// [`RaopServerBuilder::advertise`]), begin accepting connections.
    pub async fn start(&mut self) -> Result<(), ShairplayError> {
        let _actual_port = self.httpd.start(self.bind.port).await?;

        // PTP follower: maps SETRATEANCHORTIME network times to local time. It also keeps the
        // sender from seeing ICMP port-unreachable for its PTP traffic, which stalls the
        // buffered-audio start. Bind failure leaves buffered audio on the unlocked fallback.
        let (event, general) = self.ptp_ports;
        let service = crate::net::ptp::acquire(event, general);
        *self
            .shared
            .ptp
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(service);

        if self.advertise {
            let info = self.service_info();
            let mut mdns = MdnsService::new()?;
            mdns.register_raop(&info)?;
            mdns.register_airplay(&info)?;
            self.mdns = Some(mdns);
        }

        Ok(())
    }

    /// Whether the server is currently running.
    pub fn is_running(&self) -> bool {
        self.httpd.is_running()
    }

    /// Stop the server: kill AP2 session tasks, abort RTSP connections, drop mDNS.
    ///
    /// PlayOnAir needs this after Cast steal so iOS leaves Now Playing. A plain
    /// accept-loop stop leaves accepted RTSP/event/audio TCP streams open.
    pub async fn stop(&mut self) {
        self.shared.hard_stop_sessions();
        // Release this server's PTP follower handle; the sockets close with the last handle.
        let service = self
            .shared
            .ptp
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(service);
        if let Some(mut mdns) = self.mdns.take() {
            mdns.unregister_raop();
            mdns.unregister_airplay();
        }
        self.httpd.stop().await;
    }

    /// Get the mDNS service info for this server.
    pub fn service_info(&self) -> AirPlayServiceInfo {
        let (_, vk) = crate::crypto::pairing_homekit::identity_keypair(&self.shared.identity_seed);
        let pk_hex: String = vk.as_bytes().iter().map(|b| format!("{b:02x}")).collect();
        AirPlayServiceInfo::new(
            &self.name,
            self.httpd.port(),
            &self.hwaddr,
            !self.shared.password.is_empty(),
            &pk_hex,
            &self.shared.pairing_id,
            self.shared.pin.is_some(),
            self.shared.pairing_store.has_any_pairing(),
        )
    }
}
