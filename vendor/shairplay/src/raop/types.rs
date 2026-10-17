//! Public types and traits for the AirPlay 2 server.

/// Audio codec type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioCodec {
    /// Decoded PCM (f32 interleaved). Always delivered regardless of the stream type.
    Pcm,
}

/// Audio format descriptor passed to [`AudioHandler::audio_init`].
#[derive(Debug, Clone, Copy)]
pub struct AudioFormat {
    /// Audio codec (always PCM for decoded output).
    pub codec: AudioCodec,
    /// Bits per sample (always 32 — samples are delivered as `&[f32]`).
    pub bits: u8,
    /// Number of channels.
    pub channels: u8,
    /// Sample rate in Hz.
    pub sample_rate: u32,
}

/// Trait for receiving AirPlay events and creating audio sessions.
///
/// `AudioHandler` is `Send + Sync` — all callbacks can be called from any thread
/// without blocking audio delivery. Metadata, volume, and artwork callbacks are
/// called directly from the RTSP handler thread, never from the audio path.
///
/// A new [`AudioSession`] is created for each audio stream via
/// [`audio_init`](AudioHandler::audio_init). The session only receives PCM samples.
pub trait AudioHandler: Send + Sync + 'static {
    /// Called when a new audio stream starts. Return a session to receive PCM data.
    fn audio_init(&self, format: AudioFormat) -> Box<dyn AudioSession>;

    // --- Metadata events (called from RTSP thread, never blocks audio) ---

    /// Volume change in dB (0.0 = max, -144.0 = mute).
    fn on_volume(&self, _volume: f32) {}
    /// Track metadata (parsed from DMAP).
    fn on_metadata(&self, _metadata: &crate::proto::dmap::TrackMetadata) {}
    /// Album artwork (JPEG or PNG).
    fn on_coverart(&self, _coverart: &[u8]) {}
    /// Playback progress (start, current, end in RTP timestamps at 44100 Hz).
    fn on_progress(&self, _start: u32, _current: u32, _end: u32) {}

    // --- Connection lifecycle ---

    /// Called when a client connects.
    fn on_client_connected(&self, _addr: &str) {}
    /// Called when a client disconnects.
    fn on_client_disconnected(&self, _addr: &str) {}
    /// Called when the library hits a runtime error on a connection — e.g. a
    /// pairing/pair-verify failure, a FairPlay or session-key decrypt failure, a
    /// rejected stream format, or an audio-decoder init failure. Fired at most
    /// once per failure (never per audio packet). Default: log at warn level.
    fn on_error(&self, error: &crate::error::ShairplayError) {
        tracing::warn!(%error, "AirPlay error");
    }
}

/// Storage for paired device keys. Implement this to persist pairing across restarts.
///
/// Without persistence, iPhones that previously paired will send encrypted data
/// on connect and fail because the server has no cached keys.
pub trait PairingStore: Send + Sync + 'static {
    /// Look up a paired device's Ed25519 public key by device ID.
    fn get(&self, device_id: &str) -> Option<[u8; 32]>;
    /// Save a paired device's Ed25519 public key.
    fn put(&self, device_id: &str, public_key: [u8; 32]);
    /// Remove a paired device.
    fn remove(&self, device_id: &str);

    /// Returns `true` once at least one controller is paired.
    ///
    /// Used to advertise `OneTimePairingRequired` (statusFlags bit 9) only until
    /// the first successful pairing, so already-paired controllers reconnect via
    /// pair-verify instead of being nudged back into setup. The default returns
    /// `false` (always advertise setup-required when a PIN is configured); the
    /// built-in [`MemoryPairingStore`] overrides it, and persistent stores should
    /// too.
    fn has_any_pairing(&self) -> bool {
        false
    }

    /// Load the accessory's persistent Ed25519 **identity** seed, if one was saved.
    ///
    /// This is the server's *own* long-term secret (distinct from the paired peer
    /// keys handled by [`get`](Self::get)/[`put`](Self::put)). Returning `Some`
    /// keeps the accessory's identity — and therefore its advertised `pk` — stable
    /// across restarts so already-paired devices don't need to re-pair.
    ///
    /// The default returns `None`: the server then generates a fresh random
    /// identity on each start and offers it to [`save_identity`](Self::save_identity).
    /// Implement both methods to persist the identity. (To reproduce the legacy
    /// insecure behaviour, return the zero-padded device id as the seed.)
    fn load_identity(&self) -> Option<[u8; 32]> {
        None
    }

    /// Persist the accessory's Ed25519 identity seed generated at startup.
    ///
    /// The default is a no-op (identity is not persisted). Implement together with
    /// [`load_identity`](Self::load_identity) to keep a stable identity.
    fn save_identity(&self, _seed: [u8; 32]) {}
}

/// In-memory pairing store (lost on restart). Use for testing or wrap with file I/O.
#[derive(Default)]
pub struct MemoryPairingStore {
    keys: std::sync::Mutex<std::collections::HashMap<String, [u8; 32]>>,
}

impl PairingStore for MemoryPairingStore {
    fn get(&self, device_id: &str) -> Option<[u8; 32]> {
        self.keys.lock().ok()?.get(device_id).copied()
    }
    fn put(&self, device_id: &str, public_key: [u8; 32]) {
        if let Ok(mut keys) = self.keys.lock() {
            keys.insert(device_id.to_string(), public_key);
        }
    }
    fn has_any_pairing(&self) -> bool {
        self.keys.lock().map(|k| !k.is_empty()).unwrap_or(false)
    }
    fn remove(&self, device_id: &str) {
        if let Ok(mut keys) = self.keys.lock() {
            keys.remove(device_id);
        }
    }
}

/// Per-connection audio session — hot path only.
///
/// Created by [`AudioHandler::audio_init`]. Dropped when the client disconnects.
/// Only receives decoded PCM samples and flush events. All metadata, volume,
/// and artwork events go to [`AudioHandler`] instead.
pub trait AudioSession: Send + Sync {
    /// Receive decoded f32 interleaved PCM audio samples.
    fn audio_process(&mut self, samples: &[f32]);
    /// Flush the audio buffer (RTSP `FLUSH`). Only the legacy RTP audio of
    /// `video`-feature screen mirroring calls this; AirPlay 2 audio uses
    /// [`on_flush`](Self::on_flush).
    fn audio_flush(&mut self) {}
    /// AP2 buffered playout rate change (`rate == 0` = pause, nonzero = play).
    ///
    /// Default is a no-op. Hosts that bridge to a sink should map this to pause/resume.
    fn on_rate(&mut self, _rate: u32) {}
    /// AP2 buffered `FLUSHBUFFERED` / playout flush. Default is a no-op.
    ///
    /// Distinct from [`audio_flush`](Self::audio_flush) so the buffered delivery
    /// thread can notify without sharing the legacy RTP session object.
    fn on_flush(&mut self) {}
}
