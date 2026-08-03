//! AirPlay 2 buffered audio processor (stream type 103).
//!
//! Receives encrypted AAC packets over TCP, decrypts with ChaCha20-Poly1305,
//! decodes via symphonia, resamples/mixes down, and delivers F32LE PCM through
//! a timed playout buffer.
//!
//! Three concurrent tasks:
//! - **Receiver** (tokio): accepts TCP, decrypts, decodes, buffers by RTP timestamp
//! - **Command handler** (tokio): processes SetRate/Flush/Stop from RTSP thread
//! - **Delivery** (std::thread): timed playout using anchor-based scheduling

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use crate::codec::aac::{AacDecoder, AudioSsrc};
use crate::error::{CodecError, NetworkError, ShairplayError};
use crate::raop::audio_pipeline::{NONCE_TRAIL_LEN, RTP_HEADER_LEN, decrypt_rtp_chacha};
use crate::raop::{AudioCodec, AudioFormat, AudioHandler};
use crate::util::now_ns;

#[derive(Debug, Clone)]
/// Output configuration passed from the server builder.
pub(crate) struct OutputConfig {
    /// Target sample rate, or None for source native rate.
    pub(crate) sample_rate: Option<u32>,
    /// Maximum output channels, or None to pass through.
    pub(crate) max_channels: Option<u8>,
}

#[derive(Debug)]
/// Commands sent from the RTSP handler thread to the playout engine.
pub enum PlayoutCommand {
    /// Set playback rate and anchor point. rate=0 means pause.
    SetRate {
        /// RTP timestamp at the anchor point.
        anchor_rtp: u32,
        /// Network time at the anchor point (ns).
        anchor_time_ns: u64,
        /// Playback rate (1 = playing, 0 = paused).
        rate: u32,
    },
    /// Flush buffered frames in the given RTP timestamp range.
    Flush {
        /// First timestamp to flush.
        from_seq: u32,
        /// Last timestamp to flush.
        until_seq: u32,
    },
    /// Stop playback and tear down.
    Stop,
}

struct PlayoutState {
    buffer: BTreeMap<u32, Vec<f32>>, // rtp_timestamp → F32 PCM samples
    anchor_rtp: u32,
    anchor_local_ns: u64,
    rate: u32,
    sample_rate: u32,
    channels: u8,
    stopped: bool,
    format_changed: bool,
    /// Set by the command task; drained by `delivery_loop` onto the live session.
    pending_rate: Option<u32>,
    /// Set by the command task on FLUSHBUFFERED; drained by `delivery_loop`.
    pending_flush: bool,
}

/// TCP listener for buffered audio. Binds a port and spawns the processing pipeline.
pub(crate) struct BufferedAudioProcessor {
    /// TCP listener waiting for the iPhone to connect.
    pub(crate) listener: TcpListener,
}

impl BufferedAudioProcessor {
    /// Start the processing pipeline.
    ///
    /// Returns the command sender and abort handles for the async command/receive tasks
    /// (so the server can force-kill sockets on hard stop).
    pub(crate) fn start(
        self,
        shk: [u8; 32],
        output_config: OutputConfig,
        handler: Arc<dyn AudioHandler>,
    ) -> (
        tokio::sync::mpsc::UnboundedSender<PlayoutCommand>,
        Vec<tokio::task::AbortHandle>,
    ) {
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let default_sr = output_config.sample_rate.unwrap_or(44100);

        let state = Arc::new((
            Mutex::new(PlayoutState {
                buffer: BTreeMap::new(),
                anchor_rtp: 0,
                anchor_local_ns: 0,
                rate: 0,
                sample_rate: default_sr,
                channels: 2,
                stopped: false,
                format_changed: false,
                pending_rate: None,
                pending_flush: false,
            }),
            Condvar::new(),
        ));

        // Delivery thread
        let state2 = state.clone();
        let handler2 = handler.clone();
        let output_config2 = output_config.clone();
        std::thread::spawn(move || {
            delivery_loop(state2, handler2, output_config2);
        });

        // Command handler
        let state3 = state.clone();
        let mut cmd_rx = cmd_rx;
        let cmd_handle = tokio::spawn(async move {
            while let Some(cmd) = cmd_rx.recv().await {
                let (lock, cvar) = &*state3;
                let mut s = lock.lock().unwrap_or_else(PoisonError::into_inner);
                match cmd {
                    PlayoutCommand::SetRate {
                        anchor_rtp,
                        anchor_time_ns: _,
                        rate,
                    } => {
                        s.anchor_rtp = anchor_rtp;
                        let was_paused = s.rate == 0;
                        s.rate = rate;
                        // Delivery thread owns the AudioSession; queue for on_rate.
                        s.pending_rate = Some(rate);
                        if rate == 0 {
                            info!("Playout paused");
                        } else {
                            // Set anchor so the earliest buffered frame is deliverable
                            // with a small lead time for smooth playback
                            if let Some(&first_ts) = s.buffer.keys().next() {
                                let lead_frames = s.sample_rate / 10; // 100ms lead
                                s.anchor_rtp = first_ts.wrapping_sub(lead_frames);
                            }
                            s.anchor_local_ns = now_ns();
                            let stale: Vec<u32> = s
                                .buffer
                                .keys()
                                .filter(|&&ts| (s.anchor_rtp.wrapping_sub(ts) as i32) > 0)
                                .copied()
                                .collect();
                            if !stale.is_empty() {
                                debug!(discarded = stale.len(), "Discarded stale frames");
                            }
                            for k in stale {
                                s.buffer.remove(&k);
                            }
                            if was_paused {
                                info!(anchor_rtp, "Playout started");
                            }
                        }
                        cvar.notify_all();
                    }
                    PlayoutCommand::Flush { from_seq, until_seq } => {
                        let keys: Vec<u32> = s
                            .buffer
                            .keys()
                            .filter(|&&ts| ts >= from_seq && ts <= until_seq)
                            .copied()
                            .collect();
                        for k in &keys {
                            s.buffer.remove(k);
                        }
                        s.pending_flush = true;
                        debug!(flushed = keys.len(), "Flushed");
                        cvar.notify_all();
                    }
                    PlayoutCommand::Stop => {
                        s.stopped = true;
                        s.buffer.clear();
                        cvar.notify_all();
                        break;
                    }
                }
            }
        });

        // Receiver task
        let state4 = state.clone();

        let recv_handle = tokio::spawn(async move {
            let (stream, addr) = match self.listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    warn!("Buffered audio accept failed: {e}");
                    handler.on_error(&ShairplayError::Network(NetworkError::Io(e)));
                    return;
                }
            };
            info!(%addr, "Buffered audio client connected");
            receive_loop(stream, &shk, output_config, state4, &handler).await;
        });

        (cmd_tx, vec![cmd_handle.abort_handle(), recv_handle.abort_handle()])
    }
}

/// TCP receive loop: reads length-prefixed packets, decrypts, decodes, buffers.
async fn receive_loop(
    mut stream: TcpStream,
    shk: &[u8; 32],
    output_config: OutputConfig,
    state: Arc<(Mutex<PlayoutState>, Condvar)>,
    handler: &Arc<dyn AudioHandler>,
) {
    use chacha20poly1305::{ChaCha20Poly1305, KeyInit};

    let cipher = ChaCha20Poly1305::new(shk.into());
    let mut len_buf = [0u8; 2];
    let mut decoder: Option<AacDecoder> = None;
    let mut current_ssrc = AudioSsrc::None;
    let mut stream_resampler: Option<crate::codec::resample::StreamResampler> = None;
    let mut source_channels: u8 = 2;
    let mut output_channels: u8 = 2;

    loop {
        if stream.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        let total_len = u16::from_be_bytes(len_buf) as usize;
        if total_len < 2 {
            break;
        }

        let mut packet = vec![0u8; total_len - 2];
        if stream.read_exact(&mut packet).await.is_err() {
            break;
        }
        if packet.len() <= RTP_HEADER_LEN + NONCE_TRAIL_LEN {
            continue;
        }

        let timestamp = u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]]);
        let ssrc_val = u32::from_be_bytes([packet[8], packet[9], packet[10], packet[11]]);
        let ssrc = AudioSsrc::from_u32(ssrc_val);

        // Detect format change
        if ssrc != AudioSsrc::None && ssrc != current_ssrc {
            current_ssrc = ssrc;
            let src_sr = ssrc.sample_rate();
            let src_ch = ssrc.channels();
            info!(ssrc = ?ssrc, src_sr, src_ch, "Audio format detected");

            decoder = AacDecoder::new(src_sr, src_ch).ok();
            if decoder.is_none() {
                warn!("Failed to create AAC decoder for {:?}", ssrc);
                handler.on_error(&ShairplayError::Codec(CodecError::UnsupportedFormat(format!(
                    "AAC decoder init failed (ssrc={ssrc:?}, sample_rate={src_sr}, channels={src_ch})"
                ))));
            }

            let target_sr = output_config.sample_rate.unwrap_or(src_sr);
            let target_ch = output_config.max_channels.map(|max| src_ch.min(max)).unwrap_or(src_ch);

            stream_resampler = crate::codec::resample::StreamResampler::new(src_sr, target_sr, target_ch as usize);
            if stream_resampler.is_some() {
                debug!(from = src_sr, to = target_sr, "Resampler initialized");
            }

            source_channels = src_ch;
            output_channels = target_ch;

            // Signal format change to delivery thread
            let (lock, cvar) = &*state;
            let mut s = lock.lock().unwrap();
            s.sample_rate = target_sr;
            s.channels = target_ch;
            s.format_changed = true;
            cvar.notify_all();
        }

        // Decrypt the ChaCha20-Poly1305 RTP frame.
        let Some(plaintext) = decrypt_rtp_chacha(&cipher, &packet) else {
            debug!("Audio decrypt failed");
            continue;
        };

        // Decode
        let pcm = if let Some(dec) = &mut decoder {
            dec.decode(&plaintext)
        } else {
            None
        };

        if let Some(pcm_data) = pcm {
            // Convert bytes to f32 samples for processing
            let samples: Vec<f32> = pcm_data
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();

            // Mix down + resample to the output format.
            let samples = crate::codec::resample::mixdown_and_resample(
                samples,
                source_channels,
                output_channels,
                &mut stream_resampler,
            );

            let (lock, cvar) = &*state;
            let mut s = lock.lock().unwrap();
            s.buffer.insert(timestamp, samples);
            cvar.notify_all();
        }
    }
    debug!("Buffered audio receive loop ended");
    let (lock, cvar) = &*state;
    if let Ok(mut s) = lock.lock() {
        s.stopped = true;
        s.buffer.clear();
        cvar.notify_all();
    }
}

/// Timed playout delivery thread. Wakes on condvar, delivers due frames to AudioSession.
fn delivery_loop(
    state: Arc<(Mutex<PlayoutState>, Condvar)>,
    handler: Arc<dyn AudioHandler>,
    _output_config: OutputConfig,
) {
    let (lock, cvar) = &*state;
    let mut session: Option<Box<dyn crate::raop::AudioSession>> = None;

    loop {
        let mut s = lock.lock().unwrap_or_else(PoisonError::into_inner);

        // Park until stop, host notifications (rate/flush), or playable audio.
        // Must wake on rate==0 with `pending_rate` so `on_rate(0)` runs before re-parking.
        while !s.stopped && s.pending_rate.is_none() && !s.pending_flush && (s.rate == 0 || s.buffer.is_empty()) {
            s = cvar.wait(s).unwrap_or_else(PoisonError::into_inner);
        }
        if s.stopped {
            break;
        }

        // Snapshot under the lock, then release before audio_init / session callbacks.
        // Dropping the old session *before* audio_init avoids Drop(Ended) clearing the
        // ring that audio_init just installed (RHS-of-assignment drop order).
        let need_init = session.is_none() || s.format_changed;
        let init_format = if need_init {
            s.format_changed = false;
            Some(AudioFormat {
                codec: AudioCodec::Pcm,
                bits: 32,
                channels: s.channels,
                sample_rate: s.sample_rate,
            })
        } else {
            None
        };
        let pending_rate = s.pending_rate.take();
        let pending_flush = std::mem::replace(&mut s.pending_flush, false);

        let mut ready: Vec<(u32, Vec<f32>)> = Vec::new();
        if s.rate != 0 && !s.buffer.is_empty() {
            let now = now_ns();
            let elapsed_ns = now.saturating_sub(s.anchor_local_ns);
            let elapsed_frames = (elapsed_ns as u128 * s.sample_rate as u128 / 1_000_000_000) as u32;
            let target_rtp = s.anchor_rtp.wrapping_add(elapsed_frames);

            ready = s
                .buffer
                .iter()
                .filter(|(ts, _)| (target_rtp.wrapping_sub(**ts) as i32) >= 0)
                .map(|(&ts, data)| (ts, data.clone()))
                .collect();

            for (ts, _) in &ready {
                s.buffer.remove(ts);
            }
        }
        drop(s);

        if let Some(format) = init_format {
            let old = session.take();
            drop(old);
            info!(?format, "Audio session initialized");
            session = Some(handler.audio_init(format));
        }

        if let Some(ref mut sess) = session {
            if pending_flush {
                sess.on_flush();
            }
            if let Some(rate) = pending_rate {
                sess.on_rate(rate);
            }
            for (_, frame) in &ready {
                sess.audio_process(frame);
            }
        }

        if ready.is_empty() {
            // Avoid busy-spin while rate > 0 and the next frame is not yet due.
            // When rate == 0 and notifications are drained, the wait above parks us.
            let s = lock.lock().unwrap_or_else(PoisonError::into_inner);
            let should_sleep = s.rate != 0
                && !s.buffer.is_empty()
                && s.pending_rate.is_none()
                && !s.pending_flush
                && !s.format_changed
                && !s.stopped;
            drop(s);
            if should_sleep {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }
    }
    info!("Delivery loop ended");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raop::AudioSession;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Shared counters for a mock session living behind `Box<dyn AudioSession>`.
    #[derive(Default)]
    struct SessionCounters {
        rates: Mutex<Vec<u32>>,
        flushes: AtomicUsize,
        drops: AtomicUsize,
    }

    struct TrackingSession {
        counters: Arc<SessionCounters>,
        /// Global init/drop order log shared with the handler.
        order: Arc<Mutex<Vec<&'static str>>>,
    }

    impl AudioSession for TrackingSession {
        fn audio_process(&mut self, _samples: &[f32]) {}
        fn on_rate(&mut self, rate: u32) {
            self.counters
                .rates
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(rate);
        }
        fn on_flush(&mut self) {
            self.counters.flushes.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Drop for TrackingSession {
        fn drop(&mut self) {
            self.counters.drops.fetch_add(1, Ordering::SeqCst);
            self.order.lock().unwrap_or_else(PoisonError::into_inner).push("drop");
        }
    }

    struct TrackingHandler {
        inits: AtomicUsize,
        order: Arc<Mutex<Vec<&'static str>>>,
        /// Counters for the most recently created session.
        current: Mutex<Option<Arc<SessionCounters>>>,
        /// All sessions ever created (for format-change drop-order checks).
        all: Mutex<Vec<Arc<SessionCounters>>>,
    }

    impl TrackingHandler {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inits: AtomicUsize::new(0),
                order: Arc::new(Mutex::new(Vec::new())),
                current: Mutex::new(None),
                all: Mutex::new(Vec::new()),
            })
        }
    }

    impl AudioHandler for TrackingHandler {
        fn audio_init(&self, _format: AudioFormat) -> Box<dyn AudioSession> {
            self.inits.fetch_add(1, Ordering::SeqCst);
            self.order
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push("audio_init");
            let counters = Arc::new(SessionCounters::default());
            self.all
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(Arc::clone(&counters));
            *self.current.lock().unwrap_or_else(PoisonError::into_inner) = Some(Arc::clone(&counters));
            Box::new(TrackingSession {
                counters,
                order: Arc::clone(&self.order),
            })
        }
    }

    fn fresh_state(sample_rate: u32, channels: u8) -> Arc<(Mutex<PlayoutState>, Condvar)> {
        Arc::new((
            Mutex::new(PlayoutState {
                buffer: BTreeMap::new(),
                anchor_rtp: 0,
                anchor_local_ns: 0,
                rate: 0,
                sample_rate,
                channels,
                stopped: false,
                format_changed: false,
                pending_rate: None,
                pending_flush: false,
            }),
            Condvar::new(),
        ))
    }

    fn spawn_delivery(
        state: Arc<(Mutex<PlayoutState>, Condvar)>,
        handler: Arc<TrackingHandler>,
    ) -> std::thread::JoinHandle<()> {
        let output = OutputConfig {
            sample_rate: Some(44_100),
            max_channels: Some(2),
        };
        std::thread::spawn(move || {
            delivery_loop(state, handler, output);
        })
    }

    fn stop_delivery(state: &Arc<(Mutex<PlayoutState>, Condvar)>) {
        let (lock, cvar) = &**state;
        let mut s = lock.lock().unwrap_or_else(PoisonError::into_inner);
        s.stopped = true;
        cvar.notify_all();
    }

    #[test]
    fn format_change_drops_old_session_before_audio_init() {
        let handler = TrackingHandler::new();
        let state = fresh_state(44_100, 2);
        let join = spawn_delivery(Arc::clone(&state), Arc::clone(&handler));

        // First session.
        {
            let (lock, cvar) = &*state;
            let mut s = lock.lock().unwrap_or_else(PoisonError::into_inner);
            s.format_changed = true;
            s.rate = 1;
            s.buffer.insert(0, vec![0.0, 0.0]);
            s.anchor_local_ns = now_ns();
            cvar.notify_all();
        }
        // Wait until first init.
        for _ in 0..50 {
            if handler.inits.load(Ordering::SeqCst) >= 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(handler.inits.load(Ordering::SeqCst), 1);

        // Format change → second init; old session must Drop before new audio_init.
        {
            let (lock, cvar) = &*state;
            let mut s = lock.lock().unwrap_or_else(PoisonError::into_inner);
            s.channels = 1;
            s.format_changed = true;
            s.buffer.insert(100, vec![0.1]);
            cvar.notify_all();
        }
        for _ in 0..50 {
            if handler.inits.load(Ordering::SeqCst) >= 2 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(handler.inits.load(Ordering::SeqCst), 2);

        stop_delivery(&state);
        join.join().expect("delivery thread");

        let order = handler.order.lock().unwrap_or_else(PoisonError::into_inner).clone();
        // Expect: audio_init, (maybe process), drop, audio_init, drop (on stop).
        let init_idxs: Vec<usize> = order
            .iter()
            .enumerate()
            .filter_map(|(i, e)| (*e == "audio_init").then_some(i))
            .collect();
        assert_eq!(init_idxs.len(), 2, "order={order:?}");
        // Between the two audio_init calls there must be a drop (old session).
        let between = &order[init_idxs[0] + 1..init_idxs[1]];
        assert!(
            between.contains(&"drop"),
            "old session must drop before second audio_init; order={order:?}"
        );
    }

    fn current_counters(handler: &TrackingHandler) -> Option<Arc<SessionCounters>> {
        handler.current.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    fn wait_until(mut pred: impl FnMut() -> bool, label: &str) {
        for _ in 0..100 {
            if pred() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("timeout waiting for {label}");
    }

    #[test]
    fn pending_rate_and_flush_reach_session() {
        let handler = TrackingHandler::new();
        let state = fresh_state(44_100, 2);
        let join = spawn_delivery(Arc::clone(&state), Arc::clone(&handler));

        // Create session via format + playable buffer, then pause/flush/resume via flags.
        {
            let (lock, cvar) = &*state;
            let mut s = lock.lock().unwrap_or_else(PoisonError::into_inner);
            s.format_changed = true;
            s.rate = 1;
            s.buffer.insert(0, vec![0.0, 0.0]);
            s.anchor_local_ns = now_ns();
            cvar.notify_all();
        }
        wait_until(|| handler.inits.load(Ordering::SeqCst) >= 1, "first audio_init");
        assert_eq!(handler.inits.load(Ordering::SeqCst), 1);

        // Drain each pending flag before posting the next so Option<u32> is not overwritten.
        {
            let (lock, cvar) = &*state;
            let mut s = lock.lock().unwrap_or_else(PoisonError::into_inner);
            s.rate = 0;
            s.pending_rate = Some(0);
            cvar.notify_all();
        }
        wait_until(
            || {
                current_counters(&handler)
                    .is_some_and(|c| c.rates.lock().unwrap_or_else(PoisonError::into_inner).contains(&0))
            },
            "on_rate(0)",
        );

        {
            let (lock, cvar) = &*state;
            let mut s = lock.lock().unwrap_or_else(PoisonError::into_inner);
            s.pending_flush = true;
            cvar.notify_all();
        }
        wait_until(
            || current_counters(&handler).is_some_and(|c| c.flushes.load(Ordering::SeqCst) >= 1),
            "on_flush",
        );

        {
            let (lock, cvar) = &*state;
            let mut s = lock.lock().unwrap_or_else(PoisonError::into_inner);
            s.rate = 1;
            s.pending_rate = Some(1);
            s.buffer.insert(200, vec![0.2, 0.2]);
            s.anchor_local_ns = now_ns();
            cvar.notify_all();
        }
        wait_until(
            || {
                current_counters(&handler)
                    .is_some_and(|c| c.rates.lock().unwrap_or_else(PoisonError::into_inner).contains(&1))
            },
            "on_rate(1)",
        );

        let counters = current_counters(&handler).expect("session counters");
        let rates = counters.rates.lock().unwrap_or_else(PoisonError::into_inner).clone();
        assert!(rates.contains(&0), "rates={rates:?}");
        assert!(rates.contains(&1), "rates={rates:?}");
        assert!(counters.flushes.load(Ordering::SeqCst) >= 1);

        stop_delivery(&state);
        join.join().expect("delivery thread");
    }
}
