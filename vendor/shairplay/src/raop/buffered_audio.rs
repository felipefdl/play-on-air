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
use crate::util::mono_now_ns;

/// Max AAC packets delivered to the host per delivery-loop tick.
///
/// Each map entry is one decoded AAC packet (typically 1024 source samples before
/// resample). Uncapped catch-up after a stall would dump the whole map into the
/// host ring in one burst. 16 packets ≈ 370 ms at 44.1/48 kHz — enough to recover
/// without flooding a multi-second ring in a single write storm. Excess due frames
/// stay in the map for the next tick.
const MAX_FRAMES_PER_TICK: usize = 16;

/// Target playout-map depth (~60 s of source audio).
///
/// Consistent with the AP2 SETUP advertised `audioBufferSize = 0x100000` (1 MB) in
/// `handlers_ap2` — iOS buffered mode is deep-buffer and may burst far ahead of real
/// time. Holding ~60 s of AAC frames keeps the map comfortably above that window.
/// Do **not** shrink the advertised `audioBufferSize`; flow control is read-driven
/// (pause TCP reads when full) rather than dropping frames at the playhead.
const TARGET_BUFFER_DURATION_SECS: u32 = 60;

/// Resume TCP reads once map depth falls below this (hysteresis under target).
const RESUME_BUFFER_DURATION_SECS: u32 = 50;

/// Pathological backstop: refuse **newest** frames if the map exceeds 3× target.
///
/// Should be unreachable with paced reads. Never drops the head (playhead) side.
const BACKSTOP_BUFFER_DURATION_SECS: u32 = 180;

/// On SetRate (rate != 0), purge buffered frames more than this many seconds of source
/// samples behind the wire RTP anchor before the earliest-frame lead nudge.
///
/// Prevents one stray pre-seek packet from dragging the playhead arbitrarily far into
/// the past when the nudge used earliest-in-map.
const ANCHOR_BACKWARD_TOLERANCE_SECS: u32 = 2;

/// Typical AAC frame length in source samples (ADTS / AirPlay buffered path).
const AAC_FRAME_SAMPLES: u32 = 1024;

/// Minimum interval between backstop warning logs (1 s of mono time).
const BACKSTOP_WARN_INTERVAL_NS: u64 = 1_000_000_000;

/// 100 ms of source samples — lead used by SetRate nudge and post-flush re-anchor.
#[inline]
fn reanchor_lead_frames(source_sample_rate: u32) -> u32 {
    source_sample_rate / 10
}

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
    /// Flush buffered frames (FLUSHBUFFERED). Prefer RTP timestamp fields when present.
    Flush {
        /// First packet sequence number (`flushFromSeq`).
        from_seq: u32,
        /// Last packet sequence number (`flushUntilSeq`).
        until_seq: u32,
        /// First RTP timestamp to flush (`flushFromTS`), when the plist carries it.
        from_ts: Option<u32>,
        /// Last RTP timestamp to flush (`flushUntilTS`), when the plist carries it.
        until_ts: Option<u32>,
    },
    /// Stop playback and tear down.
    Stop,
}

struct PlayoutState {
    buffer: BTreeMap<u32, Vec<f32>>, // rtp_timestamp → F32 PCM samples
    anchor_rtp: u32,
    /// Monotonic local time at the anchor (see [`mono_now_ns`]).
    anchor_local_ns: u64,
    rate: u32,
    /// Output sample rate for `AudioFormat` / host ring (may differ after resample).
    sample_rate: u32,
    /// Source RTP clock rate (AAC/ALAC stream). Playout math scales wall time by this.
    source_sample_rate: u32,
    channels: u8,
    stopped: bool,
    format_changed: bool,
    /// Set by the command task; drained by `delivery_loop` onto the live session.
    pending_rate: Option<u32>,
    /// Set by the command task on FLUSHBUFFERED; drained by `delivery_loop`.
    pending_flush: bool,
    /// Armed by playing FLUSHBUFFERED; resolved on first post-flush packet that survives epoch purge.
    ///
    /// Apple may re-key the RTP epoch on track skip. Anchoring immediately to `flushUntilTS`
    /// would leave the playhead in the old epoch so wrap-safe comparisons mark the whole new
    /// prebuffer already-due (firehose into the host ring). Defer re-anchor until a real packet
    /// arrives and claims the new epoch.
    pending_reanchor: bool,
    /// Cumulative map entries refused by the newest-drop pathological backstop.
    backstop_newest_drops: u64,
    /// Mono time of last rate-limited backstop warning.
    last_backstop_warn_ns: u64,
}

/// Synchronous, abort-safe stop handle for buffered playout.
///
/// Call [`stop`](Self::stop) *before* aborting the command/receive tasks so the
/// delivery thread unblocks even if the async [`PlayoutCommand::Stop`] is never polled.
#[derive(Clone)]
pub(crate) struct PlayoutStop {
    state: Arc<(Mutex<PlayoutState>, Condvar)>,
}

impl PlayoutStop {
    /// Mark playout stopped and wake the delivery thread.
    pub(crate) fn stop(&self) {
        let (lock, cvar) = &*self.state;
        let mut s = lock.lock().unwrap_or_else(PoisonError::into_inner);
        s.stopped = true;
        s.buffer.clear();
        cvar.notify_all();
    }
}

/// Ensures delivery stops when the receive task exits or is aborted.
struct ReceiveCleanup {
    stop: PlayoutStop,
}

impl Drop for ReceiveCleanup {
    fn drop(&mut self) {
        self.stop.stop();
    }
}

/// Wrap-safe closed range on the RTP u32 timeline: `from <= ts <= until` with wrap.
///
/// Mirrors the `(a.wrapping_sub(b) as i32) >= 0` half-plane checks used for due frames.
fn rtp_in_flush_range(ts: u32, from: u32, until: u32) -> bool {
    (ts.wrapping_sub(from) as i32) >= 0 && (until.wrapping_sub(ts) as i32) >= 0
}

/// Remove and return up to `max_frames` due packets (RTP ts ≤ `target_rtp`, wrap-safe).
fn take_due_frames(buffer: &mut BTreeMap<u32, Vec<f32>>, target_rtp: u32, max_frames: usize) -> Vec<(u32, Vec<f32>)> {
    let keys: Vec<u32> = buffer
        .keys()
        .copied()
        .filter(|&ts| (target_rtp.wrapping_sub(ts) as i32) >= 0)
        .take(max_frames)
        .collect();
    keys.into_iter()
        .filter_map(|ts| buffer.remove(&ts).map(|data| (ts, data)))
        .collect()
}

/// Max AAC packets for `secs` of source audio at 1024 samples/frame.
fn max_packets_for_secs(source_sample_rate: u32, secs: u32) -> usize {
    if source_sample_rate == 0 || secs == 0 {
        return 0;
    }
    (u64::from(source_sample_rate)
        .saturating_mul(u64::from(secs))
        .div_ceil(u64::from(AAC_FRAME_SAMPLES)))
    .max(1) as usize
}

/// True when the playout map is at/above `secs` of source audio by packet count or RTP span.
fn buffer_at_or_above_depth(buffer: &BTreeMap<u32, Vec<f32>>, source_sample_rate: u32, secs: u32) -> bool {
    if buffer.is_empty() || source_sample_rate == 0 || secs == 0 {
        return false;
    }
    if buffer.len() >= max_packets_for_secs(source_sample_rate, secs) {
        return true;
    }
    if let (Some((&first, _)), Some((&last, _))) = (buffer.first_key_value(), buffer.last_key_value()) {
        let span = last.wrapping_sub(first);
        let max_span = source_sample_rate.saturating_mul(secs);
        // Raw u32 BTree order: only treat non-wrapping positive spans as depth.
        if (span as i32) >= 0 && span >= max_span {
            return true;
        }
    }
    false
}

/// Block until the map is below the resume threshold or the session is stopped.
///
/// Entry condition is applied by the caller (depth ≥ target). Once waiting, stay
/// parked until depth &lt; resume (hysteresis) or `stopped`. Wakes on delivery consume,
/// flush, stop, and rate/command `notify_all`.
fn wait_for_map_space(lock: &Mutex<PlayoutState>, cvar: &Condvar) {
    let mut s = lock.lock().unwrap_or_else(PoisonError::into_inner);
    if s.stopped || !buffer_at_or_above_depth(&s.buffer, s.source_sample_rate, TARGET_BUFFER_DURATION_SECS) {
        return;
    }
    while !s.stopped && buffer_at_or_above_depth(&s.buffer, s.source_sample_rate, RESUME_BUFFER_DURATION_SECS) {
        s = cvar.wait(s).unwrap_or_else(PoisonError::into_inner);
    }
}

/// Wrap-aware signed RTP delta: `ts - reference` as `i32` (circular half-plane).
#[inline]
fn rtp_signed_delta(ts: u32, reference: u32) -> i32 {
    ts.wrapping_sub(reference) as i32
}

/// Drop map entries farther than `max_span` (source samples) from `reference_ts`.
///
/// After FLUSH / re-anchor, clients often jump the RTP epoch by billions of ticks while
/// old-epoch keys remain. Raw BTree span then looks &gt;180s and the old backstop dropped
/// **newest** (new epoch) while keeping obsolete head. Keeping only the epoch around the
/// reference (typically the newest insert) fixes that without raising the 180s cap.
fn retain_near_rtp_epoch(buffer: &mut BTreeMap<u32, Vec<f32>>, reference_ts: u32, max_span: u32) -> usize {
    if buffer.is_empty() || max_span == 0 {
        return 0;
    }
    let keys: Vec<u32> = buffer
        .keys()
        .copied()
        .filter(|&ts| rtp_signed_delta(ts, reference_ts).unsigned_abs() > max_span)
        .collect();
    let n = keys.len();
    for k in keys {
        buffer.remove(&k);
    }
    n
}

/// Apply SETRATEANCHORTIME to playout state: rate, wire anchor, optional nudge/purge.
///
/// On rate != 0: purge frames more than [`ANCHOR_BACKWARD_TOLERANCE_SECS`] behind the
/// wire anchor, then nudge `anchor_rtp` to earliest remaining minus 100 ms lead (or keep
/// the wire anchor if the map is empty). Frames still behind the nudged anchor are
/// discarded. On rate == 0: wire assignment only (pause) — no purge, no nudge.
///
/// Always clears [`PlayoutState::pending_reanchor`]: a wire SetRate is authoritative and
/// supersedes any deferred flush re-anchor.
fn apply_set_rate(s: &mut PlayoutState, wire_anchor: u32, rate: u32) {
    let was_paused = s.rate == 0;
    s.pending_reanchor = false;
    s.anchor_rtp = wire_anchor;
    s.rate = rate;
    s.pending_rate = Some(rate);

    if rate == 0 {
        info!("Playout paused");
        return;
    }

    // Drop pre-seek strays far behind the wire so earliest-in-map cannot drag the playhead.
    let tolerance_frames = s.source_sample_rate.saturating_mul(ANCHOR_BACKWARD_TOLERANCE_SECS);
    let tolerance_i32 = tolerance_frames as i32;
    let stray: Vec<u32> = s
        .buffer
        .keys()
        .filter(|&&ts| rtp_signed_delta(ts, wire_anchor) < -tolerance_i32)
        .copied()
        .collect();
    if !stray.is_empty() {
        debug!(purged = stray.len(), "Purged stray frames behind wire anchor");
    }
    for k in stray {
        s.buffer.remove(&k);
    }

    // Nudge so the earliest remaining frame is due with a small lead (smooth start / skip dead air).
    if let Some(&first_ts) = s.buffer.keys().next() {
        let lead_frames = reanchor_lead_frames(s.source_sample_rate);
        s.anchor_rtp = first_ts.wrapping_sub(lead_frames);
    }
    s.anchor_local_ns = mono_now_ns();

    // Frames within tolerance behind wire can still sit behind the nudged anchor
    // (e.g. earliest ahead of wire → nudge jumps past them).
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
        // Log the wire value from the command, not the post-nudge anchor.
        info!(anchor_rtp = wire_anchor, "Playout started");
    }
}

/// Apply FLUSHBUFFERED to playout state: trim map, epoch purge, arm pending re-anchor.
///
/// When both `from_ts` / `until_ts` are present, range and epoch use RTP timestamps (map
/// key units). Otherwise seq numbers are used (legacy path). While playing (`rate != 0`)
/// with `until_ts`, arm [`PlayoutState::pending_reanchor`] instead of anchoring to
/// `flushUntilTS` immediately — Apple may re-key the RTP epoch on the next packets.
/// Resolution happens on the receive insert path (see [`insert_decoded_frame`]: pending
/// resolve plus epoch-collapse correction if a straggler claimed pending first).
fn apply_flush(s: &mut PlayoutState, from_seq: u32, until_seq: u32, from_ts: Option<u32>, until_ts: Option<u32>) {
    // Prefer RTP timestamp fields when both are present — map keys are timestamps, not seq.
    let (range_from, range_until) = match (from_ts, until_ts) {
        (Some(from), Some(until)) => (from, until),
        _ => (from_seq, until_seq),
    };
    let epoch_ref = until_ts.unwrap_or(range_until);

    let keys: Vec<u32> = s
        .buffer
        .keys()
        .filter(|&&ts| rtp_in_flush_range(ts, range_from, range_until))
        .copied()
        .collect();
    for k in &keys {
        s.buffer.remove(k);
    }
    // FLUSH is an RTP epoch boundary: drop orphans far from the flush end so a later
    // multi-billion-tick re-anchor cannot trip the backstop.
    let max_span = s.source_sample_rate.saturating_mul(BACKSTOP_BUFFER_DURATION_SECS);
    let epoch_purged = retain_near_rtp_epoch(&mut s.buffer, epoch_ref, max_span);
    s.pending_flush = true;

    // Track skip often sends FLUSHBUFFERED without a following SETRATEANCHORTIME. Same-epoch
    // skips leave the next frame ~prebuffer ahead of a stale playhead (~30 s silence) unless
    // we re-anchor. Cross-epoch skips re-key RTP, so anchoring to flushUntilTS is wrong.
    // Arm pending and resolve on the first post-flush packet that survives epoch enforcement.
    // Do not compare flushUntilTS to the playhead — that comparison is cross-epoch coin-flip.
    if s.rate != 0 && until_ts.is_some() {
        s.pending_reanchor = true;
        debug!("Flush armed pending re-anchor for first surviving post-flush packet");
    }

    debug!(
        flushed = keys.len(),
        epoch_purged,
        map_len = s.buffer.len(),
        has_ts = until_ts.is_some(),
        pending_reanchor = s.pending_reanchor,
        "Flushed"
    );
}

/// Re-anchor playout to `inserted_ts` with 100 ms lead and clear `pending_reanchor`.
///
/// Returns the lead in source frames (for tracing).
fn apply_reanchor_to_inserted(s: &mut PlayoutState, inserted_ts: u32) -> u32 {
    let lead_frames = reanchor_lead_frames(s.source_sample_rate);
    s.anchor_rtp = inserted_ts.wrapping_sub(lead_frames);
    s.anchor_local_ns = mono_now_ns();
    s.pending_reanchor = false;
    lead_frames
}

/// Resolve a deferred flush re-anchor after a receive-path insert + epoch retain.
///
/// Call only after `buffer.insert(inserted_ts, …)` and [`retain_near_rtp_epoch`] so we can
/// tell whether the inserted key is a real new-epoch packet (still in the map) or an
/// old-epoch straggler that epoch purge just removed. Surviving packet re-anchors with a
/// 100 ms lead ([`reanchor_lead_frames`]); straggler leaves `pending_reanchor` set for the
/// next insert.
fn resolve_pending_reanchor_after_insert(s: &mut PlayoutState, inserted_ts: u32) {
    if !s.pending_reanchor {
        return;
    }
    // Old-epoch straggler: epoch retain removed the key — wait for a real new-epoch packet.
    if !s.buffer.contains_key(&inserted_ts) {
        return;
    }
    let lead_frames = apply_reanchor_to_inserted(s, inserted_ts);
    debug!(
        inserted_ts,
        lead_frames,
        anchor_rtp = s.anchor_rtp,
        "Pending re-anchor applied from first post-flush packet"
    );
}

/// When epoch retain collapses the map onto a new RTP epoch while playing, re-anchor if the
/// surviving insert is epoch-far from the current anchor.
///
/// Covers the cross-epoch skip hole where an old-epoch straggler claimed
/// [`PlayoutState::pending_reanchor`] first: pending is already false, but the true new-epoch
/// packet still needs the playhead moved. Wire SetRate still overrides later.
fn maybe_reanchor_on_epoch_collapse(s: &mut PlayoutState, inserted_ts: u32, epoch_dropped: usize, max_span: u32) {
    if epoch_dropped == 0 || s.rate == 0 || max_span == 0 {
        return;
    }
    // Inserted packet is the retain reference so it always survives; require it present anyway.
    if !s.buffer.contains_key(&inserted_ts) {
        return;
    }
    if rtp_signed_delta(inserted_ts, s.anchor_rtp).unsigned_abs() <= max_span {
        return;
    }
    let lead_frames = apply_reanchor_to_inserted(s, inserted_ts);
    debug!(
        inserted_ts,
        lead_frames,
        epoch_dropped,
        anchor_rtp = s.anchor_rtp,
        "Epoch-collapse re-anchor applied after map retained new epoch"
    );
}

/// Insert decoded PCM, enforce RTP epoch, then pending / epoch-collapse re-anchor.
///
/// Shared by the production receive loop and unit tests so both hit the same post-retain
/// logic. Returns the number of entries dropped by [`retain_near_rtp_epoch`] (for backstop
/// logging). Callers that need the newest-drop backstop run [`enforce_newest_backstop`] after.
fn insert_decoded_frame(s: &mut PlayoutState, timestamp: u32, samples: Vec<f32>) -> usize {
    s.buffer.insert(timestamp, samples);
    let max_span = s.source_sample_rate.saturating_mul(BACKSTOP_BUFFER_DURATION_SECS);
    let epoch_dropped = retain_near_rtp_epoch(&mut s.buffer, timestamp, max_span);
    resolve_pending_reanchor_after_insert(s, timestamp);
    maybe_reanchor_on_epoch_collapse(s, timestamp, epoch_dropped, max_span);
    epoch_dropped
}

/// Pathological backstop: drop **newest** entries until under 3× target depth.
///
/// Within a single epoch, never removes the head of the map (packets about to be
/// delivered). Callers should run [`retain_near_rtp_epoch`] first so multi-epoch
/// spans from FLUSH re-anchor do not look "pathologically deep". Returns count dropped.
fn enforce_newest_backstop(buffer: &mut BTreeMap<u32, Vec<f32>>, source_sample_rate: u32) -> usize {
    if buffer.is_empty() || source_sample_rate == 0 {
        return 0;
    }
    let max_packets = max_packets_for_secs(source_sample_rate, BACKSTOP_BUFFER_DURATION_SECS);
    let max_span = source_sample_rate.saturating_mul(BACKSTOP_BUFFER_DURATION_SECS);

    let mut dropped = 0usize;
    while buffer.len() > max_packets {
        if buffer.pop_last().is_some() {
            dropped += 1;
        } else {
            break;
        }
    }
    while let (Some((&first, _)), Some((&last, _))) = (buffer.first_key_value(), buffer.last_key_value()) {
        let span = last.wrapping_sub(first);
        if (span as i32) >= 0 && span <= max_span {
            break;
        }
        // Remaining multi-epoch / wrap span: drop oldest (obsolete epoch), not newest.
        if buffer.pop_first().is_some() {
            dropped += 1;
        } else {
            break;
        }
    }
    dropped
}

/// TCP listener for buffered audio. Binds a port and spawns the processing pipeline.
pub(crate) struct BufferedAudioProcessor {
    /// TCP listener waiting for the iPhone to connect.
    pub(crate) listener: TcpListener,
}

impl BufferedAudioProcessor {
    /// Start the processing pipeline.
    ///
    /// Returns the command sender, a **synchronous** stop handle (use this from
    /// `hard_stop_sessions` / TEARDOWN before aborting tasks), and abort handles for
    /// the async command/receive tasks.
    pub(crate) fn start(
        self,
        shk: [u8; 32],
        output_config: OutputConfig,
        handler: Arc<dyn AudioHandler>,
    ) -> (
        tokio::sync::mpsc::UnboundedSender<PlayoutCommand>,
        PlayoutStop,
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
                // Until format is detected, assume RTP clock == default output rate.
                source_sample_rate: default_sr,
                channels: 2,
                stopped: false,
                format_changed: false,
                pending_rate: None,
                pending_flush: false,
                pending_reanchor: false,
                backstop_newest_drops: 0,
                last_backstop_warn_ns: 0,
            }),
            Condvar::new(),
        ));
        let playout_stop = PlayoutStop {
            state: Arc::clone(&state),
        };

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
                        apply_set_rate(&mut s, anchor_rtp, rate);
                        cvar.notify_all();
                    }
                    PlayoutCommand::Flush {
                        from_seq,
                        until_seq,
                        from_ts,
                        until_ts,
                    } => {
                        apply_flush(&mut s, from_seq, until_seq, from_ts, until_ts);
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

        // Receiver task — cleanup runs via Drop even if this task is aborted.
        let state4 = state.clone();
        let recv_handle = tokio::spawn(async move {
            let _cleanup = ReceiveCleanup {
                stop: PlayoutStop {
                    state: Arc::clone(&state4),
                },
            };
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

        (
            cmd_tx,
            playout_stop,
            vec![cmd_handle.abort_handle(), recv_handle.abort_handle()],
        )
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
        // Read-driven flow control: before the next length-prefixed packet, if the
        // playout map is at/above target depth, park until delivery/flush drains it
        // below the resume threshold (or stop). Not reading fills the kernel RCVBUF
        // and TCP window so iOS paces itself. Condvar wait runs on the blocking pool
        // because the RTSP connection uses a current_thread runtime.
        // Guard must end in its own block so it is not held across `.await` (Send).
        let (stopped, need_wait) = {
            let (lock, _cvar) = &*state;
            let s = lock.lock().unwrap_or_else(PoisonError::into_inner);
            let stopped = s.stopped;
            let need_wait =
                !stopped && buffer_at_or_above_depth(&s.buffer, s.source_sample_rate, TARGET_BUFFER_DURATION_SECS);
            (stopped, need_wait)
        };
        if stopped {
            break;
        }
        if need_wait {
            let state_wait = Arc::clone(&state);
            let stopped_after = tokio::task::spawn_blocking(move || {
                let (lock, cvar) = &*state_wait;
                wait_for_map_space(lock, cvar);
                lock.lock().unwrap_or_else(PoisonError::into_inner).stopped
            })
            .await
            .unwrap_or(true);
            if stopped_after {
                break;
            }
        }

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
            let mut s = lock.lock().unwrap_or_else(PoisonError::into_inner);
            s.sample_rate = target_sr;
            s.source_sample_rate = src_sr;
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
            let mut s = lock.lock().unwrap_or_else(PoisonError::into_inner);
            // Insert + epoch purge + pending/epoch-collapse re-anchor, then packet-count backstop.
            let epoch_dropped = insert_decoded_frame(&mut s, timestamp, samples);
            let source_sr = s.source_sample_rate;
            let backstop_dropped = enforce_newest_backstop(&mut s.buffer, source_sr);
            let dropped = epoch_dropped.saturating_add(backstop_dropped);
            if dropped > 0 {
                s.backstop_newest_drops = s.backstop_newest_drops.saturating_add(dropped as u64);
                let now = mono_now_ns();
                if now.saturating_sub(s.last_backstop_warn_ns) >= BACKSTOP_WARN_INTERVAL_NS {
                    let first_rtp = s.buffer.keys().next().copied();
                    let last_rtp = s.buffer.keys().next_back().copied();
                    let span = match (first_rtp, last_rtp) {
                        (Some(f), Some(l)) => Some(l.wrapping_sub(f)),
                        _ => None,
                    };
                    warn!(
                        dropped_now = dropped,
                        dropped_total = s.backstop_newest_drops,
                        epoch_dropped,
                        backstop_dropped,
                        map_len = s.buffer.len(),
                        first_rtp,
                        last_rtp,
                        span_rtp = span,
                        "Buffered audio map exceeded backstop; dropped packets"
                    );
                    s.last_backstop_warn_ns = now;
                }
            }
            cvar.notify_all();
        }
    }
    // Delivery stop is owned by `ReceiveCleanup` Drop on the receive task so
    // abort still unblocks the delivery thread.
    debug!("Buffered audio receive loop ended");
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
            let now = mono_now_ns();
            let elapsed_ns = now.saturating_sub(s.anchor_local_ns);
            // Elapsed media time must advance on the *source* RTP clock, not the
            // (possibly resampled) output rate — otherwise target_rtp drifts.
            let elapsed_frames = (elapsed_ns as u128 * u128::from(s.source_sample_rate) / 1_000_000_000) as u32;
            let target_rtp = s.anchor_rtp.wrapping_add(elapsed_frames);

            ready = take_due_frames(&mut s.buffer, target_rtp, MAX_FRAMES_PER_TICK);
            // Wake a receive-loop waiter parked on full-map flow control.
            if !ready.is_empty() {
                cvar.notify_all();
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
        process_calls: AtomicUsize,
        samples_processed: AtomicUsize,
    }

    struct TrackingSession {
        counters: Arc<SessionCounters>,
        /// Global init/drop order log shared with the handler.
        order: Arc<Mutex<Vec<&'static str>>>,
    }

    impl AudioSession for TrackingSession {
        fn audio_process(&mut self, samples: &[f32]) {
            self.counters.process_calls.fetch_add(1, Ordering::SeqCst);
            self.counters
                .samples_processed
                .fetch_add(samples.len(), Ordering::SeqCst);
        }
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
                source_sample_rate: sample_rate,
                channels,
                stopped: false,
                format_changed: false,
                pending_rate: None,
                pending_flush: false,
                pending_reanchor: false,
                backstop_newest_drops: 0,
                last_backstop_warn_ns: 0,
            }),
            Condvar::new(),
        ))
    }

    /// Mirror the receive path: insert → epoch retain → pending / epoch-collapse re-anchor.
    fn receive_insert_with_pending_resolve(s: &mut PlayoutState, timestamp: u32, samples: Vec<f32>) {
        let _epoch_dropped = insert_decoded_frame(s, timestamp, samples);
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
        PlayoutStop {
            state: Arc::clone(state),
        }
        .stop();
    }

    fn join_with_timeout(join: std::thread::JoinHandle<()>, label: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if join.is_finished() {
                join.join().unwrap_or_else(|_| panic!("{label} panicked"));
                return;
            }
            if std::time::Instant::now() >= deadline {
                panic!("{label} did not exit within timeout");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
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
            s.anchor_local_ns = mono_now_ns();
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
        join_with_timeout(join, "delivery thread");

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

    /// Hard-stop path: sync stop must unblock delivery without an async PlayoutCommand.
    #[test]
    fn sync_stop_unblocks_delivery_and_drops_session() {
        let handler = TrackingHandler::new();
        let state = fresh_state(44_100, 2);
        let stop = PlayoutStop {
            state: Arc::clone(&state),
        };
        let join = spawn_delivery(Arc::clone(&state), Arc::clone(&handler));

        {
            let (lock, cvar) = &*state;
            let mut s = lock.lock().unwrap_or_else(PoisonError::into_inner);
            s.format_changed = true;
            s.rate = 1;
            s.buffer.insert(0, vec![0.0, 0.0]);
            s.anchor_local_ns = mono_now_ns();
            cvar.notify_all();
        }
        wait_until(|| handler.inits.load(Ordering::SeqCst) >= 1, "audio_init");

        // Mimic hard_stop_sessions: sync stop only (no async channel, no command task).
        stop.stop();
        join_with_timeout(join, "delivery after sync stop");

        let counters = current_counters(&handler).expect("session counters");
        assert!(
            counters.drops.load(Ordering::SeqCst) >= 1,
            "AudioSession must Drop when delivery exits after sync stop"
        );
        assert!(
            state.0.lock().unwrap_or_else(PoisonError::into_inner).stopped,
            "playout state must be stopped"
        );
    }

    #[test]
    fn receive_cleanup_drop_stops_playout() {
        let state = fresh_state(44_100, 2);
        {
            let _cleanup = ReceiveCleanup {
                stop: PlayoutStop {
                    state: Arc::clone(&state),
                },
            };
            assert!(!state.0.lock().unwrap_or_else(PoisonError::into_inner).stopped);
        }
        assert!(
            state.0.lock().unwrap_or_else(PoisonError::into_inner).stopped,
            "ReceiveCleanup Drop must stop playout (abort-safe)"
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
            s.anchor_local_ns = mono_now_ns();
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
            s.anchor_local_ns = mono_now_ns();
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
        join_with_timeout(join, "delivery thread");
    }

    #[test]
    fn take_due_frames_caps_per_tick() {
        let mut buffer = BTreeMap::new();
        // 64 due packets, all ts ≤ target.
        for i in 0u32..64 {
            buffer.insert(i * 1024, vec![i as f32]);
        }
        let ready = take_due_frames(&mut buffer, 64 * 1024, MAX_FRAMES_PER_TICK);
        assert_eq!(ready.len(), MAX_FRAMES_PER_TICK);
        assert_eq!(buffer.len(), 64 - MAX_FRAMES_PER_TICK);
        // Excess remains for a later tick.
        let more = take_due_frames(&mut buffer, 64 * 1024, MAX_FRAMES_PER_TICK);
        assert_eq!(more.len(), MAX_FRAMES_PER_TICK);
        assert_eq!(buffer.len(), 64 - 2 * MAX_FRAMES_PER_TICK);
    }

    #[test]
    fn delivery_tick_delivers_at_most_max_frames() {
        let handler = TrackingHandler::new();
        let state = fresh_state(44_100, 2);
        let join = spawn_delivery(Arc::clone(&state), Arc::clone(&handler));

        {
            let (lock, cvar) = &*state;
            let mut s = lock.lock().unwrap_or_else(PoisonError::into_inner);
            s.format_changed = true;
            s.rate = 1;
            s.anchor_rtp = 0;
            // Anchor far in the past so every inserted packet is immediately due.
            s.anchor_local_ns = mono_now_ns().saturating_sub(5_000_000_000);
            for i in 0u32..48 {
                s.buffer.insert(i * 1024, vec![0.0, 0.0]);
            }
            cvar.notify_all();
        }

        wait_until(
            || current_counters(&handler).is_some_and(|c| c.process_calls.load(Ordering::SeqCst) >= 1),
            "first audio_process",
        );
        // Give the delivery loop a short window to potentially over-deliver, then
        // assert a single-tick bound: process_calls should climb in steps of ≤ MAX.
        // After first burst, remaining packets stay buffered until subsequent ticks.
        std::thread::sleep(Duration::from_millis(15));
        let counters = current_counters(&handler).expect("session");
        let calls = counters.process_calls.load(Ordering::SeqCst);
        // Within ~15ms with 5ms sleep between empty-ready waits, at most a few ticks.
        // The critical property: map still holds excess after the first delivery window.
        let remaining = state.0.lock().unwrap_or_else(PoisonError::into_inner).buffer.len();
        assert!(
            remaining > 0,
            "expected excess due frames to remain after capped ticks; process_calls={calls}"
        );
        assert!(calls <= 48, "should not invent packets; process_calls={calls}");
        // First few ticks cannot empty 48 due packets under the per-tick cap.
        // Allow a handful of 5ms ticks in the 15ms window while still proving the cap.
        let max_ticks_in_window = 4usize;
        let min_remaining = 48usize.saturating_sub(MAX_FRAMES_PER_TICK.saturating_mul(max_ticks_in_window));
        assert!(
            remaining >= min_remaining,
            "cap should leave a large remainder early; remaining={remaining}, calls={calls}, min={min_remaining}"
        );

        stop_delivery(&state);
        join_with_timeout(join, "delivery frame-cap");
    }

    #[test]
    fn rtp_flush_range_wrap_safe() {
        // Range spanning u32 wrap: from near max to small positive.
        let from = u32::MAX - 100;
        let until = 50u32;
        assert!(rtp_in_flush_range(u32::MAX - 10, from, until));
        assert!(rtp_in_flush_range(0, from, until));
        assert!(rtp_in_flush_range(50, from, until));
        assert!(!rtp_in_flush_range(100, from, until));
        assert!(!rtp_in_flush_range(u32::MAX - 200, from, until));

        // Non-wrapping range still works.
        assert!(rtp_in_flush_range(1500, 1000, 2000));
        assert!(!rtp_in_flush_range(999, 1000, 2000));
        assert!(!rtp_in_flush_range(2001, 1000, 2000));
    }

    #[test]
    fn flush_command_removes_wrap_range_from_map() {
        let state = fresh_state(44_100, 2);
        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            s.buffer.insert(u32::MAX - 50, vec![1.0]);
            s.buffer.insert(10, vec![2.0]);
            s.buffer.insert(5000, vec![3.0]); // outside wrap range
        }

        // Simulate command-handler flush logic.
        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            let from_seq = u32::MAX - 100;
            let until_seq = 100u32;
            let keys: Vec<u32> = s
                .buffer
                .keys()
                .filter(|&&ts| rtp_in_flush_range(ts, from_seq, until_seq))
                .copied()
                .collect();
            for k in &keys {
                s.buffer.remove(k);
            }
            assert_eq!(keys.len(), 2);
            assert!(s.buffer.contains_key(&5000));
            assert_eq!(s.buffer.len(), 1);
        }
    }

    fn fill_map_packets(buffer: &mut BTreeMap<u32, Vec<f32>>, count: usize) {
        for i in 0..count {
            buffer.insert((i as u32).wrapping_mul(AAC_FRAME_SAMPLES), vec![i as f32]);
        }
    }

    /// At ≥ target depth, reads pause (waiter blocks); delivery drain below resume unblocks.
    /// No backstop drops while depth plateaus at target.
    #[test]
    fn read_flow_control_hysteresis_pauses_and_resumes() {
        let source_sr = 48_000u32;
        let state = fresh_state(source_sr, 2);
        let target_pkts = max_packets_for_secs(source_sr, TARGET_BUFFER_DURATION_SECS);
        let resume_pkts = max_packets_for_secs(source_sr, RESUME_BUFFER_DURATION_SECS);
        assert!(target_pkts > resume_pkts);

        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            fill_map_packets(&mut s.buffer, target_pkts);
            assert!(buffer_at_or_above_depth(
                &s.buffer,
                source_sr,
                TARGET_BUFFER_DURATION_SECS
            ));
            assert_eq!(s.buffer.len(), target_pkts, "map should plateau at target (no drops)");
        }

        let state_wait = Arc::clone(&state);
        let waiter = std::thread::spawn(move || {
            let (lock, cvar) = &*state_wait;
            wait_for_map_space(lock, cvar);
        });

        // Waiter must stay parked while depth stays at target.
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            !waiter.is_finished(),
            "read flow-control must pause while map is at target depth"
        );
        {
            let s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            assert_eq!(s.buffer.len(), target_pkts, "depth must not drop without delivery");
            assert_eq!(s.backstop_newest_drops, 0);
        }

        // Simulate delivery: shrink below resume and notify (as delivery_loop does).
        {
            let (lock, cvar) = &*state;
            let mut s = lock.lock().unwrap_or_else(PoisonError::into_inner);
            while s.buffer.len() >= resume_pkts {
                let _ = s.buffer.pop_first();
            }
            assert!(!buffer_at_or_above_depth(
                &s.buffer,
                source_sr,
                RESUME_BUFFER_DURATION_SECS
            ));
            cvar.notify_all();
        }

        join_with_timeout(waiter, "flow-control waiter after drain");
        let s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(s.buffer.len() < resume_pkts);
        assert_eq!(s.backstop_newest_drops, 0);
    }

    /// Stop while read-paused must wake the waiter promptly (abort-safe path).
    #[test]
    fn stop_unblocks_read_flow_control_wait() {
        let source_sr = 48_000u32;
        let state = fresh_state(source_sr, 2);
        let target_pkts = max_packets_for_secs(source_sr, TARGET_BUFFER_DURATION_SECS);
        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            fill_map_packets(&mut s.buffer, target_pkts);
        }

        let state_wait = Arc::clone(&state);
        let waiter = std::thread::spawn(move || {
            let (lock, cvar) = &*state_wait;
            wait_for_map_space(lock, cvar);
        });

        std::thread::sleep(Duration::from_millis(30));
        assert!(!waiter.is_finished(), "waiter should be parked on full map");

        PlayoutStop {
            state: Arc::clone(&state),
        }
        .stop();
        join_with_timeout(waiter, "flow-control waiter after stop");

        let s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(s.stopped);
        assert!(s.buffer.is_empty(), "stop clears the map");
    }

    /// Flush while read-paused empties enough of the map and unblocks the waiter.
    #[test]
    fn flush_unblocks_read_flow_control_wait() {
        let source_sr = 48_000u32;
        let state = fresh_state(source_sr, 2);
        let target_pkts = max_packets_for_secs(source_sr, TARGET_BUFFER_DURATION_SECS);
        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            fill_map_packets(&mut s.buffer, target_pkts);
        }

        let state_wait = Arc::clone(&state);
        let waiter = std::thread::spawn(move || {
            let (lock, cvar) = &*state_wait;
            wait_for_map_space(lock, cvar);
        });

        std::thread::sleep(Duration::from_millis(30));
        assert!(!waiter.is_finished(), "waiter should be parked on full map");

        // Command-path flush: clear map + notify_all (same as PlayoutCommand::Flush full clear).
        {
            let (lock, cvar) = &*state;
            let mut s = lock.lock().unwrap_or_else(PoisonError::into_inner);
            s.buffer.clear();
            s.pending_flush = true;
            cvar.notify_all();
        }

        join_with_timeout(waiter, "flow-control waiter after flush");
        let s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(s.buffer.is_empty());
        assert!(!s.stopped, "flush must not stop the session");
    }

    /// Pathological backstop drops newest only; head (playhead) stays intact.
    #[test]
    fn backstop_drops_newest_preserves_head() {
        let source_sr = 48_000u32;
        let mut buffer = BTreeMap::new();
        let backstop_pkts = max_packets_for_secs(source_sr, BACKSTOP_BUFFER_DURATION_SECS);
        let over = backstop_pkts + 64;
        fill_map_packets(&mut buffer, over);
        let head_before = *buffer.keys().next().expect("non-empty");
        let tail_before = *buffer.keys().next_back().expect("non-empty");

        let dropped = enforce_newest_backstop(&mut buffer, source_sr);
        assert!(dropped > 0, "expected newest packets refused past 3× target");
        assert!(
            buffer.len() <= backstop_pkts,
            "len={} backstop_pkts={backstop_pkts}",
            buffer.len()
        );

        let head_after = *buffer.keys().next().expect("non-empty after backstop");
        assert_eq!(head_after, head_before, "head/playhead must remain intact");
        let tail_after = *buffer.keys().next_back().expect("non-empty after backstop");
        assert!(
            tail_after < tail_before,
            "newest/tail must shrink; before={tail_before} after={tail_after}"
        );

        // Under limit: no-op.
        let mut small = BTreeMap::new();
        fill_map_packets(&mut small, 10);
        assert_eq!(enforce_newest_backstop(&mut small, source_sr), 0);
        assert_eq!(small.len(), 10);
    }

    /// Wait returns immediately when the map is under the target depth.
    #[test]
    fn wait_for_map_space_noop_when_under_target() {
        let source_sr = 48_000u32;
        let state = fresh_state(source_sr, 2);
        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            fill_map_packets(&mut s.buffer, 8);
        }
        let (lock, cvar) = &*state;
        // Must not block.
        wait_for_map_space(lock, cvar);
        assert!(!state.0.lock().unwrap_or_else(PoisonError::into_inner).stopped);
    }

    // --- B3: FLUSH / RTP epoch discontinuity ---

    /// Forward multi-billion-tick jump must purge old epoch, not drop thousands of new packets.
    #[test]
    fn flush_forward_epoch_jump_keeps_new_epoch() {
        let source_sr = 44_100u32;
        let max_span = source_sr.saturating_mul(BACKSTOP_BUFFER_DURATION_SECS);
        let mut buffer = BTreeMap::new();
        // Old epoch: ~1400 packets (~32s) of keys near zero.
        fill_map_packets(&mut buffer, 1400);
        assert_eq!(buffer.len(), 1400);

        // New epoch after FLUSH re-anchor (multi-billion tick jump).
        let new_base: u32 = 3_000_000_000;
        for i in 0..32u32 {
            buffer.insert(new_base.wrapping_add(i.wrapping_mul(AAC_FRAME_SAMPLES)), vec![i as f32]);
        }
        // Pre-fix span backstop would pop_last thousands of times and kill the new epoch.
        let epoch_dropped = retain_near_rtp_epoch(&mut buffer, new_base, max_span);
        assert!(
            epoch_dropped >= 1400,
            "old epoch must be purged; dropped={epoch_dropped}"
        );
        let backstop_dropped = enforce_newest_backstop(&mut buffer, source_sr);
        assert_eq!(
            backstop_dropped, 0,
            "single new-epoch cluster must not trip count/span backstop"
        );
        assert!(
            buffer.len() <= 32,
            "only new-epoch packets remain; len={}",
            buffer.len()
        );
        assert!(
            buffer
                .keys()
                .all(|&k| rtp_signed_delta(k, new_base).unsigned_abs() <= max_span),
            "all remaining keys must be near new epoch"
        );
    }

    /// Backward multi-billion-tick jump must purge the obsolete high keys.
    #[test]
    fn flush_backward_epoch_jump_keeps_new_epoch() {
        let source_sr = 44_100u32;
        let max_span = source_sr.saturating_mul(BACKSTOP_BUFFER_DURATION_SECS);
        let mut buffer = BTreeMap::new();
        let old_base: u32 = 3_000_000_000;
        for i in 0..1400u32 {
            buffer.insert(old_base.wrapping_add(i.wrapping_mul(AAC_FRAME_SAMPLES)), vec![1.0]);
        }
        let new_base: u32 = 100_000;
        for i in 0..16u32 {
            buffer.insert(new_base.wrapping_add(i.wrapping_mul(AAC_FRAME_SAMPLES)), vec![2.0]);
        }

        let epoch_dropped = retain_near_rtp_epoch(&mut buffer, new_base, max_span);
        assert!(epoch_dropped >= 1400, "old high epoch purged; dropped={epoch_dropped}");
        assert_eq!(enforce_newest_backstop(&mut buffer, source_sr), 0);
        assert!(
            buffer
                .keys()
                .all(|&k| rtp_signed_delta(k, new_base).unsigned_abs() <= max_span)
        );
        assert!(buffer.contains_key(&new_base));
    }

    /// FLUSH range removal + epoch retain must not leave a multi-epoch span.
    #[test]
    fn flush_command_epoch_boundary_no_bulk_backstop() {
        let source_sr = 44_100u32;
        let state = fresh_state(source_sr, 2);
        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            // Old keys outside a partial flush range + keys inside range.
            for i in 0..200u32 {
                s.buffer.insert(i.wrapping_mul(AAC_FRAME_SAMPLES), vec![i as f32]);
            }
            // Simulate command-handler flush of the low range, then epoch retain on until.
            let from_seq = 0u32;
            let until_seq = 150u32 * AAC_FRAME_SAMPLES;
            let keys: Vec<u32> = s
                .buffer
                .keys()
                .filter(|&&ts| rtp_in_flush_range(ts, from_seq, until_seq))
                .copied()
                .collect();
            for k in &keys {
                s.buffer.remove(k);
            }
            let max_span = source_sr.saturating_mul(BACKSTOP_BUFFER_DURATION_SECS);
            let purged = retain_near_rtp_epoch(&mut s.buffer, until_seq, max_span);
            // Remaining should be near until_seq (or empty if all far).
            let dropped = enforce_newest_backstop(&mut s.buffer, source_sr);
            assert_eq!(dropped, 0, "no multi-thousand backstop after flush epoch retain");
            assert!(
                s.buffer.len() < 100,
                "map must not retain multi-epoch bulk; len={} purged={purged}",
                s.buffer.len()
            );
        }
    }

    // --- FLUSHBUFFERED TS fields + pending re-anchor (track skip without SETRATEANCHORTIME) ---

    /// Same-epoch skip: flush arms pending; first post-flush packet at until_ts re-anchors
    /// so the frame is due within the lead window (protects the original ~30 s silence fix).
    #[test]
    fn flush_with_ts_while_playing_reanchors() {
        let source_sr = 44_100u32;
        let lead = source_sr / 10;
        let flush_until = 1_322_880u32; // ~30 s at 44.1 kHz (sender prebuffer depth)
        let state = fresh_state(source_sr, 2);
        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            s.rate = 1;
            s.anchor_rtp = 0;
            s.anchor_local_ns = mono_now_ns();
            let anchor_before = s.anchor_rtp;
            let local_before = s.anchor_local_ns;
            // Prebuffer/old track frames inside the flush range (removed by trim).
            s.buffer.insert(1000, vec![0.0]);
            s.buffer.insert(500_000, vec![0.5]);

            apply_flush(&mut s, 0, 0, Some(0), Some(flush_until));

            assert!(s.pending_flush);
            assert!(s.pending_reanchor, "playing flush with until_ts must arm pending");
            assert!(!s.buffer.contains_key(&1000));
            assert!(!s.buffer.contains_key(&500_000));
            // Anchor must not move until a surviving post-flush packet arrives.
            assert_eq!(s.anchor_rtp, anchor_before);
            assert_eq!(s.anchor_local_ns, local_before);

            // Next-track packet at flush end (same epoch) — receive path resolve.
            receive_insert_with_pending_resolve(&mut s, flush_until, vec![1.0, 1.0]);
            assert!(!s.pending_reanchor, "surviving insert must clear pending");
            assert_eq!(s.anchor_rtp, flush_until.wrapping_sub(lead));

            let target_at_lead = s.anchor_rtp.wrapping_add(lead);
            let due = take_due_frames(&mut s.buffer, target_at_lead, MAX_FRAMES_PER_TICK);
            assert!(
                due.iter().any(|(ts, _)| *ts == flush_until),
                "frame at flushUntilTS must be due within lead; due={due:?}"
            );
        }
    }

    /// Cross-epoch skip with an in-flight old-epoch straggler before the first new-track
    /// packet: straggler claims pending and clears it; true new-epoch insert collapses the
    /// map (`epoch_dropped > 0`) and must still re-anchor (pending may already be false).
    /// Without the epoch-collapse correction the anchor stays in the dead epoch and the
    /// new-track burst firehoses via `take_due_frames`.
    #[test]
    fn flush_cross_epoch_straggler_then_new_epoch_reanchors() {
        let source_sr = 44_100u32;
        let lead = reanchor_lead_frames(source_sr);
        let old_epoch_until = 1_000_000u32;
        // In-flight old-epoch packet near flush end (same half-plane as pre-flush map).
        let old_straggler = old_epoch_until.wrapping_sub(5_000);
        // New RTP base ~2e9 ticks from old flush end (wrap-safe past half-plane).
        let new_base = old_epoch_until.wrapping_sub(2_000_000_000);
        let burst_packets = 40u32;
        let state = fresh_state(source_sr, 2);
        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            s.rate = 1;
            s.anchor_rtp = 0;
            s.anchor_local_ns = mono_now_ns();
            for i in 0..20u32 {
                s.buffer.insert(i.wrapping_mul(AAC_FRAME_SAMPLES), vec![0.0]);
            }

            apply_flush(&mut s, 0, 0, Some(0), Some(old_epoch_until));
            assert!(s.pending_reanchor);
            assert_eq!(s.anchor_rtp, 0, "flush must not re-anchor until insert");

            // Straggler arrives first: map still old-epoch → no collapse, claims pending.
            receive_insert_with_pending_resolve(&mut s, old_straggler, vec![0.5, 0.5]);
            assert!(!s.pending_reanchor, "straggler must clear pending_reanchor");
            assert_eq!(
                s.anchor_rtp,
                old_straggler.wrapping_sub(lead),
                "straggler claims anchor before new epoch arrives"
            );
            assert!(s.buffer.contains_key(&old_straggler));

            // True new-epoch packet collapses map off the straggler; anchor must follow.
            receive_insert_with_pending_resolve(&mut s, new_base, vec![1.0, 1.0]);
            assert!(
                !s.buffer.contains_key(&old_straggler),
                "epoch retain must drop straggler"
            );
            assert!(s.buffer.contains_key(&new_base));
            assert!(!s.pending_reanchor);
            assert_eq!(
                s.anchor_rtp,
                new_base.wrapping_sub(lead),
                "epoch-collapse must re-anchor to new-epoch insert despite pending already clear"
            );

            // Fill remaining burst on the new epoch (first packet already inserted).
            for i in 1..burst_packets {
                let ts = new_base.wrapping_add(i.wrapping_mul(AAC_FRAME_SAMPLES));
                receive_insert_with_pending_resolve(&mut s, ts, vec![1.0, 1.0]);
            }
            // Anchor must stay on the new epoch (not drift back to the straggler).
            assert_eq!(s.anchor_rtp, new_base.wrapping_sub(lead));

            let target_at_lead = s.anchor_rtp.wrapping_add(lead);
            assert_eq!(target_at_lead, new_base);
            let due = take_due_frames(&mut s.buffer, target_at_lead, MAX_FRAMES_PER_TICK);
            assert!(
                !due.is_empty(),
                "at least the first new-epoch frame must be due within lead"
            );
            assert!(
                due.len() <= 2,
                "lead window must not firehose the burst after straggler race; due={}",
                due.len()
            );
            assert!(
                due.iter().all(|(ts, _)| rtp_signed_delta(*ts, new_base) <= 0),
                "only frames at/before new_base due at lead target; due={due:?}"
            );
            let remaining = s.buffer.len();
            assert!(
                remaining >= (burst_packets as usize).saturating_sub(due.len()),
                "most of the burst must still be buffered; remaining={remaining} due={}",
                due.len()
            );

            // Sanity: stranded old-epoch playhead would have firehosed the whole burst.
            let bad_target = old_straggler.wrapping_sub(lead).wrapping_add(lead);
            let mut probe = BTreeMap::new();
            for i in 0..burst_packets {
                let ts = new_base.wrapping_add(i.wrapping_mul(AAC_FRAME_SAMPLES));
                probe.insert(ts, vec![1.0, 1.0]);
            }
            let would_be_due = take_due_frames(&mut probe, bad_target, MAX_FRAMES_PER_TICK);
            assert!(
                would_be_due.len() >= MAX_FRAMES_PER_TICK.min(burst_packets as usize),
                "sanity: old-epoch playhead would firehose; due={}",
                would_be_due.len()
            );
        }
    }

    /// Cross-epoch skip: Apple re-keys RTP after FLUSH into a past half-plane of the old
    /// flushUntilTS. Immediate anchor to flushUntilTS would mark the whole new prebuffer
    /// already-due (firehose). Pending resolve on the new-epoch insert keeps only the
    /// lead-window amount due at the post-anchor target.
    #[test]
    fn flush_cross_epoch_pending_reanchor_no_firehose() {
        let source_sr = 44_100u32;
        let lead = source_sr / 10;
        let old_epoch_until = 1_000_000u32;
        // New RTP base ~2e9 ticks "behind" old flush end (wrap-safe past half-plane).
        // Anchoring to old flushUntil would make this entire burst already-due.
        let new_base = old_epoch_until.wrapping_sub(2_000_000_000);
        let burst_packets = 40u32;
        let state = fresh_state(source_sr, 2);
        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            s.rate = 1;
            s.anchor_rtp = 0;
            s.anchor_local_ns = mono_now_ns();
            // Old-epoch prebuffer inside the flush range.
            for i in 0..20u32 {
                s.buffer.insert(i.wrapping_mul(AAC_FRAME_SAMPLES), vec![0.0]);
            }

            apply_flush(&mut s, 0, 0, Some(0), Some(old_epoch_until));
            assert!(s.pending_reanchor);
            assert_eq!(s.anchor_rtp, 0, "flush must not re-anchor until insert");

            // Burst of new-epoch packets (receive path).
            for i in 0..burst_packets {
                let ts = new_base.wrapping_add(i.wrapping_mul(AAC_FRAME_SAMPLES));
                receive_insert_with_pending_resolve(&mut s, ts, vec![1.0, 1.0]);
            }
            assert!(!s.pending_reanchor, "first surviving new-epoch insert must re-anchor");
            assert_eq!(s.anchor_rtp, new_base.wrapping_sub(lead));

            // Prove the firehose condition: relative to an old-epoch playhead the whole
            // burst would be due; with correct re-anchor only the lead window is due.
            let bad_target = old_epoch_until.wrapping_sub(lead);
            let would_be_due = s
                .buffer
                .keys()
                .filter(|&&ts| (bad_target.wrapping_sub(ts) as i32) >= 0)
                .count();
            assert!(
                would_be_due >= burst_packets as usize,
                "sanity: old-epoch playhead would firehose; would_be_due={would_be_due}"
            );

            let target_at_lead = s.anchor_rtp.wrapping_add(lead);
            assert_eq!(target_at_lead, new_base);
            let due = take_due_frames(&mut s.buffer, target_at_lead, MAX_FRAMES_PER_TICK);
            assert!(
                !due.is_empty(),
                "at least the first new-epoch frame must be due within lead"
            );
            // At lead target only ~1 packet is due (the one at new_base), not the full tick cap.
            assert!(
                due.len() <= 2,
                "lead window must not firehose the burst; due={}",
                due.len()
            );
            assert!(
                due.iter().all(|(ts, _)| rtp_signed_delta(*ts, new_base) <= 0),
                "only frames at/before new_base due at lead target; due={due:?}"
            );
            let remaining = s.buffer.len();
            assert!(
                remaining >= (burst_packets as usize).saturating_sub(due.len()),
                "most of the burst must still be buffered; remaining={remaining} due={}",
                due.len()
            );
        }
    }

    /// Pause-path flush (rate == 0) must not arm pending re-anchor or move the anchor.
    #[test]
    fn flush_while_paused_does_not_reanchor() {
        let source_sr = 44_100u32;
        let state = fresh_state(source_sr, 2);
        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            s.rate = 0;
            s.anchor_rtp = 42_000;
            s.anchor_local_ns = 99_000_000;
            s.buffer.insert(100, vec![1.0]);
            s.buffer.insert(50_000, vec![2.0]);

            apply_flush(&mut s, 0, 0, Some(0), Some(50_000));

            assert_eq!(s.anchor_rtp, 42_000);
            assert_eq!(s.anchor_local_ns, 99_000_000);
            assert!(s.pending_flush);
            assert!(!s.pending_reanchor, "paused flush must not arm pending_reanchor");
            assert!(!s.buffer.contains_key(&100));
        }
    }

    /// Wire SETRATEANCHORTIME clears pending and applies SetRate anchor rules.
    #[test]
    fn set_rate_clears_pending_reanchor() {
        let source_sr = 44_100u32;
        let lead = source_sr / 10;
        let flush_until = 1_000_000u32;
        let wire = 2_000_000u32;
        let state = fresh_state(source_sr, 2);
        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            s.rate = 1;
            s.anchor_rtp = 0;
            s.anchor_local_ns = mono_now_ns();
            s.buffer.insert(wire, vec![1.0, 1.0]);

            apply_flush(&mut s, 0, 0, Some(0), Some(flush_until));
            assert!(s.pending_reanchor);

            apply_set_rate(&mut s, wire, 1);

            assert!(!s.pending_reanchor, "SetRate must clear pending_reanchor");
            assert_eq!(s.anchor_rtp, wire.wrapping_sub(lead));
            assert_eq!(s.rate, 1);
        }
    }

    /// TS-based trim removes exactly [fromTS, untilTS] (wrap-safe) and leaves later frames.
    #[test]
    fn flush_ts_trim_removes_range_wrap_safe() {
        let state = fresh_state(44_100, 2);
        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            s.rate = 0; // no re-anchor side effects
            let from_ts = u32::MAX - 100;
            let until_ts = 100u32;
            s.buffer.insert(u32::MAX - 50, vec![1.0]);
            s.buffer.insert(10, vec![2.0]);
            s.buffer.insert(until_ts, vec![3.0]); // inclusive end
            s.buffer.insert(5000, vec![4.0]); // after range
            // Seq fields deliberately wrong units / values — TS path must win.
            apply_flush(&mut s, 999_999, 999_999, Some(from_ts), Some(until_ts));

            assert!(!s.buffer.contains_key(&(u32::MAX - 50)));
            assert!(!s.buffer.contains_key(&10));
            assert!(!s.buffer.contains_key(&until_ts));
            assert!(s.buffer.contains_key(&5000));
            assert_eq!(s.buffer.len(), 1);
        }
    }

    /// Seq-only flush (no TS fields) keeps legacy range/epoch behavior and does not re-anchor.
    #[test]
    fn flush_seq_only_preserves_old_behavior() {
        let source_sr = 44_100u32;
        let state = fresh_state(source_sr, 2);
        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            s.rate = 1;
            s.anchor_rtp = 0;
            s.anchor_local_ns = mono_now_ns();
            let anchor_before = s.anchor_rtp;
            let local_before = s.anchor_local_ns;

            // Keys are RTP timestamps; seq-only path compares seq numbers as if they were TS.
            s.buffer.insert(u32::MAX - 50, vec![1.0]);
            s.buffer.insert(10, vec![2.0]);
            s.buffer.insert(5000, vec![3.0]);

            let from_seq = u32::MAX - 100;
            let until_seq = 100u32;
            apply_flush(&mut s, from_seq, until_seq, None, None);

            assert!(!s.buffer.contains_key(&(u32::MAX - 50)));
            assert!(!s.buffer.contains_key(&10));
            assert!(s.buffer.contains_key(&5000));
            assert_eq!(s.buffer.len(), 1);
            // No until_ts → no re-anchor / pending even while playing.
            assert_eq!(s.anchor_rtp, anchor_before);
            assert_eq!(s.anchor_local_ns, local_before);
            assert!(s.pending_flush);
            assert!(!s.pending_reanchor);
        }
    }

    // --- SETRATEANCHORTIME wire-anchor authority + stray purge before lead nudge ---

    /// A packet ~145 s behind the wire must not drag the playhead into the past after scrub.
    #[test]
    fn set_rate_purges_stray_far_behind_wire_anchor() {
        let source_sr = 44_100u32;
        let lead = source_sr / 10;
        let wire = 941_597_805u32;
        let stray = wire.wrapping_sub(145 * source_sr);
        let state = fresh_state(source_sr, 2);
        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            s.rate = 0;
            s.buffer.insert(stray, vec![0.0]);
            s.buffer.insert(wire, vec![1.0, 1.0]);

            apply_set_rate(&mut s, wire, 1);

            assert!(!s.buffer.contains_key(&stray), "stray far behind wire must be purged");
            assert!(s.buffer.contains_key(&wire), "frame at wire must remain");
            assert_eq!(s.anchor_rtp, wire.wrapping_sub(lead));
            assert_eq!(s.rate, 1);
            assert_eq!(s.pending_rate, Some(1));

            let target_at_lead = s.anchor_rtp.wrapping_add(lead);
            let due = take_due_frames(&mut s.buffer, target_at_lead, MAX_FRAMES_PER_TICK);
            assert!(
                due.iter().any(|(ts, _)| *ts == wire),
                "frame at wire must be due within lead; due={due:?}"
            );
        }
    }

    /// Earliest within the 2 s backward tolerance still gets the earliest-minus-lead nudge.
    #[test]
    fn set_rate_nudge_within_backward_tolerance() {
        let source_sr = 44_100u32;
        let lead = source_sr / 10;
        let wire = 1_000_000u32;
        let earliest = wire.wrapping_sub(source_sr); // 1 s behind wire
        let state = fresh_state(source_sr, 2);
        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            s.rate = 0;
            s.buffer.insert(earliest, vec![1.0]);
            s.buffer.insert(wire, vec![2.0]);

            apply_set_rate(&mut s, wire, 1);

            assert!(s.buffer.contains_key(&earliest));
            assert_eq!(s.anchor_rtp, earliest.wrapping_sub(lead));
        }
    }

    /// Earliest ahead of wire: nudge to earliest - lead (skip dead air).
    #[test]
    fn set_rate_nudge_when_earliest_ahead_of_wire() {
        let source_sr = 44_100u32;
        let lead = source_sr / 10;
        let wire = 1_000_000u32;
        let earliest = wire.wrapping_add(5 * source_sr); // 5 s ahead
        let state = fresh_state(source_sr, 2);
        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            s.rate = 0;
            s.buffer.insert(earliest, vec![1.0]);

            apply_set_rate(&mut s, wire, 1);

            assert!(s.buffer.contains_key(&earliest));
            assert_eq!(s.anchor_rtp, earliest.wrapping_sub(lead));
        }
    }

    /// Empty map keeps the wire anchor exactly (no nudge source).
    #[test]
    fn set_rate_empty_buffer_keeps_wire_anchor() {
        let source_sr = 44_100u32;
        let wire = 941_597_805u32;
        let state = fresh_state(source_sr, 2);
        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            s.rate = 0;
            // Sentinel: mono_now_ns() can legitimately return 0 on the process's first
            // call (lazy epoch), so assert the field changed rather than became nonzero.
            s.anchor_local_ns = u64::MAX;

            apply_set_rate(&mut s, wire, 1);

            assert!(s.buffer.is_empty());
            assert_eq!(s.anchor_rtp, wire);
            assert_ne!(s.anchor_local_ns, u64::MAX, "local anchor time must update on play");
            assert_eq!(s.pending_rate, Some(1));
        }
    }

    /// rate == 0: wire assignment only — no purge, no earliest-frame nudge.
    #[test]
    fn set_rate_zero_no_purge_no_nudge() {
        let source_sr = 44_100u32;
        let wire = 941_597_805u32;
        let stray = wire.wrapping_sub(145 * source_sr);
        let state = fresh_state(source_sr, 2);
        {
            let mut s = state.0.lock().unwrap_or_else(PoisonError::into_inner);
            s.rate = 1;
            s.anchor_rtp = 0;
            s.anchor_local_ns = 123;
            s.buffer.insert(stray, vec![0.0]);

            s.pending_reanchor = true;
            apply_set_rate(&mut s, wire, 0);

            assert_eq!(s.rate, 0);
            assert_eq!(s.anchor_rtp, wire);
            assert_eq!(s.anchor_local_ns, 123, "pause must not rewrite local anchor");
            assert!(s.buffer.contains_key(&stray), "pause must not purge strays");
            assert_eq!(s.pending_rate, Some(0));
            assert!(!s.pending_reanchor, "SetRate rate=0 must still clear pending");
        }
    }
}
