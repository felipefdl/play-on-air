//! AirPlay 2 buffered audio (stream type 103).
//!
//! The sender pushes encrypted AAC packets over TCP, usually far ahead of real time, and tells
//! the receiver *when* to play them with `SETRATEANCHORTIME`. This module follows the reference
//! receiver (`docs/ap2-receiver-spec.md` sections 1 and 2):
//!
//! - **Timeline.** RTP timestamps are extended to 64 bits (rollover counted on the assumption
//!   that RTP time increases). An anchor pins RTP `anchor_rtp` to local instant `H(anchor)`;
//!   every sample `r` is presented at `H(anchor) + (r - anchor_rtp) / sample_rate`.
//! - **Release.** PlayOnAir's output is not a DAC but a host ring feeding a Cast speaker that
//!   holds its own cushion. The receiver advertises that output delay as the RTSP
//!   `Audio-Latency` (`L`, in source samples). A packet is handed to the host when
//!   `now >= presentation - L`, so it becomes audible at its presentation time after the Cast
//!   cushion. Release is therefore paced by the sender's timeline, never by arrival.
//! - **Late packets.** A packet whose end is already behind the playhead (`presentation < now`)
//!   is pruned; an anchor in the past joins mid-stream.
//! - **Rate.** Only 0 and 1 are valid. Rate 0 stops release, keeps the buffer and clears the
//!   anchor; the next rate 1 must carry a new anchor. Each anchor start fades in over 30 ms.
//! - **Back-pressure.** Up to 120 s of audio is buffered. At that high-water mark the TCP
//!   reader stops reading so the kernel window throttles the sender. Nothing is dropped for
//!   being early.
//! - **FLUSHBUFFERED.** Sequence numbers are compared modulo 2^24. Flush-all stops playback,
//!   discards everything, resets the anchor and RTP epoch, and drops incoming packets before
//!   `flushUntilSeq`. A range flush keeps playing, trims buffered audio from `flushFromTS` to the
//!   end, and drops incoming packets in `[flushFromSeq, flushUntilSeq)`. Each drop rule retires
//!   once a packet at `flushUntilSeq + 20` or later arrives.
//!
//! Anchors normally arrive already mapped to a local instant through the PTP follower
//! ([`crate::net::ptp`]). When the follower cannot provide that mapping (ports owned by another
//! PTP stack, not yet locked, or a timeline that is not a session peer) the reference receiver
//! refuses the anchor; PlayOnAir plays anyway with the **unlocked fallback**: the first anchor on
//! a timeline is mapped so its sample is released on receipt (`H = receipt + L`), and that
//! sender-clock to local-clock offset is kept for later anchors on the same timeline so their
//! relative timing (pause/resume, seeks) stays correct. If a later anchor lands more than
//! [`FALLBACK_MAX_SKEW_NS`] away from "release on receipt", the offset is re-pinned, which
//! bounds the error to that skew. Pacing stays real time either way.
//!
//! Packets are stored compressed (decrypted AAC) and decoded on the delivery thread at release,
//! so 120 s of buffer costs a few MiB instead of tens of MiB of PCM.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tracing::{debug, info, warn};

use crate::codec::aac::{AacDecoder, AudioSsrc};
use crate::error::{CodecError, NetworkError, ShairplayError};
use crate::raop::audio_pipeline::{NONCE_TRAIL_LEN, RTP_HEADER_LEN, decrypt_rtp_chacha};
use crate::raop::{AudioCodec, AudioFormat, AudioHandler, AudioSession};
use crate::util::mono_now_ns;

const NS_PER_SEC: i128 = 1_000_000_000;
/// Buffered-audio high-water mark: reads suspend once this much audio is queued.
const HIGH_WATER_NS: i128 = 120 * NS_PER_SEC;
/// Reads resume below this depth (1 s of hysteresis so the reader is not woken per packet).
const RESUME_BELOW_NS: i128 = 119 * NS_PER_SEC;
/// Fade-in applied at every anchor start.
const FADE_IN_NS: u64 = 30_000_000;
/// A flush drop rule retires once a packet this many sequence numbers past `flushUntilSeq`
/// arrives; packets in between pass and keep it alive for reordered stragglers.
const FLUSH_RETIRE_SPAN: u32 = 20;
/// Buffered RTP sequence numbers are 24-bit.
const SEQ_MASK: u32 = 0x00FF_FFFF;
const SEQ_HALF: u32 = 0x0080_0000;
/// AAC-LC access unit length in source samples.
const AAC_FRAME_SAMPLES: i128 = 1024;
/// Unlocked fallback: re-pin the sender-clock offset when an anchor lands farther than this
/// from "release on receipt".
const FALLBACK_MAX_SKEW_NS: i128 = NS_PER_SEC;
/// RTP clock assumed until the first packet names its format.
const DEFAULT_SOURCE_RATE: u32 = 44_100;
/// Reference receiver error for an invalid `SETRATEANCHORTIME` / `FLUSHBUFFERED` parameter.
pub(crate) const STATUS_PARAM_ERROR: i64 = -6705;

/// SETUP `audioBufferSize`: 120 s times the largest bytes-per-second of the formats the
/// buffered stream accepts.
///
/// The decoder takes AAC-LC at 44.1 or 48 kHz. The spec's per-format AAC bit rates are not
/// public, so this uses the PCM rule (rate x bits x channels) for the largest stereo format,
/// 48 kHz 16-bit: 120 x 192000 B/s. Advertising the PCM bound is safe: the real limit is the
/// 120 s duration high-water mark enforced by TCP back-pressure.
pub(crate) const AUDIO_BUFFER_SIZE_BYTES: i64 = 120 * 48_000 * 2 * 2;

/// Output configuration passed from the server builder.
#[derive(Debug, Clone)]
pub(crate) struct OutputConfig {
    /// Target sample rate, or None for source native rate.
    pub(crate) sample_rate: Option<u32>,
    /// Maximum output channels, or None to pass through.
    pub(crate) max_channels: Option<u8>,
    /// Receiver output latency advertised as RTSP `Audio-Latency` (source samples).
    pub(crate) latency_samples: u32,
}

/// `true` when 24-bit sequence `a` is strictly before `b` (modulo 2^24).
const fn seq_before(a: u32, b: u32) -> bool {
    a.wrapping_sub(b) & SEQ_MASK >= SEQ_HALF
}

/// Extends 32-bit RTP timestamps to a 64-bit timeline, counting rollovers forward.
#[derive(Debug, Default, Clone, Copy)]
struct RtpExtender {
    /// Newest extended timestamp seen.
    last: Option<u64>,
}

impl RtpExtender {
    /// Base offset so timestamps slightly before the first one do not underflow.
    const BASE: u64 = 1 << 32;

    fn peek(self, ts: u32) -> u64 {
        match self.last {
            None => Self::BASE + u64::from(ts),
            Some(last) => {
                let delta = i64::from(ts.wrapping_sub(last as u32) as i32);
                last.saturating_add_signed(delta)
            }
        }
    }

    fn extend(&mut self, ts: u32) -> u64 {
        let ext = self.peek(ts);
        if self.last.is_none_or(|last| ext > last) {
            self.last = Some(ext);
        }
        ext
    }
}

/// When the sender wants the anchor sample presented.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AnchorTime {
    /// Already mapped to a local monotonic instant through a locked PTP timeline.
    Local(u64),
    /// Network time the PTP follower could not map (unlocked fallback applies).
    Network {
        /// `networkTimeTimelineID`.
        timeline_id: u64,
        /// Network time in nanoseconds.
        ns: u64,
    },
}

/// Parsed `SETRATEANCHORTIME` request.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SetRateRequest {
    /// `rate` as sent (only 0 and 1 are valid).
    pub(crate) rate: u64,
    /// `rtpTime` plus the anchor time; required when `rate` is 1.
    pub(crate) anchor: Option<(u32, AnchorTime)>,
}

/// Parsed `FLUSHBUFFERED` request.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FlushRequest {
    /// `flushFromSeq`; absent selects flush-all.
    pub(crate) from_seq: Option<u32>,
    /// `flushFromTS`; a range flush needs it together with `from_seq`.
    pub(crate) from_ts: Option<u32>,
    /// `flushUntilSeq` (exclusive bound).
    pub(crate) until_seq: u32,
}

/// Rejected playout request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlayoutError {
    /// `rate` other than 0 or 1.
    InvalidRate(u64),
    /// Rate 1 without `rtpTime` / network time.
    MissingAnchor,
}

impl PlayoutError {
    /// Status code for the response plist.
    pub(crate) const fn status(self) -> i64 {
        match self {
            Self::InvalidRate(_) | Self::MissingAnchor => STATUS_PARAM_ERROR,
        }
    }
}

/// One incoming-packet drop rule armed by FLUSHBUFFERED.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FlushRule {
    /// Inclusive lower bound (range flush); `None` drops everything before `until`.
    from: Option<u32>,
    /// Exclusive upper bound.
    until: u32,
}

impl FlushRule {
    fn drops(self, seq: u32) -> bool {
        seq_before(seq, self.until) && self.from.is_none_or(|from| !seq_before(seq, from))
    }

    fn retired_by(self, seq: u32) -> bool {
        !seq_before(seq, self.until.wrapping_add(FLUSH_RETIRE_SPAN) & SEQ_MASK)
    }
}

#[derive(Debug)]
struct Packet {
    ssrc: AudioSsrc,
    /// Decrypted AAC access unit.
    payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
struct Anchor {
    rtp: u64,
    local_ns: i128,
}

#[derive(Debug, Clone, Copy)]
struct FallbackPin {
    timeline_id: u64,
    /// `network_ns - local_ns` for anchors on this timeline.
    offset_ns: i128,
}

/// A packet due for the host.
#[derive(Debug)]
pub(crate) struct Released {
    /// RTP timestamp (32-bit wire value).
    pub(crate) rtp: u32,
    ssrc: AudioSsrc,
    payload: Vec<u8>,
    /// First packet after an anchor start: fade in.
    pub(crate) fade_in: bool,
}

/// What the delivery thread should do next.
#[derive(Debug)]
pub(crate) enum Poll {
    /// Hand this packet to the host now.
    Release(Released),
    /// Nothing due before this local instant (ns).
    Wait(u64),
    /// Not playing, or nothing buffered.
    Idle,
}

/// Buffered playout timeline: queue, anchor, flush rules. Pure state; callers pass `now`.
#[derive(Debug)]
pub(crate) struct Playout {
    queue: BTreeMap<u64, Packet>,
    rtp: RtpExtender,
    /// Present only while rate is 1.
    anchor: Option<Anchor>,
    source_rate: u32,
    latency_samples: u32,
    flush_rules: Vec<FlushRule>,
    fallback_pin: Option<FallbackPin>,
    fallback_warned: bool,
    fade_next: bool,
}

impl Playout {
    pub(crate) fn new(latency_samples: u32) -> Self {
        Self {
            queue: BTreeMap::new(),
            rtp: RtpExtender::default(),
            anchor: None,
            source_rate: DEFAULT_SOURCE_RATE,
            latency_samples,
            flush_rules: Vec::new(),
            fallback_pin: None,
            fallback_warned: false,
            fade_next: false,
        }
    }

    fn samples_to_ns(&self, samples: i128) -> i128 {
        samples * NS_PER_SEC / i128::from(self.source_rate.max(1))
    }

    fn latency_ns(&self) -> i128 {
        self.samples_to_ns(i128::from(self.latency_samples))
    }

    fn presentation_ns(&self, anchor: Anchor, rtp: u64) -> i128 {
        anchor.local_ns + self.samples_to_ns(i128::from(rtp) - i128::from(anchor.rtp))
    }

    /// Queued audio duration (first packet start to last packet end).
    fn buffered_ns(&self) -> i128 {
        match (self.queue.first_key_value(), self.queue.last_key_value()) {
            (Some((&first, _)), Some((&last, _))) => {
                self.samples_to_ns(i128::from(last) - i128::from(first) + AAC_FRAME_SAMPLES)
            }
            _ => 0,
        }
    }

    /// At the high-water mark: the reader must stop reading.
    pub(crate) fn is_full(&self) -> bool {
        self.buffered_ns() >= HIGH_WATER_NS
    }

    /// Below the resume threshold: a suspended reader may continue.
    pub(crate) fn has_room(&self) -> bool {
        self.buffered_ns() < RESUME_BELOW_NS
    }

    /// RTP of the earliest buffered sample (`firstAvailableSampleTS`).
    pub(crate) fn first_available(&self) -> Option<u32> {
        self.queue.first_key_value().map(|(&rtp, _)| rtp as u32)
    }

    /// Apply the FLUSHBUFFERED drop rules to an incoming sequence number.
    pub(crate) fn admit(&mut self, seq: u32) -> bool {
        let seq = seq & SEQ_MASK;
        let mut dropped = false;
        self.flush_rules.retain(|rule| {
            dropped |= rule.drops(seq);
            !rule.retired_by(seq)
        });
        !dropped
    }

    fn insert(&mut self, rtp: u32, ssrc: AudioSsrc, payload: Vec<u8>) {
        if ssrc != AudioSsrc::None {
            self.source_rate = ssrc.sample_rate();
        }
        let key = self.rtp.extend(rtp);
        self.queue.insert(key, Packet { ssrc, payload });
    }

    /// Apply SETRATEANCHORTIME. Returns `firstAvailableSampleTS`.
    pub(crate) fn set_rate(&mut self, request: SetRateRequest, now_ns: u64) -> Result<Option<u32>, PlayoutError> {
        match request.rate {
            0 => {
                self.anchor = None;
                self.fade_next = false;
                info!(buffered_ms = self.buffered_ns() / 1_000_000, "Playout stopped (rate 0)");
            }
            1 => {
                let (rtp, time) = request.anchor.ok_or(PlayoutError::MissingAnchor)?;
                let rtp = self.rtp.extend(rtp);
                let local_ns = self.resolve_anchor(time, now_ns);
                self.anchor = Some(Anchor { rtp, local_ns });
                self.fade_next = true;
                info!(
                    anchor_rtp = rtp as u32,
                    lead_ms = (local_ns - i128::from(now_ns)) / 1_000_000,
                    buffered_ms = self.buffered_ns() / 1_000_000,
                    "Playout anchored (rate 1)"
                );
            }
            other => return Err(PlayoutError::InvalidRate(other)),
        }
        Ok(self.first_available())
    }

    fn resolve_anchor(&mut self, time: AnchorTime, now_ns: u64) -> i128 {
        let (timeline_id, ns) = match time {
            AnchorTime::Local(local) => return i128::from(local),
            AnchorTime::Network { timeline_id, ns } => (timeline_id, ns),
        };
        // Release-on-receipt target for the anchor sample.
        let target = i128::from(now_ns) + self.latency_ns();
        if let Some(pin) = self.fallback_pin
            && pin.timeline_id == timeline_id
        {
            let mapped = i128::from(ns) - pin.offset_ns;
            if (mapped - target).abs() <= FALLBACK_MAX_SKEW_NS {
                return mapped;
            }
        }
        self.fallback_pin = Some(FallbackPin {
            timeline_id,
            offset_ns: i128::from(ns) - target,
        });
        if self.fallback_warned {
            debug!(timeline_id, "Unlocked anchor fallback re-pinned");
        } else {
            self.fallback_warned = true;
            warn!(
                timeline_id = format_args!("{timeline_id:#018x}"),
                "PTP not locked on the anchor timeline; anchoring to receipt time plus output latency"
            );
        }
        target
    }

    /// Apply FLUSHBUFFERED.
    pub(crate) fn flush(&mut self, request: FlushRequest) {
        let until = request.until_seq & SEQ_MASK;
        match (request.from_seq, request.from_ts) {
            (Some(from_seq), Some(from_ts)) => {
                let from_key = self.rtp.peek(from_ts);
                let trimmed = self.queue.split_off(&from_key).len();
                // Replacement audio continues the epoch from the cut.
                self.rtp.last = Some(from_key);
                self.flush_rules.push(FlushRule {
                    from: Some(from_seq & SEQ_MASK),
                    until,
                });
                info!(from_seq, from_ts, until_seq = until, trimmed, "FLUSHBUFFERED range");
            }
            _ => {
                let discarded = self.queue.len();
                self.queue.clear();
                self.anchor = None;
                self.fade_next = false;
                self.rtp = RtpExtender::default();
                self.flush_rules.clear();
                self.flush_rules.push(FlushRule { from: None, until });
                info!(until_seq = until, discarded, "FLUSHBUFFERED all");
            }
        }
    }

    /// Next packet due at `now_ns`, pruning packets already behind the playhead.
    pub(crate) fn poll(&mut self, now_ns: u64) -> Poll {
        let Some(anchor) = self.anchor else {
            return Poll::Idle;
        };
        let now = i128::from(now_ns);
        let latency = self.latency_ns();
        let mut pruned = 0usize;
        let poll = loop {
            let Some((&rtp, _)) = self.queue.first_key_value() else {
                break Poll::Idle;
            };
            let end = self.presentation_ns(anchor, rtp.saturating_add(AAC_FRAME_SAMPLES as u64));
            if end <= now {
                self.queue.pop_first();
                pruned += 1;
                continue;
            }
            let release = self.presentation_ns(anchor, rtp) - latency;
            if release > now {
                break Poll::Wait(u64::try_from(release).unwrap_or(u64::MAX));
            }
            let Some((_, packet)) = self.queue.pop_first() else {
                break Poll::Idle;
            };
            break Poll::Release(Released {
                rtp: rtp as u32,
                ssrc: packet.ssrc,
                payload: packet.payload,
                fade_in: std::mem::take(&mut self.fade_next),
            });
        };
        if pruned > 0 {
            debug!(pruned, "Pruned packets behind the playhead");
        }
        poll
    }
}

/// Host notifications, delivered in order on the delivery thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostEvent {
    Rate(u32),
    Flush,
}

#[derive(Debug)]
struct State {
    playout: Playout,
    stopped: bool,
    events: Vec<HostEvent>,
}

struct Shared {
    state: Mutex<State>,
    /// Wakes the delivery thread (new packet, command, stop).
    wake: Condvar,
    /// Wakes a reader suspended at the high-water mark.
    room: Notify,
}

impl Shared {
    fn new(latency_samples: u32) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                playout: Playout::new(latency_samples),
                stopped: false,
                events: Vec::with_capacity(4),
            }),
            wake: Condvar::new(),
            room: Notify::new(),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Synchronous control handle for one buffered stream (RTSP handlers, hard stop).
#[derive(Clone)]
pub(crate) struct PlayoutHandle {
    shared: Arc<Shared>,
}

impl PlayoutHandle {
    /// Apply SETRATEANCHORTIME; returns `firstAvailableSampleTS`.
    pub(crate) fn set_rate(&self, request: SetRateRequest) -> Result<Option<u32>, PlayoutError> {
        let mut state = self.shared.lock();
        let reply = state.playout.set_rate(request, mono_now_ns())?;
        if let Ok(rate) = u32::try_from(request.rate) {
            state.events.push(HostEvent::Rate(rate));
        }
        drop(state);
        self.shared.wake.notify_one();
        Ok(reply)
    }

    /// Apply FLUSHBUFFERED.
    pub(crate) fn flush(&self, request: FlushRequest) {
        let mut state = self.shared.lock();
        state.playout.flush(request);
        state.events.push(HostEvent::Flush);
        drop(state);
        self.shared.wake.notify_one();
        self.shared.room.notify_one();
    }

    /// Stop playout and wake every waiter. Abort-safe: call before aborting the reader task.
    pub(crate) fn stop(&self) {
        let mut state = self.shared.lock();
        state.stopped = true;
        state.playout.queue.clear();
        drop(state);
        self.shared.wake.notify_all();
        self.shared.room.notify_one();
    }
}

/// Stops playout when the reader task exits or is aborted.
struct ReceiveCleanup {
    handle: PlayoutHandle,
}

impl Drop for ReceiveCleanup {
    fn drop(&mut self) {
        self.handle.stop();
    }
}

/// TCP listener for buffered audio. Binds a port and spawns the processing pipeline.
pub(crate) struct BufferedAudioProcessor {
    /// TCP listener waiting for the sender to connect.
    pub(crate) listener: TcpListener,
}

impl BufferedAudioProcessor {
    /// Start the reader task and delivery thread.
    ///
    /// Returns the control handle and the reader task abort handle.
    pub(crate) fn start(
        self,
        shk: [u8; 32],
        output_config: OutputConfig,
        handler: Arc<dyn AudioHandler>,
    ) -> (PlayoutHandle, tokio::task::AbortHandle) {
        let shared = Shared::new(output_config.latency_samples);
        let handle = PlayoutHandle {
            shared: Arc::clone(&shared),
        };

        let delivery_shared = Arc::clone(&shared);
        let delivery_handler = Arc::clone(&handler);
        let spawned = std::thread::Builder::new()
            .name("ap2-buffered-delivery".into())
            .spawn(move || delivery_loop(&delivery_shared, Output::new(delivery_handler, output_config)));
        if let Err(err) = spawned {
            warn!(error = %err, "Buffered audio delivery thread spawn failed");
            handler.on_error(&ShairplayError::Network(NetworkError::Io(err)));
            handle.stop();
        }

        let cleanup_handle = handle.clone();
        let recv = tokio::spawn(async move {
            let _cleanup = ReceiveCleanup { handle: cleanup_handle };
            let (stream, addr) = match self.listener.accept().await {
                Ok(accepted) => accepted,
                Err(err) => {
                    warn!("Buffered audio accept failed: {err}");
                    handler.on_error(&ShairplayError::Network(NetworkError::Io(err)));
                    return;
                }
            };
            info!(%addr, "Buffered audio client connected");
            receive_loop(stream, &shk, &shared).await;
        });
        (handle, recv.abort_handle())
    }
}

/// Wait until the queue is below the resume threshold. Returns `false` once stopped.
async fn wait_for_room(shared: &Shared) -> bool {
    let mut suspended = false;
    loop {
        {
            let state = shared.lock();
            if state.stopped {
                return false;
            }
            let ready = if suspended {
                state.playout.has_room()
            } else {
                !state.playout.is_full()
            };
            if ready {
                if suspended {
                    debug!("Resuming buffered audio network reads");
                }
                return true;
            }
        }
        if !suspended {
            debug!("Suspending buffered audio network reads (120 s buffered)");
            suspended = true;
        }
        shared.room.notified().await;
    }
}

/// TCP reader: length-prefixed RTP packets, flush rules, decrypt, queue.
async fn receive_loop(mut stream: TcpStream, shk: &[u8; 32], shared: &Shared) {
    use chacha20poly1305::{ChaCha20Poly1305, KeyInit};

    let cipher = ChaCha20Poly1305::new(shk.into());
    let mut len_buf = [0u8; 2];
    let mut packet = Vec::with_capacity(2048);

    loop {
        // Back-pressure: stop reading at the high-water mark so TCP flow control paces the sender.
        if !wait_for_room(shared).await {
            break;
        }
        if stream.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        // The 2-byte big-endian length includes itself.
        let Some(body_len) = usize::from(u16::from_be_bytes(len_buf)).checked_sub(2) else {
            break;
        };
        packet.resize(body_len, 0);
        if stream.read_exact(&mut packet).await.is_err() {
            break;
        }
        if packet.len() <= RTP_HEADER_LEN + NONCE_TRAIL_LEN {
            continue;
        }
        let word = |at: usize| {
            packet
                .get(at..at + 4)
                .and_then(|b| <[u8; 4]>::try_from(b).ok())
                .map_or(0, u32::from_be_bytes)
        };
        let seq = word(0) & SEQ_MASK;
        let rtp = word(4);
        let ssrc = AudioSsrc::from_u32(word(8));

        if !shared.lock().playout.admit(seq) {
            continue;
        }
        let Some(payload) = decrypt_rtp_chacha(&cipher, &packet) else {
            debug!(seq, "Buffered audio decrypt failed");
            continue;
        };
        let mut state = shared.lock();
        if state.stopped {
            break;
        }
        state.playout.insert(rtp, ssrc, payload);
        drop(state);
        shared.wake.notify_one();
    }
    debug!("Buffered audio receive loop ended");
}

/// 30 ms linear fade-in at anchor starts.
#[derive(Debug, Default)]
struct Fade {
    total_frames: u64,
    done_frames: u64,
}

impl Fade {
    fn start(&mut self, sample_rate: u32) {
        self.total_frames = u64::from(sample_rate) * FADE_IN_NS / 1_000_000_000;
        self.done_frames = 0;
    }

    fn apply(&mut self, samples: &mut [f32], channels: usize) {
        if self.done_frames >= self.total_frames || channels == 0 {
            return;
        }
        for frame in samples.chunks_exact_mut(channels) {
            if self.done_frames >= self.total_frames {
                break;
            }
            let gain = self.done_frames as f32 / self.total_frames as f32;
            for sample in frame {
                *sample *= gain;
            }
            self.done_frames += 1;
        }
    }
}

/// Delivery-side decode and host session.
struct Output {
    handler: Arc<dyn AudioHandler>,
    config: OutputConfig,
    session: Option<Box<dyn AudioSession>>,
    format: Option<(u32, u8)>,
    decoder: Option<AacDecoder>,
    decoder_ssrc: AudioSsrc,
    resampler: Option<crate::codec::resample::StreamResampler>,
    source_channels: u8,
    output_channels: u8,
    output_rate: u32,
    fade: Fade,
}

impl Output {
    fn new(handler: Arc<dyn AudioHandler>, config: OutputConfig) -> Self {
        Self {
            handler,
            config,
            session: None,
            format: None,
            decoder: None,
            decoder_ssrc: AudioSsrc::None,
            resampler: None,
            source_channels: 2,
            output_channels: 2,
            output_rate: DEFAULT_SOURCE_RATE,
            fade: Fade::default(),
        }
    }

    fn host_event(&mut self, event: HostEvent) {
        let Some(session) = self.session.as_mut() else {
            return;
        };
        match event {
            HostEvent::Rate(rate) => session.on_rate(rate),
            HostEvent::Flush => session.on_flush(),
        }
    }

    fn reconfigure(&mut self, ssrc: AudioSsrc) {
        self.decoder_ssrc = ssrc;
        let src_rate = ssrc.sample_rate();
        let src_channels = ssrc.channels();
        info!(?ssrc, src_rate, src_channels, "Buffered audio format detected");
        self.decoder = match AacDecoder::new(src_rate, src_channels) {
            Ok(decoder) => Some(decoder),
            Err(err) => {
                warn!(?ssrc, "AAC decoder init failed: {err}");
                self.handler
                    .on_error(&ShairplayError::Codec(CodecError::UnsupportedFormat(format!(
                        "AAC decoder init failed (ssrc={ssrc:?}, sample_rate={src_rate}, channels={src_channels})"
                    ))));
                None
            }
        };
        let target_rate = self.config.sample_rate.unwrap_or(src_rate);
        let target_channels = self
            .config
            .max_channels
            .map_or(src_channels, |max| src_channels.min(max));
        self.resampler =
            crate::codec::resample::StreamResampler::new(src_rate, target_rate, usize::from(target_channels));
        self.source_channels = src_channels;
        self.output_channels = target_channels;
        self.output_rate = target_rate;
        if self.format != Some((target_rate, target_channels)) {
            // Drop the old session before audio_init so its Drop cannot clear the new ring.
            drop(self.session.take());
            let format = AudioFormat {
                codec: AudioCodec::Pcm,
                bits: 32,
                channels: target_channels,
                sample_rate: target_rate,
            };
            info!(?format, "Audio session initialized");
            self.session = Some(self.handler.audio_init(format));
            self.format = Some((target_rate, target_channels));
        }
    }

    fn play(&mut self, packet: Released) {
        if packet.ssrc != AudioSsrc::None && packet.ssrc != self.decoder_ssrc {
            self.reconfigure(packet.ssrc);
        }
        let Some(decoder) = self.decoder.as_mut() else {
            return;
        };
        let Some(pcm) = decoder.decode(&packet.payload) else {
            debug!(rtp = packet.rtp, "AAC decode failed");
            return;
        };
        let samples: Vec<f32> = pcm.as_chunks::<4>().0.iter().map(|c| f32::from_le_bytes(*c)).collect();
        let mut samples = crate::codec::resample::mixdown_and_resample(
            samples,
            self.source_channels,
            self.output_channels,
            &mut self.resampler,
        );
        if packet.fade_in {
            self.fade.start(self.output_rate);
        }
        self.fade.apply(&mut samples, usize::from(self.output_channels));
        if let Some(session) = self.session.as_mut() {
            session.audio_process(&samples);
        }
    }
}

/// Delivery thread: hands packets to the host exactly when the anchor timeline makes them due.
fn delivery_loop(shared: &Shared, mut output: Output) {
    let mut events = Vec::with_capacity(4);
    let mut state = shared.lock();
    loop {
        if state.stopped {
            break;
        }
        if !state.events.is_empty() {
            std::mem::swap(&mut state.events, &mut events);
            drop(state);
            for event in events.drain(..) {
                output.host_event(event);
            }
            state = shared.lock();
            continue;
        }
        let now = mono_now_ns();
        match state.playout.poll(now) {
            Poll::Release(packet) => {
                let room = state.playout.has_room();
                drop(state);
                if room {
                    shared.room.notify_one();
                }
                output.play(packet);
                state = shared.lock();
            }
            Poll::Wait(until) => {
                let timeout = Duration::from_nanos(until.saturating_sub(now));
                state = shared
                    .wake
                    .wait_timeout(state, timeout)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
            }
            Poll::Idle => {
                state = shared.wake.wait(state).unwrap_or_else(PoisonError::into_inner);
            }
        }
    }
    drop(state);
    drop(output);
    info!("Buffered audio delivery ended");
}

#[cfg(test)]
mod tests {
    use super::*;

    const SR: u32 = 44_100;
    const FRAME: u32 = 1024;
    /// 2 s at 44.1 kHz.
    const LATENCY: u32 = 2 * SR;
    const SSRC: AudioSsrc = AudioSsrc::Aac44100F24Stereo;
    const S: u64 = 1_000_000_000;

    fn frame_ns(frames: u64) -> u64 {
        frames * S / u64::from(SR)
    }

    fn fill(p: &mut Playout, first_seq: u32, first_rtp: u32, count: u32) {
        for i in 0..count {
            let seq = first_seq.wrapping_add(i) & SEQ_MASK;
            if p.admit(seq) {
                p.insert(first_rtp.wrapping_add(i * FRAME), SSRC, vec![0; 4]);
            }
        }
    }

    fn play(rtp: u32, local_ns: u64) -> SetRateRequest {
        SetRateRequest {
            rate: 1,
            anchor: Some((rtp, AnchorTime::Local(local_ns))),
        }
    }

    fn release_rtp(poll: Poll) -> u32 {
        match poll {
            Poll::Release(r) => r.rtp,
            other => panic!("expected release, got {other:?}"),
        }
    }

    fn queued_rtps(p: &Playout) -> Vec<u32> {
        p.queue.keys().map(|&k| k as u32).collect()
    }

    /// A packet reaches the host exactly at presentation minus the output latency, and the
    /// following one a frame later.
    #[test]
    fn anchored_release_waits_until_presentation_minus_latency() {
        let mut p = Playout::new(LATENCY);
        fill(&mut p, 1, 5_000, 4);
        assert!(matches!(p.poll(0), Poll::Idle), "no anchor, nothing due");

        let anchor_at = 10 * S;
        assert_eq!(p.set_rate(play(5_000, anchor_at), S).unwrap(), Some(5_000));
        let first_release = anchor_at - 2 * S;
        match p.poll(first_release - 1) {
            Poll::Wait(until) => assert_eq!(until, first_release),
            other => panic!("expected wait, got {other:?}"),
        }
        let Poll::Release(first) = p.poll(first_release) else {
            panic!("anchor packet due at presentation - latency");
        };
        assert_eq!(first.rtp, 5_000);
        assert!(first.fade_in, "anchor start fades in");
        let second_release = first_release + frame_ns(u64::from(FRAME));
        assert!(matches!(p.poll(second_release - 1_000), Poll::Wait(_)));
        let Poll::Release(second) = p.poll(second_release) else {
            panic!("second packet due one frame later");
        };
        assert_eq!(second.rtp, 5_000 + FRAME);
        assert!(!second.fade_in);
    }

    /// Anchor already in the past: packets entirely behind the playhead are pruned and playback
    /// joins at the packet that contains "now".
    #[test]
    fn anchor_in_past_prunes_and_joins_mid_stream() {
        let mut p = Playout::new(LATENCY);
        fill(&mut p, 1, 0, 100);
        let now = 20 * S;
        // Anchor rtp 0 presented 1 s ago: ~43 packets are fully behind the playhead.
        p.set_rate(play(0, now - S), now).unwrap();
        let rtp = release_rtp(p.poll(now));
        let behind = u64::from(rtp) * S / u64::from(SR);
        assert!(
            behind <= S && behind + frame_ns(u64::from(FRAME)) > S,
            "joined at rtp {rtp}"
        );
        assert_eq!(rtp, 43 * FRAME);
    }

    /// Rate 0 stops release, keeps the buffer and clears the anchor; a new anchor resumes from
    /// the kept audio on its own timeline.
    #[test]
    fn pause_keeps_buffer_and_resume_uses_new_anchor() {
        let mut p = Playout::new(LATENCY);
        fill(&mut p, 1, 0, 20);
        p.set_rate(play(0, 3 * S), S).unwrap();
        assert_eq!(release_rtp(p.poll(S)), 0);

        let paused = p.set_rate(SetRateRequest { rate: 0, anchor: None }, 2 * S).unwrap();
        assert_eq!(
            paused,
            Some(FRAME),
            "firstAvailableSampleTS is the earliest kept sample"
        );
        assert!(matches!(p.poll(100 * S), Poll::Idle), "nothing released while paused");
        assert_eq!(p.queue.len(), 19, "buffer kept across pause");

        p.set_rate(play(FRAME, 60 * S), 50 * S).unwrap();
        match p.poll(57 * S) {
            Poll::Wait(until) => assert_eq!(until, 58 * S),
            other => panic!("expected wait for the new anchor, got {other:?}"),
        }
        let Poll::Release(resumed) = p.poll(58 * S) else {
            panic!("resume at new anchor minus latency");
        };
        assert_eq!(resumed.rtp, FRAME);
        assert!(resumed.fade_in);
    }

    #[test]
    fn rate_must_be_zero_or_one_and_play_needs_anchor() {
        let mut p = Playout::new(LATENCY);
        let err = p.set_rate(SetRateRequest { rate: 2, anchor: None }, 0).unwrap_err();
        assert_eq!(err, PlayoutError::InvalidRate(2));
        assert_eq!(err.status(), STATUS_PARAM_ERROR);
        let err = p.set_rate(SetRateRequest { rate: 1, anchor: None }, 0).unwrap_err();
        assert_eq!(err, PlayoutError::MissingAnchor);
    }

    /// The log scenario: the sender bursts five minutes ahead while playing. The reader stops
    /// at the 120 s high-water mark (TCP back-pressure) instead of dropping; the host receives
    /// audio no faster than real time plus the output latency, so a host ring sized for latency
    /// plus Cast start never overflows, and every packet is eventually played.
    #[test]
    fn far_ahead_burst_backpressures_and_never_floods_the_host() {
        let total_packets = 300 * SR / FRAME; // ~5 minutes
        let mut p = Playout::new(LATENCY);
        let start = 5 * S;
        p.set_rate(play(0, start + 2 * S), start).unwrap();

        // Host ring model: the Cast consumer starts pulling 3 s after the first release (LOAD)
        // and then drains in real time; ring capacity as configured product-side (10 s).
        let ring_capacity_frames = 10 * u64::from(SR);
        let consumer_delay = 3 * S;
        let mut sent = 0u32;
        let mut released_frames = 0u64;
        let mut first_release: Option<u64> = None;
        let mut max_occupancy = 0u64;
        let mut suspended_reads = 0u32;
        let step = 5_000_000u64; // 5 ms
        let mut now = start;
        while released_frames < u64::from(total_packets) * u64::from(FRAME) {
            // Reader: as fast as the socket allows unless at high water.
            while sent < total_packets {
                if p.is_full() {
                    suspended_reads += 1;
                    break;
                }
                fill(&mut p, sent + 1, sent * FRAME, 1);
                sent += 1;
            }
            assert!(p.buffered_ns() <= HIGH_WATER_NS + i128::from(frame_ns(u64::from(FRAME))));
            while let Poll::Release(_) = p.poll(now) {
                first_release.get_or_insert(now);
                released_frames += u64::from(FRAME);
            }
            let consumed = first_release.map_or(0, |t0| now.saturating_sub(t0 + consumer_delay) * u64::from(SR) / S);
            let occupancy = released_frames.saturating_sub(consumed);
            max_occupancy = max_occupancy.max(occupancy);
            now += step;
            assert!(now < start + 400 * S, "playback stalled");
        }
        assert!(suspended_reads > 0, "reader hit the high-water mark");
        assert_eq!(sent, total_packets, "no packet dropped for being early");
        assert!(
            max_occupancy <= u64::from(LATENCY) + 3 * u64::from(SR) + 2 * u64::from(FRAME),
            "host received faster than real time: max occupancy {max_occupancy} frames"
        );
        assert!(max_occupancy < ring_capacity_frames, "ring overflow");
        // Real-time pacing: the whole program took its own duration (plus the anchor lead).
        let elapsed = now - start;
        let program = u64::from(total_packets) * frame_ns(u64::from(FRAME));
        let elapsed_with_last = elapsed + frame_ns(u64::from(FRAME));
        assert!(
            elapsed_with_last >= program && elapsed <= program + S,
            "elapsed {elapsed} vs {program}"
        );
    }

    /// Flush-all stops, discards everything, resets anchor and RTP epoch, drops seq before
    /// until (exclusive) across the 24-bit wrap, and retires at until + 20.
    #[test]
    fn flush_all_stops_and_drops_before_until_across_wrap() {
        let mut p = Playout::new(LATENCY);
        fill(&mut p, 0xFF_FFE0, 1_000, 8);
        p.set_rate(play(1_000, 3 * S), S).unwrap();
        let until = 0xFF_FFF8;
        p.flush(FlushRequest {
            from_seq: None,
            from_ts: Some(0),
            until_seq: until,
        });
        assert!(p.queue.is_empty());
        assert!(matches!(p.poll(10 * S), Poll::Idle), "stopped until a new anchor");

        assert!(!p.admit(0xFF_FFF7), "before until: dropped");
        assert!(!p.admit(0xFF_FFE0), "old straggler: dropped");
        assert!(p.admit(until), "until itself is kept");
        assert!(p.admit(0x00_0005), "wrapped past until: kept");
        assert!(!p.admit(0xFF_FFF0), "rule still alive below until + 20");
        assert!(p.admit((until + 20) & SEQ_MASK), "until + 20 retires the rule");
        assert!(p.admit(0xFF_FFF0), "retired: no longer dropped");

        // New epoch: RTP restarts far away and plays from its own anchor.
        fill(&mut p, until, 3_000_000_000, 2);
        assert_eq!(queued_rtps(&p), vec![3_000_000_000, 3_000_000_000 + FRAME]);
        p.set_rate(play(3_000_000_000, 30 * S), 20 * S).unwrap();
        assert_eq!(release_rtp(p.poll(28 * S)), 3_000_000_000);
    }

    /// Range flush keeps playing, trims from flushFromTS to the end, drops [from, until), keeps
    /// until, and continues the RTP epoch from the cut. Sequence bounds wrap at 2^24.
    #[test]
    fn range_flush_trims_to_end_and_drops_from_inclusive_until_exclusive() {
        let mut p = Playout::new(LATENCY);
        let base_seq = 0xFF_FFFA;
        fill(&mut p, base_seq, 0, 12);
        p.set_rate(play(0, 3 * S), S).unwrap();
        assert_eq!(release_rtp(p.poll(S)), 0);

        let from_seq = (base_seq + 4) & SEQ_MASK; // rtp 4 * FRAME
        let until_seq = (base_seq + 9) & SEQ_MASK; // wraps to 3
        p.flush(FlushRequest {
            from_seq: Some(from_seq),
            from_ts: Some(4 * FRAME),
            until_seq,
        });
        assert_eq!(
            queued_rtps(&p),
            vec![FRAME, 2 * FRAME, 3 * FRAME],
            "trimmed from fromTS to end"
        );
        assert!(p.anchor.is_some(), "range flush keeps playing");

        assert!(p.admit((base_seq + 3) & SEQ_MASK), "before from: kept");
        assert!(!p.admit(from_seq), "from is inclusive");
        assert!(!p.admit((base_seq + 8) & SEQ_MASK), "inside range across wrap");
        assert!(p.admit(until_seq), "until is exclusive");
        // Replacement audio continues the epoch from the cut.
        p.insert(4 * FRAME, SSRC, vec![0; 4]);
        assert_eq!(queued_rtps(&p).last(), Some(&(4 * FRAME)));
        assert!(p.admit((until_seq + 20) & SEQ_MASK));
        assert!(p.flush_rules.is_empty(), "rule retired at until + 20");
    }

    /// Unlocked fallback: the first anchor on a timeline is released on receipt; a later anchor
    /// on the same timeline keeps the pinned offset (relative timing preserved); an anchor far
    /// from the pin re-pins.
    #[test]
    fn unlocked_anchor_falls_back_to_receipt_and_keeps_relative_timing() {
        let mut p = Playout::new(LATENCY);
        fill(&mut p, 1, 0, 400);
        let timeline_id = 0xABCD;
        let network = |ns: u64| SetRateRequest {
            rate: 1,
            anchor: Some((0, AnchorTime::Network { timeline_id, ns })),
        };
        let receipt = 7 * S;
        p.set_rate(network(1_000_000 * S), receipt).unwrap();
        assert_eq!(release_rtp(p.poll(receipt)), 0, "anchor sample released on receipt");

        // Pause, then resume with an anchor 300 ms later on the sender clock, received 100 ms
        // later locally: the pinned offset keeps the sender's 300 ms.
        p.set_rate(SetRateRequest { rate: 0, anchor: None }, receipt + S)
            .unwrap();
        let resume = SetRateRequest {
            rate: 1,
            anchor: Some((
                FRAME,
                AnchorTime::Network {
                    timeline_id,
                    ns: 1_000_000 * S + 10 * S + 300_000_000,
                },
            )),
        };
        p.set_rate(resume, receipt + 10 * S + 100_000_000).unwrap();
        match p.poll(receipt + 10 * S + 100_000_000) {
            Poll::Wait(until) => assert_eq!(until, receipt + 10 * S + 300_000_000),
            other => panic!("expected the sender-relative wait, got {other:?}"),
        }

        // An anchor 5 s off the pin re-pins to release on receipt.
        let skewed = SetRateRequest {
            rate: 1,
            anchor: Some((
                20 * FRAME,
                AnchorTime::Network {
                    timeline_id,
                    ns: 1_000_000 * S + 30 * S,
                },
            )),
        };
        let at = receipt + 18 * S;
        p.set_rate(skewed, at).unwrap();
        // Packets before the new anchor are within the output latency: released at once, up
        // to the anchor sample; the next one waits a frame.
        let mut last = None;
        while let Poll::Release(r) = p.poll(at) {
            last = Some(r.rtp);
        }
        assert_eq!(last, Some(20 * FRAME));
        match p.poll(at) {
            Poll::Wait(until) => assert_eq!(until, at + frame_ns(u64::from(FRAME))),
            other => panic!("expected wait, got {other:?}"),
        }
    }

    #[test]
    fn rtp_extension_counts_rollover_and_tolerates_reorder() {
        let mut ext = RtpExtender::default();
        let a = ext.extend(u32::MAX - 1023);
        let b = ext.extend(0); // rolled over
        let c = ext.extend(u32::MAX - 2047); // late straggler before the wrap
        assert_eq!(b - a, 1024);
        assert_eq!(a - c, 1024);
        assert_eq!(ext.last, Some(b));
    }

    #[test]
    fn fade_ramps_from_silence_over_30_ms() {
        let mut fade = Fade::default();
        fade.start(1_000); // 30 frames
        let mut samples = vec![1.0f32; 100];
        fade.apply(&mut samples, 2);
        assert_eq!(samples[0], 0.0);
        assert!((samples[2] - 1.0 / 30.0).abs() < 1e-6);
        assert!(samples[58] < 1.0);
        assert_eq!(samples[60], 1.0);
    }

    struct NullHandler;
    impl AudioHandler for NullHandler {
        fn audio_init(&self, _format: AudioFormat) -> Box<dyn AudioSession> {
            struct Null;
            impl AudioSession for Null {
                fn audio_process(&mut self, _samples: &[f32]) {}
            }
            Box::new(Null)
        }
    }

    /// Hard stop from another thread ends a delivery thread parked on a far-future release.
    #[test]
    fn stop_unblocks_parked_delivery_thread() {
        let shared = Shared::new(LATENCY);
        let handle = PlayoutHandle {
            shared: Arc::clone(&shared),
        };
        {
            let mut state = shared.lock();
            fill(&mut state.playout, 1, 0, 4);
            let now = mono_now_ns();
            state.playout.set_rate(play(0, now + 3_600 * S), now).unwrap();
        }
        let thread_shared = Arc::clone(&shared);
        let config = OutputConfig {
            sample_rate: None,
            max_channels: Some(2),
            latency_samples: LATENCY,
        };
        let join =
            std::thread::spawn(move || delivery_loop(&thread_shared, Output::new(Arc::new(NullHandler), config)));
        std::thread::sleep(Duration::from_millis(30));
        assert!(!join.is_finished(), "parked until the far release");
        handle.stop();
        for _ in 0..200 {
            if join.is_finished() {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("delivery thread did not exit after stop");
    }

    /// The reader suspended at high water resumes when delivery frees room, and stop wakes it.
    #[tokio::test]
    async fn reader_waits_at_high_water_until_room_or_stop() {
        let shared = Shared::new(LATENCY);
        {
            let mut state = shared.lock();
            fill(&mut state.playout, 1, 0, 120 * SR / FRAME + 2);
            assert!(state.playout.is_full());
        }
        let waiter_shared = Arc::clone(&shared);
        let waiter = tokio::spawn(async move { wait_for_room(&waiter_shared).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "suspended at high water");
        {
            let mut state = shared.lock();
            for _ in 0..(2 * SR / FRAME) {
                state.playout.queue.pop_first();
            }
            assert!(state.playout.has_room());
        }
        shared.room.notify_one();
        assert!(waiter.await.unwrap(), "resumed with room");

        let handle = PlayoutHandle {
            shared: Arc::clone(&shared),
        };
        {
            let mut state = shared.lock();
            fill(&mut state.playout, 10_000, 0, 120 * SR / FRAME + 2);
        }
        let waiter_shared = Arc::clone(&shared);
        let waiter = tokio::spawn(async move { wait_for_room(&waiter_shared).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        handle.stop();
        assert!(!waiter.await.unwrap(), "stop ends the wait");
    }
}
