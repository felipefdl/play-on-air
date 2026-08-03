//! Realtime ALAC audio receiver (stream type 96).
//!
//! Receives UDP packets with RTP headers, decrypts with ChaCha20-Poly1305,
//! reorders by RTP sequence number (small window; silence for aged gaps),
//! decodes ALAC in order only, resamples/mixes down, and delivers f32 PCM.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tracing::{debug, info, warn};

use chacha20poly1305::{ChaCha20Poly1305, KeyInit};

use crate::error::{NetworkError, ShairplayError};
use crate::raop::audio_pipeline::{NONCE_TRAIL_LEN, RTP_HEADER_LEN, decrypt_rtp_chacha};
use crate::raop::{AudioCodec, AudioFormat, AudioHandler};

#[cfg(feature = "resample")]
use crate::codec::resample::StreamResampler;

/// Reorder window depth in packets (~128 ms at 352 samples/packet, 44.1 kHz).
///
/// Must stay small for realtime latency. Power-of-two so slot indexing is cheap.
const REORDER_WINDOW: usize = 16;

/// How often to emit a rate-limited debug summary of seq stats.
const STATS_LOG_INTERVAL: Duration = Duration::from_secs(5);

/// Output configuration for resampling/mixdown.
pub(crate) struct OutputConfig {
    /// Source sample rate from the stream SETUP.
    pub(crate) source_sample_rate: u32,
    /// Source samples per ALAC frame from the stream SETUP.
    pub(crate) samples_per_frame: u32,
    /// Source channel count.
    pub(crate) channels: u8,
    /// Source bit depth.
    pub(crate) bit_depth: u8,
    /// Target sample rate, or None for source native rate.
    pub(crate) sample_rate: Option<u32>,
    /// Maximum output channels, or None to pass through.
    pub(crate) max_channels: Option<u8>,
}

fn alac_decoder_info(config: &OutputConfig) -> [u8; 48] {
    let mut info = [0u8; 48];
    info[24..28].copy_from_slice(&config.samples_per_frame.to_be_bytes());
    info[29] = config.bit_depth;
    info[30] = 40; // pb
    info[31] = 10; // mb
    info[32] = 14; // kb
    info[33] = config.channels;
    info[34..36].copy_from_slice(&255u16.to_be_bytes());
    info[44..48].copy_from_slice(&config.source_sample_rate.to_be_bytes());
    info
}

/// Compare two RTP sequence numbers with wrapping (handles 16-bit overflow).
/// Returns negative if `s1` is before `s2`, positive if after, zero if equal.
fn seqnum_cmp(s1: u16, s2: u16) -> i16 {
    s1.wrapping_sub(s2) as i16
}

/// Parse the 16-bit RTP sequence number from header bytes 2–3 (big-endian).
fn rtp_seqnum(packet: &[u8]) -> Option<u16> {
    if packet.len() < 4 {
        return None;
    }
    Some(u16::from_be_bytes([packet[2], packet[3]]))
}

/// Cumulative RTP sequence statistics for a realtime session.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RtpSeqStats {
    /// Packets never received and substituted with silence (aged past the window).
    losses: u64,
    /// Packets that arrived ahead of the next expected sequence number.
    reorders: u64,
    /// Duplicate or late (already passed) sequence numbers dropped.
    duplicates: u64,
}

/// One ordered frame from the reorder window: ALAC payload or a lost gap.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OrderedFrame {
    /// Decrypted ALAC payload, ready for in-order decode.
    Payload(Vec<u8>),
    /// Missing packet older than the reorder window — fill with silence.
    Lost,
}

/// Small circular reorder window for realtime type-96 RTP.
///
/// Concepts mirror AP1 [`crate::raop::buffer::RaopBuffer`]: insert by sequence
/// number, deliver strictly in order, drop duplicates, substitute silence for
/// gaps that age out of the window. No retransmit requests (out of scope).
struct RealtimeReorderWindow {
    /// Next sequence number expected for delivery.
    next_seq: u16,
    /// Whether the first packet has established the sequence baseline.
    started: bool,
    /// Payload slots; index = `seqnum % REORDER_WINDOW`. Stores `(seqnum, payload)`.
    slots: Vec<Option<(u16, Vec<u8>)>>,
    stats: RtpSeqStats,
}

impl RealtimeReorderWindow {
    fn new() -> Self {
        Self {
            next_seq: 0,
            started: false,
            slots: (0..REORDER_WINDOW).map(|_| None).collect(),
            stats: RtpSeqStats::default(),
        }
    }

    fn stats(&self) -> RtpSeqStats {
        self.stats
    }

    /// Whether `seq` still fits in the open window starting at `next_seq`.
    fn fits(&self, seq: u16) -> bool {
        seqnum_cmp(seq, self.next_seq.wrapping_add(REORDER_WINDOW as u16)) < 0
    }

    /// Emit the next expected frame (payload if buffered, else silence / loss).
    fn emit_next(&mut self, out: &mut Vec<OrderedFrame>) {
        let idx = self.next_seq as usize % REORDER_WINDOW;
        match self.slots.get_mut(idx).and_then(Option::take) {
            Some((stored_seq, payload)) if stored_seq == self.next_seq => {
                out.push(OrderedFrame::Payload(payload));
            }
            Some(_) | None => {
                out.push(OrderedFrame::Lost);
                self.stats.losses = self.stats.losses.saturating_add(1);
            }
        }
        self.next_seq = self.next_seq.wrapping_add(1);
    }

    /// Insert a decrypted ALAC payload by RTP sequence number.
    ///
    /// Returns zero or more frames ready for ordered delivery (payload or lost).
    fn push(&mut self, seq: u16, payload: Vec<u8>) -> Vec<OrderedFrame> {
        let mut out = Vec::new();

        if !self.started {
            self.next_seq = seq;
            self.started = true;
        }

        // Late or already-delivered/silence-filled: drop as duplicate/redundant.
        if seqnum_cmp(seq, self.next_seq) < 0 {
            self.stats.duplicates = self.stats.duplicates.saturating_add(1);
            return out;
        }

        // Exact duplicate still sitting in the window.
        let idx = seq as usize % REORDER_WINDOW;
        if let Some(Some((stored_seq, _))) = self.slots.get(idx)
            && *stored_seq == seq
        {
            self.stats.duplicates = self.stats.duplicates.saturating_add(1);
            return out;
        }

        // Packet too far ahead: age out gaps with silence until it fits.
        while !self.fits(seq) {
            self.emit_next(&mut out);
        }

        if seq != self.next_seq {
            self.stats.reorders = self.stats.reorders.saturating_add(1);
        }

        if let Some(slot) = self.slots.get_mut(idx) {
            *slot = Some((seq, payload));
        }

        // Drain every contiguous in-order frame now available.
        while let Some(payload) = self.take_next_payload() {
            out.push(OrderedFrame::Payload(payload));
        }

        out
    }

    /// Pop the payload for `next_seq` if present; advance on success.
    fn take_next_payload(&mut self) -> Option<Vec<u8>> {
        let drain_idx = self.next_seq as usize % REORDER_WINDOW;
        let ready = matches!(
            self.slots.get(drain_idx),
            Some(Some((stored_seq, _))) if *stored_seq == self.next_seq
        );
        if !ready {
            return None;
        }
        let payload = self.slots.get_mut(drain_idx).and_then(Option::take).map(|(_, p)| p)?;
        self.next_seq = self.next_seq.wrapping_add(1);
        Some(payload)
    }
}

/// Run the realtime audio receiver loop.
pub(crate) async fn run(socket: UdpSocket, shk: [u8; 32], handler: Arc<dyn AudioHandler>, output_config: OutputConfig) {
    let cipher = ChaCha20Poly1305::new((&shk).into());
    let mut buf = vec![0u8; 4096];
    let mut decoder: Option<crate::codec::alac::AlacDecoder> = None;
    #[cfg(feature = "resample")]
    let mut resampler: Option<StreamResampler> = None;
    let mut session: Option<Box<dyn crate::raop::AudioSession>> = None;
    #[allow(unused_assignments)]
    let mut src_sr: u32 = 44100;
    let mut src_ch: u8 = 2;
    let mut out_ch: u8 = 2;
    let mut frame_samples: usize = 352 * 2;
    let mut reorder = RealtimeReorderWindow::new();
    let mut last_stats_log = Instant::now();

    info!("Realtime ALAC receiver started");

    loop {
        let n = match socket.recv(&mut buf).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                warn!("Realtime audio recv error: {e}");
                handler.on_error(&ShairplayError::Network(NetworkError::Io(e)));
                break;
            }
        };

        let packet = &buf[..n];
        if packet.len() <= RTP_HEADER_LEN + NONCE_TRAIL_LEN {
            continue;
        }

        let Some(seq) = rtp_seqnum(packet) else {
            continue;
        };

        // Lazy init decoder + session on first packet
        if session.is_none() {
            src_sr = output_config.source_sample_rate;
            src_ch = output_config.channels;
            let target_sr = output_config.sample_rate.unwrap_or(src_sr);
            out_ch = output_config.max_channels.map(|m| src_ch.min(m)).unwrap_or(src_ch);
            frame_samples = (output_config.samples_per_frame as usize).saturating_mul(src_ch as usize);

            let mut alac = crate::codec::alac::AlacDecoder::new(output_config.bit_depth as i32, src_ch as i32);
            let decoder_info = alac_decoder_info(&output_config);
            alac.set_info(&decoder_info);
            decoder = Some(alac);
            #[cfg(feature = "resample")]
            if target_sr != src_sr {
                resampler = StreamResampler::new(src_sr, target_sr, out_ch as usize);
            }

            let format = AudioFormat {
                codec: AudioCodec::Pcm,
                bits: 32,
                channels: out_ch,
                sample_rate: output_config.sample_rate.unwrap_or(src_sr),
            };
            info!(?format, "Realtime audio session initialized");
            session = Some(handler.audio_init(format));
        }

        // Decrypt the ChaCha20-Poly1305 RTP frame.
        let Some(alac_data) = decrypt_rtp_chacha(&cipher, packet) else {
            debug!("Realtime audio decrypt failed");
            continue;
        };

        // Reorder / de-dupe / silence-fill aged gaps; feed ALAC only in order.
        let ordered = reorder.push(seq, alac_data);
        for frame in ordered {
            let mut samples = match frame {
                OrderedFrame::Payload(alac) => {
                    let Some(decoded) = decoder.as_mut().and_then(|d| d.decode_frame_f32(&alac)) else {
                        continue;
                    };
                    decoded
                }
                OrderedFrame::Lost => vec![0.0f32; frame_samples],
            };

            // Mix down + resample to the output format.
            #[cfg(feature = "resample")]
            {
                samples = crate::codec::resample::mixdown_and_resample(samples, src_ch, out_ch, &mut resampler);
            }

            if let Some(ref mut sess) = session {
                sess.audio_process(&samples);
            }
        }

        if last_stats_log.elapsed() >= STATS_LOG_INTERVAL {
            let s = reorder.stats();
            debug!(
                losses = s.losses,
                reorders = s.reorders,
                duplicates = s.duplicates,
                "Realtime RTP sequence stats"
            );
            last_stats_log = Instant::now();
        }
    }

    let s = reorder.stats();
    info!(
        losses = s.losses,
        reorders = s.reorders,
        duplicates = s.duplicates,
        "Realtime ALAC receiver ended"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alac_decoder_info_uses_realtime_setup_values() {
        let info = alac_decoder_info(&OutputConfig {
            source_sample_rate: 48_000,
            samples_per_frame: 352,
            channels: 2,
            bit_depth: 16,
            sample_rate: None,
            max_channels: None,
        });

        assert_eq!(u32::from_be_bytes(info[24..28].try_into().unwrap()), 352);
        assert_eq!(info[29], 16);
        assert_eq!(info[30], 40);
        assert_eq!(info[31], 10);
        assert_eq!(info[32], 14);
        assert_eq!(info[33], 2);
        assert_eq!(u16::from_be_bytes(info[34..36].try_into().unwrap()), 255);
        assert_eq!(u32::from_be_bytes(info[44..48].try_into().unwrap()), 48_000);
    }

    #[test]
    fn rtp_seqnum_reads_header_bytes() {
        let mut pkt = [0u8; 12];
        pkt[2] = 0x12;
        pkt[3] = 0x34;
        assert_eq!(rtp_seqnum(&pkt), Some(0x1234));
        assert_eq!(rtp_seqnum(&[0x80, 0x60]), None);
    }

    #[test]
    fn reorder_in_order_delivers_immediately() {
        let mut w = RealtimeReorderWindow::new();
        assert_eq!(w.push(10, vec![1]), vec![OrderedFrame::Payload(vec![1])]);
        assert_eq!(w.push(11, vec![2]), vec![OrderedFrame::Payload(vec![2])]);
        assert_eq!(w.push(12, vec![3]), vec![OrderedFrame::Payload(vec![3])]);
        assert_eq!(w.stats(), RtpSeqStats::default());
    }

    #[test]
    fn reorder_swapped_pair_preserves_order() {
        let mut w = RealtimeReorderWindow::new();
        assert_eq!(w.push(1, vec![1]), vec![OrderedFrame::Payload(vec![1])]);

        // 3 arrives before 2 — hold, count reorder.
        assert!(w.push(3, vec![3]).is_empty());
        assert_eq!(w.stats().reorders, 1);
        assert_eq!(w.stats().losses, 0);

        // 2 completes the gap — deliver 2 then 3.
        assert_eq!(
            w.push(2, vec![2]),
            vec![OrderedFrame::Payload(vec![2]), OrderedFrame::Payload(vec![3])]
        );
        assert_eq!(w.stats().reorders, 1);
        assert_eq!(w.stats().duplicates, 0);
    }

    #[test]
    fn reorder_duplicate_dropped_no_double_deliver() {
        let mut w = RealtimeReorderWindow::new();
        assert_eq!(w.push(1, vec![1]), vec![OrderedFrame::Payload(vec![1])]);

        // Retransmit of already-delivered seq.
        assert!(w.push(1, vec![0xff]).is_empty());
        assert_eq!(w.stats().duplicates, 1);

        // Hold an out-of-order packet, then duplicate it while buffered.
        assert!(w.push(3, vec![3]).is_empty());
        assert!(w.push(3, vec![0xee]).is_empty());
        assert_eq!(w.stats().duplicates, 2);

        // Only one copy of 3 should emerge after 2 arrives.
        assert_eq!(
            w.push(2, vec![2]),
            vec![OrderedFrame::Payload(vec![2]), OrderedFrame::Payload(vec![3])]
        );
    }

    #[test]
    fn reorder_gap_emits_silence_when_older_than_window() {
        let mut w = RealtimeReorderWindow::new();
        assert_eq!(w.push(0, vec![0]), vec![OrderedFrame::Payload(vec![0])]);

        // Next expected is 1. Packet at 1+WINDOW forces seq 1 to age out as loss.
        let far = 1u16.wrapping_add(REORDER_WINDOW as u16);
        let out = w.push(far, vec![0xaa]);
        assert!(
            out.iter().any(|f| matches!(f, OrderedFrame::Lost)),
            "expected silence fill for aged gap, got {out:?}"
        );
        assert_eq!(w.stats().losses, 1);
        assert_eq!(w.stats().reorders, 1);

        // Small gap still inside the window must not silence-fill yet.
        let mut w2 = RealtimeReorderWindow::new();
        w2.push(0, vec![0]);
        assert!(w2.push(2, vec![2]).is_empty());
        assert_eq!(w2.stats().losses, 0);
        assert_eq!(w2.stats().reorders, 1);

        // Closing the gap delivers in order without silence.
        assert_eq!(
            w2.push(1, vec![1]),
            vec![OrderedFrame::Payload(vec![1]), OrderedFrame::Payload(vec![2])]
        );
        assert_eq!(w2.stats().losses, 0);
    }

    #[test]
    fn reorder_multiple_aged_losses_then_payload() {
        let mut w = RealtimeReorderWindow::new();
        w.push(0, vec![0]);

        // Jump two full window steps ahead of next(1): force losses for 1 and 2,
        // then store and (once contiguous) the far payload is still held until
        // intermediate seqs are lost or filled.
        // next=1; push(1+WINDOW+1=18):
        //   !fits while next=1 (1+16=17, 18>=17) → lose 1, next=2
        //   !fits while next=2 (2+16=18, 18>=18) → lose 2, next=3
        //   fits; store 18; no drain
        let far = 1u16.wrapping_add(REORDER_WINDOW as u16).wrapping_add(1);
        let out = w.push(far, vec![0xbb]);
        assert_eq!(out.iter().filter(|f| matches!(f, OrderedFrame::Lost)).count(), 2);
        assert_eq!(w.stats().losses, 2);
        assert!(!out.iter().any(|f| matches!(f, OrderedFrame::Payload(_))));

        // Force the rest of the gap until `far` itself is delivered.
        // next=3; need to age out 3..far-1.
        let force = far.wrapping_add(REORDER_WINDOW as u16);
        let out2 = w.push(force, vec![0xcc]);
        let lost2 = out2.iter().filter(|f| matches!(f, OrderedFrame::Lost)).count();
        let payloads: Vec<_> = out2
            .iter()
            .filter_map(|f| match f {
                OrderedFrame::Payload(p) => Some(p.as_slice()),
                OrderedFrame::Lost => None,
            })
            .collect();
        // Aged losses for 3..=(far-1), then deliver stored `far` payload, then
        // further losses until `force` is stored (not yet drained).
        assert!(lost2 >= (far - 3) as usize);
        assert!(payloads.iter().any(|p| *p == [0xbb].as_slice()));
        assert_eq!(w.stats().losses, 2 + lost2 as u64);
    }

    #[test]
    fn reorder_seqnum_wrap() {
        let mut w = RealtimeReorderWindow::new();
        let start = u16::MAX - 1;
        assert_eq!(w.push(start, vec![1]), vec![OrderedFrame::Payload(vec![1])]);
        assert_eq!(w.push(u16::MAX, vec![2]), vec![OrderedFrame::Payload(vec![2])]);
        assert_eq!(w.push(0, vec![3]), vec![OrderedFrame::Payload(vec![3])]);
        assert_eq!(w.push(1, vec![4]), vec![OrderedFrame::Payload(vec![4])]);
        assert_eq!(w.stats(), RtpSeqStats::default());
    }
}
