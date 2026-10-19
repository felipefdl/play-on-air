//! Lossless FLAC encode for Cast HTTP media egress.
//!
//! The live streaming encoder ([`LiveFlacEncoder`]: STREAMINFO header, then one fixed-size
//! FLAC frame per PCM block).

use std::fmt;

use bytes::{Bytes, BytesMut};
use flacenc::bitsink::ByteSink;
use flacenc::component::{BitRepr, Stream, StreamInfo};
use flacenc::config;
use flacenc::encode_fixed_size_frame;
use flacenc::error::{Verified, Verify};
use flacenc::source::{Fill, FrameBuf};

use crate::error::{Error, Result};

/// Fixed FLAC block size (frames per channel) for live streaming.
///
/// 2048 frames is ~46 ms at 44.1 kHz: the steady state waits for half as much PCM per frame as
/// flacenc's 4096 default. On stereo 44.1 kHz music it encodes at the same CPU cost (within
/// run-to-run noise) and ~1 % smaller than 4096. It has a dedicated frame-header code and stays
/// inside the FLAC streamable subset.
pub const FLAC_BLOCK_SIZE: usize = 2048;

/// Initial capacity of a live egress output arena (FLAC frames and WAV chunks).
///
/// 64 KiB is the largest capacity `BytesMut` remembers when it must replace a buffer that is
/// still shared, so a replacement arena is the same size as the first one.
pub(crate) const LIVE_ARENA_BYTES: usize = 64 * 1024;

/// Verified encoder config for the live encoder: flacenc defaults without constant subframes.
///
/// A constant subframe codes a block of digital silence in a few bytes, so silence streams at
/// about 7 kbit/s. A Cast receiver does not play until it has read about 32 KB, and after an
/// underrun it waits for far more, so a silent intro never starts and 30 s of silence mid-track
/// stalls it for about 20 s, after which it plays that far behind. Without constant subframes a
/// silent block costs at least 1 bit per sample (about 96 kbit/s for 48 kHz stereo), which keeps
/// the 8 s read-ahead well above that threshold. Decoded audio is unchanged.
fn verified_encoder_config() -> Result<Verified<config::Encoder>> {
  let mut encoder = config::Encoder::default();
  encoder.subframe_coding.use_constant = false;
  encoder
    .into_verified()
    .map_err(|(_cfg, err)| Error::Audio(format!("invalid FLAC encoder config: {err:?}")))
}

/// Build a streaming [`StreamInfo`] with fixed block size and unknown total samples.
///
/// `total_samples` stays 0 and MD5 is zeroed (RFC 9639 allows both for live streams).
fn live_stream_info(sample_rate: u32, channels: u16) -> Result<StreamInfo> {
  if channels == 0 {
    return Err(Error::Audio("channels must be at least 1".to_owned()));
  }
  if sample_rate == 0 {
    return Err(Error::Audio("sample_rate must be non-zero".to_owned()));
  }
  let mut info = StreamInfo::new(sample_rate as usize, usize::from(channels), 16)
    .map_err(|err| Error::Audio(format!("invalid FLAC StreamInfo: {err:?}")))?;
  // Fixed block size for decoders (offline defaults of min=u16::MAX / max=0 are wrong for streaming).
  info
    .set_block_sizes(FLAC_BLOCK_SIZE, FLAC_BLOCK_SIZE)
    .map_err(|err| Error::Audio(format!("invalid FLAC block sizes: {err:?}")))?;
  Ok(info)
}

/// Write `fLaC` + STREAMINFO only (empty stream body) for the start of a live HTTP response.
fn live_stream_header_bytes(stream_info: &StreamInfo) -> Result<Vec<u8>> {
  let stream = Stream::with_stream_info(stream_info.clone());
  let mut sink = ByteSink::new();
  stream
    .write(&mut sink)
    .map_err(|err| Error::Audio(format!("FLAC STREAMINFO write failed: {err}")))?;
  Ok(sink.as_slice().to_vec())
}

/// Worst-case bytes of one live FLAC frame.
///
/// Frame header ≤ 16 bytes; per channel a 1-byte subframe header plus verbatim samples at up to
/// 17 bits (stereo side channel); then byte alignment and CRC-16.
const fn max_live_frame_bytes(channels: usize) -> usize {
  let verbatim_subframe = FLAC_BLOCK_SIZE.saturating_mul(17).div_ceil(8).saturating_add(1);
  channels.saturating_mul(verbatim_subframe).saturating_add(19)
}

/// Round frame count up to a whole number of [`FLAC_BLOCK_SIZE`] blocks (fixed-size live stream).
pub const fn round_up_to_flac_blocks(frames: usize) -> usize {
  if frames == 0 {
    return 0;
  }
  frames.div_ceil(FLAC_BLOCK_SIZE).saturating_mul(FLAC_BLOCK_SIZE)
}

/// Live FLAC encoder: one fixed-size ([`FLAC_BLOCK_SIZE`]) frame per PCM block.
///
/// Every per-frame buffer is sized at construction. Encoded frames are appended to an output
/// arena and handed out as owned [`Bytes`] views of it. Once the HTTP layer drops every earlier
/// chunk, the next frame reclaims the whole arena, so the steady state allocates no output
/// buffers; while chunks are still in flight, a fresh arena is allocated at most once per
/// arena's worth of frames. Allocations inside flacenc's frame encoder are outside this type.
pub struct LiveFlacEncoder {
  channels: u16,
  encoder_config: Verified<config::Encoder>,
  stream_info: StreamInfo,
  framebuf: FrameBuf,
  /// i16 → i32 widening for flacenc input, sized for one block.
  i32_scratch: Vec<i32>,
  /// flacenc bit sink, sized for a worst-case frame so it never grows.
  frame_sink: ByteSink,
  /// Output arena; each frame is split off as an owned chunk.
  arena: BytesMut,
  frame_number: usize,
}

impl fmt::Debug for LiveFlacEncoder {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("LiveFlacEncoder")
      .field("channels", &self.channels)
      .field("frame_number", &self.frame_number)
      .field("arena_capacity", &self.arena.capacity())
      .finish_non_exhaustive()
  }
}

impl LiveFlacEncoder {
  /// Build an encoder for `sample_rate` Hz and `channels` 16-bit channels.
  pub fn new(sample_rate: u32, channels: u16) -> Result<Self> {
    let stream_info = live_stream_info(sample_rate, channels)?;
    let ch = usize::from(channels);
    let framebuf = FrameBuf::with_size(ch, FLAC_BLOCK_SIZE)
      .map_err(|err| Error::Audio(format!("FLAC FrameBuf alloc failed: {err:?}")))?;
    let max_frame = max_live_frame_bytes(ch);
    Ok(Self {
      channels,
      encoder_config: verified_encoder_config()?,
      stream_info,
      framebuf,
      i32_scratch: Vec::with_capacity(FLAC_BLOCK_SIZE.saturating_mul(ch)),
      frame_sink: ByteSink::with_capacity(max_frame.saturating_mul(8)),
      arena: BytesMut::with_capacity(LIVE_ARENA_BYTES.max(max_frame.saturating_mul(2))),
      frame_number: 0,
    })
  }

  /// `fLaC` + STREAMINFO (min = max block size, unknown length) that starts every live body.
  pub fn header_bytes(&self) -> Result<Bytes> {
    live_stream_header_bytes(&self.stream_info).map(Bytes::from)
  }

  /// Encode exactly one block (`FLAC_BLOCK_SIZE * channels` interleaved samples) as the next frame.
  ///
  /// STREAMINFO declares a fixed block size, so a short block is rejected rather than encoded.
  pub fn encode_block(&mut self, i16_samples: &[i16]) -> Result<Bytes> {
    let expected = FLAC_BLOCK_SIZE.saturating_mul(usize::from(self.channels));
    if i16_samples.len() != expected {
      return Err(Error::Audio(format!(
        "FLAC block has {} samples, expected {expected} ({FLAC_BLOCK_SIZE} frames x {} channels)",
        i16_samples.len(),
        self.channels
      )));
    }

    self.i32_scratch.clear();
    self.i32_scratch.extend(i16_samples.iter().map(|&s| i32::from(s)));
    self
      .framebuf
      .fill_interleaved(&self.i32_scratch)
      .map_err(|err| Error::Audio(format!("FLAC FrameBuf fill failed: {err}")))?;

    let frame = encode_fixed_size_frame(&self.encoder_config, &self.framebuf, self.frame_number, &self.stream_info)
      .map_err(|err| Error::Audio(format!("FLAC frame encode failed: {err}")))?;
    self.frame_sink.clear();
    frame
      .write(&mut self.frame_sink)
      .map_err(|err| Error::Audio(format!("FLAC frame write failed: {err}")))?;

    let encoded = self.frame_sink.as_slice();
    self.arena.reserve(encoded.len());
    self.arena.extend_from_slice(encoded);
    self.frame_number = self.frame_number.saturating_add(1);
    Ok(self.arena.split().freeze())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// Hard-to-compress stereo PCM (LCG noise, independent channels) so frames approach verbatim
  /// size and a run of them overflows one output arena.
  fn stereo_noise_i16(frames: usize, seed: u32) -> Vec<i16> {
    let mut state = seed;
    let mut out = Vec::with_capacity(frames * 2);
    for _ in 0..frames * 2 {
      state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
      out.push((state >> 16) as i16);
    }
    out
  }

  #[test]
  fn live_header_is_flac_magic_plus_streaminfo() {
    let encoder = LiveFlacEncoder::new(48_000, 2).expect("encoder");
    let header = encoder.header_bytes().expect("header");
    assert_eq!(&header[0..4], b"fLaC");
    // fLaC (4) + metadata block header (4) + STREAMINFO body (34) = 42
    assert_eq!(header.len(), 42);
  }

  /// Many live frames held at once (HTTP backlog) across arena turnover: the concatenated
  /// stream decodes bit-exact, and every frame is exactly the STREAMINFO block size.
  #[test]
  fn live_frames_held_across_arena_turnover_decode_bit_exact() {
    let sample_rate = 44_100_u32;
    let channels = 2_u16;
    let mut encoder = LiveFlacEncoder::new(sample_rate, channels).expect("encoder");
    let blocks = 24;
    let pcm = stereo_noise_i16(FLAC_BLOCK_SIZE * blocks, 7);

    let mut chunks = Vec::with_capacity(blocks);
    for block in pcm.as_chunks::<{ FLAC_BLOCK_SIZE * 2 }>().0 {
      chunks.push(encoder.encode_block(block).expect("frame"));
    }
    let frame_bytes: usize = chunks.iter().map(Bytes::len).sum();
    assert!(
      frame_bytes > LIVE_ARENA_BYTES * 2,
      "test must overflow the arena ({frame_bytes} bytes)"
    );

    let mut bitstream = encoder.header_bytes().expect("header").to_vec();
    for chunk in &chunks {
      bitstream.extend_from_slice(chunk);
    }
    let mut reader = claxon::FlacReader::new(std::io::Cursor::new(&bitstream)).expect("claxon");
    let info = reader.streaminfo();
    assert_eq!(usize::from(info.min_block_size), FLAC_BLOCK_SIZE);
    assert_eq!(usize::from(info.max_block_size), FLAC_BLOCK_SIZE);

    let mut decoded = Vec::with_capacity(pcm.len());
    let mut frames = reader.blocks();
    let mut buffer = Vec::new();
    while let Some(block) = frames.read_next_or_eof(buffer).expect("frame") {
      assert_eq!(block.duration() as usize, FLAC_BLOCK_SIZE, "fixed block size");
      for (left, right) in block.stereo_samples() {
        decoded.push(left);
        decoded.push(right);
      }
      buffer = block.into_buffer();
    }
    assert_eq!(decoded.len(), pcm.len());
    for (i, (&orig, &dec)) in pcm.iter().zip(decoded.iter()).enumerate() {
      assert_eq!(i32::from(orig), dec, "mismatch at sample {i}");
    }
  }

  /// Digital silence keeps a byte rate a Cast receiver can buffer: every silent frame carries at
  /// least 1 bit per sample (a constant subframe would be a few bytes and stall the speaker), and
  /// it still decodes to exact zeros.
  #[test]
  fn digital_silence_frames_keep_one_bit_per_sample_and_decode_to_zero() {
    let mut encoder = LiveFlacEncoder::new(48_000, 2).expect("encoder");
    let silence = vec![0_i16; FLAC_BLOCK_SIZE * 2];
    let mut bitstream = encoder.header_bytes().expect("header").to_vec();
    for _ in 0..4 {
      let frame = encoder.encode_block(&silence).expect("frame");
      assert!(
        frame.len() >= FLAC_BLOCK_SIZE * 2 / 8,
        "silent frame is {} bytes, under 1 bit per sample",
        frame.len()
      );
      bitstream.extend_from_slice(&frame);
    }

    let mut reader = claxon::FlacReader::new(std::io::Cursor::new(&bitstream)).expect("claxon");
    let decoded: Vec<i32> = reader.samples().map(|s| s.expect("sample")).collect();
    assert_eq!(decoded.len(), FLAC_BLOCK_SIZE * 2 * 4);
    assert!(decoded.iter().all(|&s| s == 0), "silence decodes to zeros");
  }

  /// Steady state: when each chunk is dropped before the next encode (HTTP wrote it), every
  /// frame lands in the arena allocated at construction — no per-frame output allocation.
  #[test]
  fn live_frames_reuse_one_arena_when_chunks_are_released() {
    let mut encoder = LiveFlacEncoder::new(44_100, 2).expect("encoder");
    let base = encoder.arena.as_ptr() as usize;
    let capacity = encoder.arena.capacity();
    let pcm = stereo_noise_i16(FLAC_BLOCK_SIZE * 64, 11);
    let mut total = 0_usize;
    for (n, block) in pcm.as_chunks::<{ FLAC_BLOCK_SIZE * 2 }>().0.iter().enumerate() {
      let chunk = encoder.encode_block(block).expect("frame");
      let start = chunk.as_ptr() as usize;
      assert!(
        start >= base && start + chunk.len() <= base + capacity,
        "frame {n} left the original arena (reallocated)"
      );
      total += chunk.len();
    }
    assert!(total > capacity * 4, "frames must wrap the arena several times");
  }

  #[test]
  fn live_encoder_rejects_short_block() {
    let mut encoder = LiveFlacEncoder::new(44_100, 2).expect("encoder");
    let short = vec![0_i16; (FLAC_BLOCK_SIZE - 1) * 2];
    let err = encoder.encode_block(&short).unwrap_err();
    assert!(matches!(err, Error::Audio(_)));
  }

  #[test]
  fn round_up_to_flac_blocks_aligns() {
    assert_eq!(round_up_to_flac_blocks(0), 0);
    assert_eq!(round_up_to_flac_blocks(1), FLAC_BLOCK_SIZE);
    assert_eq!(round_up_to_flac_blocks(FLAC_BLOCK_SIZE), FLAC_BLOCK_SIZE);
    assert_eq!(round_up_to_flac_blocks(FLAC_BLOCK_SIZE + 1), FLAC_BLOCK_SIZE * 2);
  }
}
