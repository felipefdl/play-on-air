//! Lossless FLAC encode for Cast HTTP media egress.
//!
//! Offline snapshot encode ([`encode_pcm_i16_to_flac`]) and live streaming helpers
//! (STREAMINFO header + per-block frames via [`encode_fixed_size_frame`]).

use flacenc::bitsink::ByteSink;
use flacenc::component::{BitRepr, Stream, StreamInfo};
use flacenc::config;
use flacenc::encode_fixed_size_frame;
use flacenc::error::{Verified, Verify};
use flacenc::source::{Fill, FrameBuf};

use crate::error::{Error, Result};

/// Fixed FLAC block size used for live streaming (flacenc default).
pub const FLAC_BLOCK_SIZE: usize = 4096;

/// Encode interleaved little-endian-order i16 PCM to a complete FLAC bitstream.
///
/// `samples` is interleaved: `[L0, R0, L1, R1, …]` for stereo.
/// `channels` must be ≥ 1. `sample_rate` is in Hz (e.g. 44100).
pub fn encode_pcm_i16_to_flac(samples: &[i16], channels: u16, sample_rate: u32) -> Result<Vec<u8>> {
  if channels == 0 {
    return Err(Error::Audio("channels must be at least 1".to_owned()));
  }
  if sample_rate == 0 {
    return Err(Error::Audio("sample_rate must be non-zero".to_owned()));
  }

  let ch = usize::from(channels);
  if !samples.len().is_multiple_of(ch) {
    return Err(Error::Audio(format!(
      "sample count {} is not a multiple of channels {channels}",
      samples.len()
    )));
  }

  // flacenc expects interleaved i32 samples in i16 range.
  let i32_samples: Vec<i32> = samples.iter().map(|&s| i32::from(s)).collect();

  let bits_per_sample = 16_usize;
  let encoder_config = verified_encoder_config()?;

  let source = flacenc::source::MemSource::from_samples(&i32_samples, ch, bits_per_sample, sample_rate as usize);

  let flac_stream = flacenc::encode_with_fixed_block_size(&encoder_config, source, encoder_config.block_size)
    .map_err(|err| Error::Audio(format!("FLAC encode failed: {err}")))?;

  let mut sink = ByteSink::new();
  flac_stream
    .write(&mut sink)
    .map_err(|err| Error::Audio(format!("FLAC bitstream write failed: {err}")))?;

  Ok(sink.as_slice().to_vec())
}

/// Default verified encoder config for offline and live encode paths.
pub fn verified_encoder_config() -> Result<Verified<config::Encoder>> {
  config::Encoder::default()
    .into_verified()
    .map_err(|(_cfg, err)| Error::Audio(format!("invalid FLAC encoder config: {err:?}")))
}

/// Build a streaming [`StreamInfo`] with fixed block size and unknown total samples.
///
/// `total_samples` stays 0 and MD5 is zeroed (RFC 9639 allows both for live streams).
pub fn live_stream_info(sample_rate: u32, channels: u16) -> Result<StreamInfo> {
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
pub fn live_stream_header_bytes(stream_info: &StreamInfo) -> Result<Vec<u8>> {
  let stream = Stream::with_stream_info(stream_info.clone());
  let mut sink = ByteSink::new();
  stream
    .write(&mut sink)
    .map_err(|err| Error::Audio(format!("FLAC STREAMINFO write failed: {err}")))?;
  Ok(sink.as_slice().to_vec())
}

/// Allocate a reusable [`FrameBuf`] for live FLAC block encoding.
pub fn live_frame_buf(channels: u16) -> Result<FrameBuf> {
  let ch = usize::from(channels.max(1));
  FrameBuf::with_size(ch, FLAC_BLOCK_SIZE).map_err(|err| Error::Audio(format!("FLAC FrameBuf alloc failed: {err:?}")))
}

/// Encode one interleaved i16 PCM block (≤ [`FLAC_BLOCK_SIZE`] frames) into a single FLAC frame.
///
/// `i16_samples` length must be `frames * channels`. Shorter than a full block is allowed for a
/// preroll tail; steady-state live encode should pass full blocks.
#[expect(
  clippy::too_many_arguments,
  reason = "encoder state is threaded explicitly for reusable live stream buffers"
)]
pub fn encode_i16_block_to_frame(
  i16_samples: &[i16],
  channels: u16,
  frame_number: usize,
  encoder_config: &Verified<config::Encoder>,
  stream_info: &StreamInfo,
  framebuf: &mut FrameBuf,
  i32_scratch: &mut Vec<i32>,
) -> Result<Vec<u8>> {
  let ch = usize::from(channels.max(1));
  if ch == 0 || !i16_samples.len().is_multiple_of(ch) {
    return Err(Error::Audio(format!(
      "FLAC block sample count {} is not a multiple of channels {channels}",
      i16_samples.len()
    )));
  }
  let frames = i16_samples.len() / ch;
  if frames == 0 {
    return Err(Error::Audio("FLAC block has zero frames".to_owned()));
  }
  if frames > FLAC_BLOCK_SIZE {
    return Err(Error::Audio(format!(
      "FLAC block frames {frames} exceed FLAC_BLOCK_SIZE {FLAC_BLOCK_SIZE}"
    )));
  }

  i32_scratch.clear();
  i32_scratch.reserve(i16_samples.len());
  for &s in i16_samples {
    i32_scratch.push(i32::from(s));
  }

  framebuf
    .fill_interleaved(i32_scratch)
    .map_err(|err| Error::Audio(format!("FLAC FrameBuf fill failed: {err}")))?;

  let frame = encode_fixed_size_frame(encoder_config, framebuf, frame_number, stream_info)
    .map_err(|err| Error::Audio(format!("FLAC frame encode failed: {err}")))?;

  let mut sink = ByteSink::new();
  frame
    .write(&mut sink)
    .map_err(|err| Error::Audio(format!("FLAC frame write failed: {err}")))?;
  Ok(sink.as_slice().to_vec())
}

/// Round frame count up to a whole number of [`FLAC_BLOCK_SIZE`] blocks (fixed-size live stream).
pub const fn round_up_to_flac_blocks(frames: usize) -> usize {
  if frames == 0 {
    return 0;
  }
  frames.div_ceil(FLAC_BLOCK_SIZE).saturating_mul(FLAC_BLOCK_SIZE)
}

#[cfg(test)]
mod tests {
  use super::*;

  fn sine_i16(frames: usize, channels: u16, freq_hz: f32, sample_rate: u32) -> Vec<i16> {
    let ch = usize::from(channels);
    let mut out = Vec::with_capacity(frames * ch);
    for n in 0..frames {
      let t = n as f32 / sample_rate as f32;
      let s = (2.0 * std::f32::consts::PI * freq_hz * t).sin();
      let v = (s * 16_000.0).round() as i16;
      for _ in 0..ch {
        out.push(v);
      }
    }
    out
  }

  #[test]
  fn encode_stereo_roundtrip_claxon() {
    let sample_rate = 44_100_u32;
    let channels = 2_u16;
    let pcm = sine_i16(4096, channels, 440.0, sample_rate);
    let flac = encode_pcm_i16_to_flac(&pcm, channels, sample_rate).expect("encode");
    assert!(flac.len() > 42, "FLAC should be non-trivial");
    // fLaC magic
    assert_eq!(&flac[0..4], b"fLaC");

    let cursor = std::io::Cursor::new(&flac);
    let mut reader = claxon::FlacReader::new(cursor).expect("claxon open");
    let info = reader.streaminfo();
    assert_eq!(info.sample_rate, sample_rate);
    assert_eq!(info.channels, u32::from(channels));

    let decoded: Vec<i32> = reader.samples().map(|r| r.expect("sample")).collect();
    assert_eq!(decoded.len(), pcm.len());
    for (i, (&orig, &dec)) in pcm.iter().zip(decoded.iter()).enumerate() {
      assert_eq!(i32::from(orig), dec, "mismatch at sample {i}");
    }
  }

  #[test]
  fn reject_zero_channels() {
    let err = encode_pcm_i16_to_flac(&[0], 0, 44_100).unwrap_err();
    assert!(matches!(err, Error::Audio(_)));
  }

  #[test]
  fn reject_bad_alignment() {
    let err = encode_pcm_i16_to_flac(&[0, 1, 2], 2, 44_100).unwrap_err();
    assert!(matches!(err, Error::Audio(_)));
  }

  #[test]
  fn live_header_is_flac_magic_plus_streaminfo() {
    let info = live_stream_info(48_000, 2).expect("stream info");
    let header = live_stream_header_bytes(&info).expect("header");
    assert_eq!(&header[0..4], b"fLaC");
    // fLaC (4) + metadata block header (4) + STREAMINFO body (34) = 42
    assert_eq!(header.len(), 42);
  }

  #[test]
  fn live_block_encode_roundtrip_claxon() {
    let sample_rate = 8_000_u32;
    let channels = 1_u16;
    let info = live_stream_info(sample_rate, channels).expect("info");
    let header = live_stream_header_bytes(&info).expect("header");
    let config = verified_encoder_config().expect("config");
    let mut framebuf = live_frame_buf(channels).expect("framebuf");
    let mut i32_scratch = Vec::new();

    let pcm = sine_i16(FLAC_BLOCK_SIZE, channels, 440.0, sample_rate);
    let frame =
      encode_i16_block_to_frame(&pcm, channels, 0, &config, &info, &mut framebuf, &mut i32_scratch).expect("frame");

    let mut bitstream = header;
    bitstream.extend_from_slice(&frame);

    let mut reader = claxon::FlacReader::new(std::io::Cursor::new(&bitstream)).expect("claxon");
    let decoded: Vec<i32> = reader.samples().map(|r| r.expect("sample")).collect();
    assert_eq!(decoded.len(), pcm.len());
    for (i, (&orig, &dec)) in pcm.iter().zip(decoded.iter()).enumerate() {
      assert_eq!(i32::from(orig), dec, "mismatch at sample {i}");
    }
  }

  #[test]
  fn round_up_to_flac_blocks_aligns() {
    assert_eq!(round_up_to_flac_blocks(0), 0);
    assert_eq!(round_up_to_flac_blocks(1), FLAC_BLOCK_SIZE);
    assert_eq!(round_up_to_flac_blocks(FLAC_BLOCK_SIZE), FLAC_BLOCK_SIZE);
    assert_eq!(round_up_to_flac_blocks(FLAC_BLOCK_SIZE + 1), FLAC_BLOCK_SIZE * 2);
  }
}
