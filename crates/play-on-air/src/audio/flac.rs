//! Lossless FLAC encode for Cast HTTP media egress.

use flacenc::bitsink::ByteSink;
use flacenc::component::BitRepr;
use flacenc::error::Verify;
use flacenc::source::MemSource;

use crate::error::{Error, Result};

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
  let config = flacenc::config::Encoder::default()
    .into_verified()
    .map_err(|err| Error::Audio(format!("invalid FLAC encoder config: {err:?}")))?;

  let source = MemSource::from_samples(&i32_samples, ch, bits_per_sample, sample_rate as usize);

  let flac_stream = flacenc::encode_with_fixed_block_size(&config, source, config.block_size)
    .map_err(|err| Error::Audio(format!("FLAC encode failed: {err}")))?;

  let mut sink = ByteSink::new();
  flac_stream
    .write(&mut sink)
    .map_err(|err| Error::Audio(format!("FLAC bitstream write failed: {err}")))?;

  Ok(sink.as_slice().to_vec())
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
}
