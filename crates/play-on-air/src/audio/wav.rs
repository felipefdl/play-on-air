//! WAV / LPCM helpers for Cast sinks that reject FLAC.

use crate::error::{Error, Result};

/// Build a standard RIFF WAVE header for PCM s16le.
///
/// `data_len` is the byte length of the PCM payload that follows (or a large
/// placeholder for continuous streams).
pub fn wav_header(channels: u16, sample_rate: u32, data_len: u32) -> Result<[u8; 44]> {
  if channels == 0 {
    return Err(Error::Audio("channels must be at least 1".to_owned()));
  }
  if sample_rate == 0 {
    return Err(Error::Audio("sample_rate must be non-zero".to_owned()));
  }

  let bits_per_sample: u16 = 16;
  let block_align = channels.saturating_mul(bits_per_sample / 8);
  let byte_rate = sample_rate.saturating_mul(u32::from(block_align));
  // RIFF chunk size = 36 + data_len (header after "RIFF" size field through end of data).
  let riff_size = 36_u32.saturating_add(data_len);

  let mut h = [0_u8; 44];
  // "RIFF"
  h[0] = b'R';
  h[1] = b'I';
  h[2] = b'F';
  h[3] = b'F';
  write_u32_le(&mut h, 4, riff_size);
  // "WAVE"
  h[8] = b'W';
  h[9] = b'A';
  h[10] = b'V';
  h[11] = b'E';
  // "fmt "
  h[12] = b'f';
  h[13] = b'm';
  h[14] = b't';
  h[15] = b' ';
  write_u32_le(&mut h, 16, 16); // PCM fmt chunk size
  write_u16_le(&mut h, 20, 1); // audio format = PCM
  write_u16_le(&mut h, 22, channels);
  write_u32_le(&mut h, 24, sample_rate);
  write_u32_le(&mut h, 28, byte_rate);
  write_u16_le(&mut h, 32, block_align);
  write_u16_le(&mut h, 34, bits_per_sample);
  // "data"
  h[36] = b'd';
  h[37] = b'a';
  h[38] = b't';
  h[39] = b'a';
  write_u32_le(&mut h, 40, data_len);
  Ok(h)
}

/// Encode a complete finite WAV file (header + interleaved i16 PCM body).
pub fn encode_pcm_i16_to_wav(samples: &[i16], channels: u16, sample_rate: u32) -> Result<Vec<u8>> {
  let ch = usize::from(channels.max(1));
  if channels == 0 {
    return Err(Error::Audio("channels must be at least 1".to_owned()));
  }
  if !samples.len().is_multiple_of(ch) {
    return Err(Error::Audio(format!(
      "sample count {} is not a multiple of channels {channels}",
      samples.len()
    )));
  }

  let data_len_usize = samples.len().saturating_mul(2);
  let data_len =
    u32::try_from(data_len_usize).map_err(|_overflow| Error::Audio("WAV data length exceeds u32".to_owned()))?;

  let header = wav_header(channels, sample_rate, data_len)?;
  let mut out = Vec::with_capacity(44 + data_len_usize);
  out.extend_from_slice(&header);
  for &s in samples {
    out.extend_from_slice(&s.to_le_bytes());
  }
  Ok(out)
}

/// Continuous-stream header using a large data size so players keep reading.
pub fn continuous_wav_header(channels: u16, sample_rate: u32) -> Result<[u8; 44]> {
  // ~4 GiB payload placeholder; many players stream until connection close.
  wav_header(channels, sample_rate, u32::MAX / 2)
}

fn write_u16_le(buf: &mut [u8; 44], offset: usize, value: u16) {
  let bytes = value.to_le_bytes();
  if let Some(slot) = buf.get_mut(offset..offset + 2) {
    slot.copy_from_slice(&bytes);
  }
}

fn write_u32_le(buf: &mut [u8; 44], offset: usize, value: u32) {
  let bytes = value.to_le_bytes();
  if let Some(slot) = buf.get_mut(offset..offset + 4) {
    slot.copy_from_slice(&bytes);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn header_magic_and_sizes() {
    let h = wav_header(2, 44_100, 1000).expect("header");
    assert_eq!(&h[0..4], b"RIFF");
    assert_eq!(&h[8..12], b"WAVE");
    assert_eq!(&h[12..16], b"fmt ");
    assert_eq!(&h[36..40], b"data");
    // channels
    assert_eq!(u16::from_le_bytes([h[22], h[23]]), 2);
    // sample rate
    assert_eq!(u32::from_le_bytes([h[24], h[25], h[26], h[27]]), 44_100);
  }

  #[test]
  fn encode_roundtrip_hound() {
    let samples: Vec<i16> = (0..256).map(|i| (i * 100) as i16).collect();
    let wav = encode_pcm_i16_to_wav(&samples, 1, 16_000).expect("encode");
    let cursor = std::io::Cursor::new(&wav);
    let mut reader = hound::WavReader::new(cursor).expect("hound");
    let spec = reader.spec();
    assert_eq!(spec.channels, 1);
    assert_eq!(spec.sample_rate, 16_000);
    let decoded: Vec<i16> = reader.samples::<i16>().map(|s| s.expect("s")).collect();
    assert_eq!(decoded, samples);
  }
}
