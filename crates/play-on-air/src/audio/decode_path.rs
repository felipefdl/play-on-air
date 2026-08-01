//! Decode-path fixture tests: simulated AirPlay PCM through ring → FLAC.
//!
//! Product code receives f32 PCM from shairplay after AP2 decode (realtime ALAC
//! or buffered AAC). These tests exercise the shipped encode path with
//! synthetic PCM shaped like each path.

#[cfg(test)]
mod tests {
  use std::f32::consts::PI;
  use std::sync::Arc;

  use crate::audio::{PcmRing, encode_pcm_i16_to_flac};

  fn sine_f32(frames: usize, channels: u16, freq_hz: f32, sample_rate: u32, amplitude: f32) -> Vec<f32> {
    let ch = usize::from(channels);
    let mut out = Vec::with_capacity(frames.saturating_mul(ch));
    for n in 0..frames {
      let t = n as f32 / sample_rate as f32;
      let s = (2.0 * PI * freq_hz * t).sin() * amplitude;
      for _ in 0..ch {
        out.push(s);
      }
    }
    out
  }

  /// Realtime-style path: sparse-ish sine at 48 kHz stereo (ALAC-like after decode).
  #[test]
  fn realtime_alac_like_pcm_to_flac() {
    let sample_rate = 48_000_u32;
    let channels = 2_u16;
    let frames = 4096_usize;
    let ring = Arc::new(PcmRing::new(channels, frames * 2));
    let pcm_f32 = sine_f32(frames, channels, 440.0, sample_rate, 0.5);
    ring.push_f32(&pcm_f32);

    let mut i16_buf = Vec::new();
    let got = ring.pop_i16(frames, &mut i16_buf);
    assert_eq!(got, frames);

    let flac = encode_pcm_i16_to_flac(&i16_buf, channels, sample_rate).expect("encode");
    assert!(flac.len() > 42);
    assert_eq!(&flac[0..4], b"fLaC");

    let cursor = std::io::Cursor::new(&flac);
    let mut reader = claxon::FlacReader::new(cursor).expect("claxon open");
    let info = reader.streaminfo();
    assert_eq!(info.sample_rate, sample_rate);
    assert_eq!(info.channels, u32::from(channels));
    let decoded: Vec<i32> = reader.samples().map(|r| r.expect("sample")).collect();
    assert_eq!(decoded.len(), i16_buf.len());
  }

  /// Buffered-style path: denser 44.1 kHz stereo block (AAC-like after decode).
  #[test]
  fn buffered_aac_like_pcm_to_flac() {
    let sample_rate = 44_100_u32;
    let channels = 2_u16;
    // Longer, denser block approximating a buffered pull window.
    let frames = 16_384_usize;
    let ring = Arc::new(PcmRing::new(channels, frames * 2));
    // Mix of two tones to look less like a pure realtime sine.
    let mut pcm_f32 = sine_f32(frames, channels, 220.0, sample_rate, 0.35);
    let overtone = sine_f32(frames, channels, 660.0, sample_rate, 0.15);
    for (dst, src) in pcm_f32.iter_mut().zip(overtone.iter()) {
      *dst += *src;
    }
    ring.push_f32(&pcm_f32);

    let mut i16_buf = Vec::new();
    let got = ring.pop_i16(frames, &mut i16_buf);
    assert_eq!(got, frames);

    let flac = encode_pcm_i16_to_flac(&i16_buf, channels, sample_rate).expect("encode");
    assert!(flac.len() > 42);
    assert_eq!(&flac[0..4], b"fLaC");

    let cursor = std::io::Cursor::new(&flac);
    let mut reader = claxon::FlacReader::new(cursor).expect("claxon open");
    let info = reader.streaminfo();
    assert_eq!(info.sample_rate, sample_rate);
    assert_eq!(info.channels, u32::from(channels));
    let decoded: Vec<i32> = reader.samples().map(|r| r.expect("sample")).collect();
    assert_eq!(decoded.len(), i16_buf.len());
  }
}
