//! Pre-sized interleaved PCM ring buffer for the steady-state audio path.

use std::collections::VecDeque;

use parking_lot::Mutex;

/// Interleaved float PCM ring with i16 pop for encode/egress.
///
/// Samples are interleaved (L,R,L,R,…). Capacity is in **sample frames**
/// (one frame = `channels` samples).
#[derive(Debug)]
pub struct PcmRing {
  channels: u16,
  /// Interleaved f32 samples.
  buf: Mutex<VecDeque<f32>>,
  /// Max interleaved samples (frames * channels).
  capacity_samples: usize,
}

impl PcmRing {
  /// Create a ring that holds up to `capacity_frames` frames at `channels`.
  pub fn new(channels: u16, capacity_frames: usize) -> Self {
    let ch = usize::from(channels.max(1));
    let capacity_samples = capacity_frames.saturating_mul(ch);
    Self {
      channels: channels.max(1),
      buf: Mutex::new(VecDeque::with_capacity(capacity_samples)),
      capacity_samples,
    }
  }

  /// Channel count.
  pub const fn channels(&self) -> u16 {
    self.channels
  }

  /// Push interleaved f32 samples (`-1.0..1.0` nominal).
  ///
  /// On overflow, oldest samples are dropped (drop-from-head).
  pub fn push_f32(&self, samples: &[f32]) {
    let mut guard = self.buf.lock();
    for &s in samples {
      if guard.len() >= self.capacity_samples {
        let _ = guard.pop_front();
      }
      guard.push_back(s);
    }
  }

  /// Pop up to `frame_count` frames as interleaved i16 PCM.
  ///
  /// Returns the number of **frames** written (may be less if underrun).
  /// Output length is `frames * channels`.
  pub fn pop_i16(&self, frame_count: usize, out: &mut Vec<i16>) -> usize {
    let ch = usize::from(self.channels);
    let need = frame_count.saturating_mul(ch);
    out.clear();
    out.reserve(need);

    let mut guard = self.buf.lock();
    let available_frames = guard.len() / ch;
    let take_frames = frame_count.min(available_frames);
    let take_samples = take_frames.saturating_mul(ch);

    for _ in 0..take_samples {
      // Underrun is guarded by `take_samples`; default is defensive only.
      let sample = guard.pop_front().unwrap_or(0.0);
      out.push(f32_to_i16(sample));
    }
    drop(guard);
    take_frames
  }

  /// Copy up to `frame_count` frames as interleaved i16 without consuming the ring.
  ///
  /// Used for FLAC quality-path snapshots while live WAV continues streaming.
  pub fn copy_i16(&self, frame_count: usize, out: &mut Vec<i16>) -> usize {
    let ch = usize::from(self.channels);
    let need = frame_count.saturating_mul(ch);
    out.clear();
    out.reserve(need);

    let guard = self.buf.lock();
    let available_frames = guard.len() / ch;
    let take_frames = frame_count.min(available_frames);
    let take_samples = take_frames.saturating_mul(ch);

    for idx in 0..take_samples {
      let sample = guard.get(idx).copied().unwrap_or(0.0);
      out.push(f32_to_i16(sample));
    }
    drop(guard);
    take_frames
  }

  /// Number of complete frames currently buffered.
  pub fn available_frames(&self) -> usize {
    let guard = self.buf.lock();
    guard.len() / usize::from(self.channels)
  }

  /// Clear all buffered samples.
  pub fn clear(&self) {
    self.buf.lock().clear();
  }
}

/// Convert one f32 sample to i16 with clamping.
fn f32_to_i16(sample: f32) -> i16 {
  let clamped = sample.clamp(-1.0, 1.0);
  // Symmetric mapping: ±1.0 → ±32767 (avoid -32768 asymmetry for encode).
  let scaled = clamped * 32_767.0;
  scaled.round() as i16
}

/// Convert interleaved i16 to f32 in place helper for tests/fixtures.
#[cfg(test)]
pub fn i16_to_f32(samples: &[i16]) -> Vec<f32> {
  samples.iter().map(|&s| f32::from(s) / 32_767.0).collect()
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn push_pop_roundtrip_stereo() {
    let ring = PcmRing::new(2, 64);
    let input: Vec<f32> = (0..32).map(|i| (i as f32) / 32.0 - 0.5).collect();
    ring.push_f32(&input);
    assert_eq!(ring.available_frames(), 16);

    let mut out = Vec::new();
    let frames = ring.pop_i16(16, &mut out);
    assert_eq!(frames, 16);
    assert_eq!(out.len(), 32);
    for (i, &v) in out.iter().enumerate() {
      let expected = f32_to_i16(input[i]);
      assert_eq!(v, expected);
    }
  }

  #[test]
  fn underrun_returns_partial() {
    let ring = PcmRing::new(1, 8);
    ring.push_f32(&[0.1, 0.2]);
    let mut out = Vec::new();
    let frames = ring.pop_i16(8, &mut out);
    assert_eq!(frames, 2);
    assert_eq!(out.len(), 2);
  }

  #[test]
  fn overflow_drops_oldest() {
    let ring = PcmRing::new(1, 2);
    ring.push_f32(&[0.1, 0.2, 0.3]);
    assert_eq!(ring.available_frames(), 2);
    let mut out = Vec::new();
    let _ = ring.pop_i16(2, &mut out);
    // Oldest 0.1 dropped; remaining 0.2, 0.3
    assert_eq!(out[0], f32_to_i16(0.2));
    assert_eq!(out[1], f32_to_i16(0.3));
  }

  #[test]
  fn clamp_extremes() {
    assert_eq!(f32_to_i16(1.0), 32_767);
    assert_eq!(f32_to_i16(-1.0), -32_767);
    assert_eq!(f32_to_i16(2.0), 32_767);
    assert_eq!(f32_to_i16(-2.0), -32_767);
    assert_eq!(f32_to_i16(0.0), 0);
  }
}
