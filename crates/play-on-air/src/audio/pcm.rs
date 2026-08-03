//! Pre-sized interleaved PCM ring buffer for the steady-state audio path.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

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
  /// Complete frames dropped because the ring was full (overflow drop-from-head).
  frames_dropped_overflow: AtomicU64,
  /// Times [`Self::pop_i16`] found an empty ring (no complete frame available).
  underrun_polls: AtomicU64,
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
      frames_dropped_overflow: AtomicU64::new(0),
      underrun_polls: AtomicU64::new(0),
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
    if samples.is_empty() {
      return;
    }
    let ch = usize::from(self.channels);
    let mut guard = self.buf.lock();
    let total = guard.len().saturating_add(samples.len());
    if total > self.capacity_samples {
      let drop_samples = total - self.capacity_samples;
      let drop_from_buf = drop_samples.min(guard.len());
      if drop_from_buf > 0 {
        // Drain without collecting — drop the iterator to discard samples.
        drop(guard.drain(..drop_from_buf));
      }
      // If the push itself exceeds capacity, keep only the newest tail.
      let skip = drop_samples.saturating_sub(drop_from_buf);
      let keep = samples.get(skip..).unwrap_or(&[]);
      // Count complete frames discarded (partial leftover sample is not a frame).
      let frames = u64::try_from(drop_samples / ch).unwrap_or(0);
      if frames > 0 {
        let _ = self.frames_dropped_overflow.fetch_add(frames, Ordering::Relaxed);
      }
      guard.extend(keep.iter().copied());
    } else {
      guard.extend(samples.iter().copied());
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
    if take_frames == 0 {
      let _ = self.underrun_polls.fetch_add(1, Ordering::Relaxed);
      return 0;
    }
    let take_samples = take_frames.saturating_mul(ch);

    let (first, second) = guard.as_slices();
    let first_take = take_samples.min(first.len());
    for &sample in first.iter().take(first_take) {
      out.push(f32_to_i16(sample));
    }
    let second_take = take_samples.saturating_sub(first_take);
    if second_take > 0 {
      for &sample in second.iter().take(second_take) {
        out.push(f32_to_i16(sample));
      }
    }
    drop(guard.drain(..take_samples));
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

    let (first, second) = guard.as_slices();
    let first_take = take_samples.min(first.len());
    for &sample in first.iter().take(first_take) {
      out.push(f32_to_i16(sample));
    }
    let second_take = take_samples.saturating_sub(first_take);
    if second_take > 0 {
      for &sample in second.iter().take(second_take) {
        out.push(f32_to_i16(sample));
      }
    }
    drop(guard);
    take_frames
  }

  /// Number of complete frames currently buffered.
  pub fn available_frames(&self) -> usize {
    let guard = self.buf.lock();
    guard.len() / usize::from(self.channels)
  }

  /// Occupancy in complete frames (alias of [`Self::available_frames`]).
  pub fn occupancy_frames(&self) -> usize {
    self.available_frames()
  }

  /// Frames discarded on overflow since construction (relaxed counter).
  pub fn frames_dropped_overflow(&self) -> u64 {
    self.frames_dropped_overflow.load(Ordering::Relaxed)
  }

  /// Empty-ring [`Self::pop_i16`] polls since construction (relaxed counter).
  pub fn underrun_polls(&self) -> u64 {
    self.underrun_polls.load(Ordering::Relaxed)
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
    assert_eq!(ring.occupancy_frames(), 16);

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
    assert_eq!(ring.underrun_polls(), 0);

    let frames_empty = ring.pop_i16(1, &mut out);
    assert_eq!(frames_empty, 0);
    assert_eq!(ring.underrun_polls(), 1);
  }

  #[test]
  fn overflow_drops_oldest() {
    let ring = PcmRing::new(1, 2);
    ring.push_f32(&[0.1, 0.2, 0.3]);
    assert_eq!(ring.available_frames(), 2);
    assert_eq!(ring.frames_dropped_overflow(), 1);
    let mut out = Vec::new();
    let _ = ring.pop_i16(2, &mut out);
    // Oldest 0.1 dropped; remaining 0.2, 0.3
    assert_eq!(out[0], f32_to_i16(0.2));
    assert_eq!(out[1], f32_to_i16(0.3));
  }

  #[test]
  fn bulk_push_across_wrapped_ring() {
    let ring = PcmRing::new(2, 8);
    // Fill, partial pop so the deque is non-contiguous after further pushes.
    ring.push_f32(&[0.1; 16]); // 8 frames
    let mut out = Vec::new();
    assert_eq!(ring.pop_i16(3, &mut out), 3);
    ring.push_f32(&[0.5; 6]); // 3 frames
    assert_eq!(ring.occupancy_frames(), 8);
    let frames = ring.pop_i16(8, &mut out);
    assert_eq!(frames, 8);
    assert_eq!(out.len(), 16);
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
