//! Pre-sized interleaved PCM ring buffer for the steady-state audio path.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::Mutex;

use super::sync::{SyncTimeline, offset_instant};

/// Due markers kept for audio still sitting in the ring (one per released chunk).
const MAX_DUE_MARKERS: usize = 2048;

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
  /// Set when [`Self::enable_due_tracking`] has installed a due log.
  tracks_due: AtomicBool,
  /// Absolute frame index → presentation instant. `None` until due tracking is enabled.
  due: Mutex<Option<DueLog>>,
  /// Live FLAC sync timeline, attached for the duration of a synced Cast session.
  timeline: Mutex<Option<Arc<SyncTimeline>>>,
}

#[derive(Debug)]
struct DueLog {
  sample_rate: u32,
  /// Absolute index of the next frame stored.
  next: u64,
  /// Absolute index of the next frame a pop will return.
  head: u64,
  /// `(absolute frame index, due instant)` of chunk starts, oldest first.
  markers: VecDeque<(u64, Instant)>,
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
      tracks_due: AtomicBool::new(false),
      due: Mutex::new(None),
      timeline: Mutex::new(None),
    }
  }

  /// Record presentation instants for pushed frames. `sample_rate` interpolates between markers.
  pub(crate) fn enable_due_tracking(&self, sample_rate: u32) {
    *self.due.lock() = Some(DueLog {
      sample_rate: sample_rate.max(1),
      next: 0,
      head: 0,
      markers: VecDeque::new(),
    });
    self.tracks_due.store(true, Ordering::Release);
  }

  /// Whether [`Self::enable_due_tracking`] ran.
  pub(crate) fn tracks_due(&self) -> bool {
    self.tracks_due.load(Ordering::Acquire)
  }

  /// Attach the session sync timeline the live FLAC body reads.
  pub(crate) fn attach_timeline(&self, timeline: Arc<SyncTimeline>) {
    *self.timeline.lock() = Some(timeline);
  }

  /// Drop the sync timeline (FLAC load failed, or sync does not apply).
  pub(crate) fn detach_timeline(&self) {
    *self.timeline.lock() = None;
  }

  /// Timeline currently attached, when this ring feeds a synced Cast session.
  pub(crate) fn sync_timeline(&self) -> Option<Arc<SyncTimeline>> {
    self.timeline.lock().as_ref().map(Arc::clone)
  }

  /// Channel count.
  pub const fn channels(&self) -> u16 {
    self.channels
  }

  /// Push interleaved f32 samples (`-1.0..1.0` nominal).
  ///
  /// On overflow, oldest samples are dropped (drop-from-head).
  pub fn push_f32(&self, samples: &[f32]) {
    self.push_with_due(samples, None);
  }

  /// [`Self::push_f32`] plus a presentation instant for the first sample of `samples`.
  pub(crate) fn push_f32_at(&self, samples: &[f32], due: Instant) {
    self.push_with_due(samples, Some(due));
  }

  fn push_with_due(&self, samples: &[f32], due: Option<Instant>) {
    if samples.is_empty() {
      return;
    }
    let ch = usize::from(self.channels);
    let mut guard = self.buf.lock();
    let total = guard.len().saturating_add(samples.len());
    let (stored, skipped, dropped_buf) = if total > self.capacity_samples {
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
      (keep.len(), skip, drop_from_buf)
    } else {
      guard.extend(samples.iter().copied());
      (samples.len(), 0, 0)
    };
    drop(guard);
    self.note_stored(dropped_buf, skipped, stored, due);
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
    self.note_pop(take_frames);
    take_frames
  }

  /// [`Self::pop_i16`] plus the absolute index of the first frame returned.
  pub(crate) fn pop_i16_indexed(&self, frame_count: usize, out: &mut Vec<i16>) -> (usize, Option<u64>) {
    let start = self.head_index();
    let frames = self.pop_i16(frame_count, out);
    let index = if frames == 0 { None } else { start };
    (frames, index)
  }

  /// Pop up to `frame_count` frames as interleaved f32 PCM, unconverted.
  ///
  /// Returns the number of **frames** written into `out` (cleared first). Moves audio between
  /// rings without quantizing it; an empty ring is not counted as an egress underrun.
  pub fn pop_f32(&self, frame_count: usize, out: &mut Vec<f32>) -> usize {
    let ch = usize::from(self.channels);
    out.clear();
    let mut guard = self.buf.lock();
    let take_frames = frame_count.min(guard.len() / ch);
    let take_samples = take_frames.saturating_mul(ch);
    out.extend(guard.drain(..take_samples));
    drop(guard);
    self.note_pop(take_frames);
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
    let mut due = self.due.lock();
    if let Some(log) = due.as_mut() {
      log.head = log.next;
      log.trim();
    }
    drop(due);
  }

  /// Presentation instant of absolute frame `frame_index`, from the nearest preceding marker.
  pub(crate) fn due_of(&self, frame_index: u64) -> Option<Instant> {
    let guard = self.due.lock();
    let found = guard.as_ref().and_then(|log| {
      let mut chosen = None;
      for &(idx, due) in &log.markers {
        if idx <= frame_index {
          chosen = Some((idx, due));
        } else {
          break;
        }
      }
      chosen.map(|(idx, due)| {
        let delta = i64::try_from(frame_index.saturating_sub(idx)).unwrap_or(i64::MAX);
        (due, delta, log.sample_rate)
      })
    });
    drop(guard);
    let (due, delta, rate) = found?;
    Some(offset_instant(due, delta, rate))
  }

  pub(crate) fn head_index(&self) -> Option<u64> {
    self.due.lock().as_ref().map(|log| log.head)
  }

  fn note_pop(&self, frames: usize) {
    if frames == 0 || !self.tracks_due.load(Ordering::Acquire) {
      return;
    }
    let mut due = self.due.lock();
    if let Some(log) = due.as_mut() {
      log.head = log.head.saturating_add(u64::try_from(frames).unwrap_or(0));
      log.trim();
    }
    drop(due);
  }

  fn note_stored(&self, dropped_buf: usize, skipped: usize, stored: usize, due: Option<Instant>) {
    if !self.tracks_due.load(Ordering::Acquire) {
      return;
    }
    let mut guard = self.due.lock();
    let Some(log) = guard.as_mut() else {
      return;
    };
    let ch = usize::from(self.channels).max(1);
    let dropped_frames = u64::try_from(dropped_buf / ch).unwrap_or(0);
    log.head = log.head.saturating_add(dropped_frames);
    let stored_frames = u64::try_from(stored / ch).unwrap_or(0);
    if stored_frames == 0 {
      log.trim();
      drop(guard);
      return;
    }
    if let Some(presentation) = due {
      let skipped_frames = i64::try_from(skipped / ch).unwrap_or(i64::MAX);
      let marked = offset_instant(presentation, skipped_frames, log.sample_rate);
      log.markers.push_back((log.next, marked));
    }
    log.next = log.next.saturating_add(stored_frames);
    log.trim();
    drop(guard);
  }
}

impl DueLog {
  fn trim(&mut self) {
    // Keep ~2 s behind the read head: the encoder looks up a block's due time right after
    // popping it, so the marker covering the popped frames must survive that pop.
    let keep_from = self.head.saturating_sub(u64::from(self.sample_rate).saturating_mul(2));
    while self.markers.len() > 1 {
      let obsolete = self
        .markers
        .front()
        .zip(self.markers.get(1))
        .is_some_and(|(_, second)| second.0 <= keep_from);
      let over = self.markers.len() > MAX_DUE_MARKERS;
      if !obsolete && !over {
        break;
      }
      let _ = self.markers.pop_front();
    }
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
  fn pop_f32_moves_samples_unchanged() {
    let ring = PcmRing::new(2, 64);
    ring.push_f32(&[0.1, -0.2, 0.3, -0.4, 0.5]);
    let mut out = Vec::new();
    assert_eq!(ring.pop_f32(8, &mut out), 2, "only complete frames");
    assert_eq!(out, vec![0.1, -0.2, 0.3, -0.4]);
    assert_eq!(ring.pop_f32(8, &mut out), 0);
    assert_eq!(out, Vec::<f32>::new());
    assert_eq!(ring.underrun_polls(), 0, "draining is not an egress underrun");
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

  /// The encoder looks up a block's due time right after popping it: the marker covering
  /// the popped frames must outlive the pop (it was trimmed at the read head before).
  #[test]
  fn due_of_popped_block_survives_the_pop() {
    let ring = PcmRing::new(1, 48_000);
    ring.enable_due_tracking(48_000);
    let start = Instant::now();
    for chunk in 0..8_u32 {
      let at = start + std::time::Duration::from_micros(u64::from(chunk) * 21_333);
      ring.push_f32_at(&[0.0; 1_024], at);
    }
    let (popped, index) = ring.pop_i16_indexed(4_096, &mut Vec::new());
    assert_eq!((popped, index), (4_096, Some(0)));
    assert_eq!(ring.due_of(0), Some(start), "first popped frame keeps its due time");
  }

  #[test]
  fn due_of_interpolates_from_the_preceding_marker() {
    let ring = PcmRing::new(1, 64);
    ring.enable_due_tracking(48_000);
    let start = Instant::now();
    // Fits in the ring, so the marker stays on absolute frame 0 at `start`.
    ring.push_f32_at(&[0.0; 64], start);
    let due = ring.due_of(48).expect("marker");
    let expect = start + std::time::Duration::from_millis(1);
    let gap = due.saturating_duration_since(expect).max(expect.saturating_duration_since(due));
    assert!(gap.as_micros() < 20, "gap {gap:?}");
    assert_eq!(ring.pop_i16_indexed(10, &mut Vec::new()).1, Some(0));
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
