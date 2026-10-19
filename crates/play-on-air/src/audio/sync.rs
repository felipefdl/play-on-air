//! Cast playout sync: due times and sample nudges for calibrated speakers.
//!
//! A session syncs when its Cast model is in the table below and the egress is live FLAC.
//! Google Cast groups and every other model keep the unsynced autoplay path.

use std::collections::VecDeque;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::registry::CAST_GROUP_MODEL;

/// Output frames between sample drops or duplicates (1000/s at 48 kHz).
pub(crate) const ADJUST_SPACING: u32 = 48;

/// Ignore smoothed error inside this band.
pub(crate) const DRIFT_DEADBAND_SECS: f64 = 0.002;
/// Above this, the drift tick also logs that a re-LOAD step would be faster.
pub(crate) const DRIFT_STEP_SECS: f64 = 0.100;
/// EMA weight of the newest predicted error.
pub(crate) const DRIFT_EMA_ALPHA: f64 = 0.2;
/// Largest nudge requested on one drift tick, in samples.
const DRIFT_MAX_SAMPLES: f64 = 1000.0;

const NEST_AUDIO_MODEL: &str = "Nest Audio";
const NEST_MINI_MODEL: &str = "Google Nest Mini";
/// PLAY-to-audible delay and `currentTime` bias, LIVE FLAC, calibrated in an AirPlay group
/// against the Mac's AirPlay receiver (receiver spec section 7). The PLAY delay includes
/// PlayOnAir's Cast command path.
const NEST_AUDIO_PLAY_MS: i64 = 290;
const NEST_AUDIO_BIAS_MS: i64 = 366;
const NEST_MINI_PLAY_MS: i64 = 270;
const NEST_MINI_BIAS_MS: i64 = 0;

const MAX_TIMELINE_MARKERS: usize = 1024;
const MAX_APPLIED: usize = 32_768;
/// Keep applied adjustments this many seconds behind the encoded head.
const APPLIED_KEEP_SECS: u64 = 30;

/// Per-model PLAY latency and `currentTime` bias for one synced device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedSync {
  /// Registry model string (logged as-is).
  pub(crate) model: String,
  /// PLAY sent to stream position 0 becoming audible.
  pub(crate) play_latency: Duration,
  /// `currentTime` minus audible position, nanoseconds (positive: status runs ahead).
  pub(crate) bias_ns: i64,
}

/// Table lookup. `None` for a Cast group or any model that is not calibrated.
pub(crate) fn resolve_sync(model: &str) -> Option<ResolvedSync> {
  if model == CAST_GROUP_MODEL {
    return None;
  }
  let (play_ms, status_bias_ms) = model_table(model)?;
  let play_latency = Duration::from_millis(u64::try_from(play_ms.max(0)).unwrap_or(0));
  Some(ResolvedSync {
    model: model.to_owned(),
    play_latency,
    bias_ns: status_bias_ms.saturating_mul(1_000_000),
  })
}

fn model_table(model: &str) -> Option<(i64, i64)> {
  match model {
    NEST_AUDIO_MODEL => Some((NEST_AUDIO_PLAY_MS, NEST_AUDIO_BIAS_MS)),
    NEST_MINI_MODEL => Some((NEST_MINI_PLAY_MS, NEST_MINI_BIAS_MS)),
    _ => None,
  }
}

/// What one drift tick asks the encoder to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DriftAction {
  /// Smoothed error is inside the deadband.
  None,
  /// Signed samples (positive drops source frames).
  Adjust(i64),
}

/// `prev * (1 - alpha) + sample * alpha`, or `sample` on the first tick.
pub(crate) fn smooth_ema(prev: Option<f64>, sample: f64) -> f64 {
  prev.map_or(sample, |previous| {
    (1.0 - DRIFT_EMA_ALPHA).mul_add(previous, DRIFT_EMA_ALPHA * sample)
  })
}

/// Deadband or a clamped 0.2·ema·rate nudge. Errors past [`DRIFT_STEP_SECS`] slew at the
/// clamp (about 2 % speed); the drift tick logs that a step would be faster.
pub(crate) fn drift_action(ema_secs: f64, sample_rate: u32) -> DriftAction {
  if ema_secs.abs() > DRIFT_DEADBAND_SECS {
    let raw = 0.2 * ema_secs * f64::from(sample_rate.max(1));
    DriftAction::Adjust(quantize_samples(raw))
  } else {
    DriftAction::None
  }
}

/// `error - pending_frames / sample_rate`.
pub(crate) fn predicted_error(error_secs: f64, pending_frames: i64, sample_rate: u32) -> f64 {
  let hz = f64::from(sample_rate.max(1));
  error_secs - (pending_frames as f64) / hz
}

fn quantize_samples(raw: f64) -> i64 {
  if !raw.is_finite() {
    return 0;
  }
  let clamped = raw.clamp(-DRIFT_MAX_SAMPLES, DRIFT_MAX_SAMPLES).round();
  if !clamped.is_finite() {
    return 0;
  }
  if clamped >= 0.0 {
    i64::try_from(clamped as u64).unwrap_or(0)
  } else {
    let positive = (-clamped) as u64;
    -i64::try_from(positive).unwrap_or(0)
  }
}

/// Move `base` by `frames` at `rate` (negative frames go earlier).
pub(crate) fn offset_instant(base: Instant, frames: i64, rate: u32) -> Instant {
  let hz = u64::from(rate.max(1));
  let abs = u64::try_from(frames.saturating_abs()).unwrap_or(u64::MAX);
  let nanos = abs.saturating_mul(1_000_000_000) / hz;
  let step = Duration::from_nanos(nanos);
  if frames >= 0 {
    base.checked_add(step).unwrap_or(base)
  } else {
    base.checked_sub(step).unwrap_or(base)
  }
}

/// One encoder step while filling a FLAC block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameOp {
  /// Copy this many source frames into the output.
  Copy(usize),
  /// Discard one source frame (speaker was late).
  Drop,
  /// Repeat the previous output frame (speaker was early).
  Dup,
}

/// Spread `budget` across `output_frames` with at most one nudge per [`ADJUST_SPACING`] frames.
pub(crate) fn plan_block(output_frames: usize, mut since: u32, mut budget: i64) -> Vec<FrameOp> {
  let mut ops = Vec::new();
  let mut produced = 0_usize;
  while produced < output_frames {
    if budget > 0 && since >= ADJUST_SPACING {
      ops.push(FrameOp::Drop);
      budget -= 1;
      since = 0;
      continue;
    }
    if budget < 0 && since >= ADJUST_SPACING {
      ops.push(FrameOp::Dup);
      budget += 1;
      since = 0;
      produced += 1;
      continue;
    }
    let room = output_frames - produced;
    let run = if budget == 0 {
      room
    } else {
      usize::try_from(ADJUST_SPACING.saturating_sub(since))
        .unwrap_or(0)
        .max(1)
        .min(room)
    };
    ops.push(FrameOp::Copy(run));
    produced += run;
    since = since.saturating_add(u32::try_from(run).unwrap_or(u32::MAX));
  }
  ops
}

/// Source frames a plan consumes.
pub(crate) fn source_frames(ops: &[FrameOp]) -> usize {
  ops
    .iter()
    .map(|op| match op {
      FrameOp::Copy(n) => *n,
      FrameOp::Drop => 1,
      FrameOp::Dup => 0,
    })
    .sum()
}

/// Net samples the plan applies (drops positive).
pub(crate) fn budget_used(ops: &[FrameOp]) -> i64 {
  let mut net = 0_i64;
  for op in ops {
    match op {
      FrameOp::Drop => net += 1,
      FrameOp::Dup => net -= 1,
      FrameOp::Copy(_) => {},
    }
  }
  net
}

/// `since` after `ops`, matching [`plan_block`]'s spacing counter.
pub(crate) fn since_after(mut since: u32, ops: &[FrameOp]) -> u32 {
  for op in ops {
    match op {
      FrameOp::Drop | FrameOp::Dup => since = 0,
      FrameOp::Copy(n) => since = since.saturating_add(u32::try_from(*n).unwrap_or(u32::MAX)),
    }
  }
  since
}

/// Source-frame offset of the first copied output frame.
pub(crate) fn first_output_source_offset(ops: &[FrameOp]) -> Option<usize> {
  let mut src = 0_usize;
  for op in ops {
    match *op {
      FrameOp::Drop => src += 1,
      FrameOp::Copy(0) | FrameOp::Dup => {},
      FrameOp::Copy(_) => return Some(src),
    }
  }
  None
}

/// Build output PCM from `source` (interleaved) and `ops`. `seed` is the previous frame.
pub(crate) fn apply_frame_ops(source: &[i16], channels: usize, ops: &[FrameOp], seed: &[i16]) -> Option<Vec<i16>> {
  let width = channels.max(1);
  let mut out = Vec::new();
  let mut src_frame = 0_usize;
  for op in ops {
    match *op {
      FrameOp::Drop => {
        src_frame = src_frame.saturating_add(1);
      },
      FrameOp::Dup => {
        if out.len() >= width {
          let start = out.len() - width;
          let frame = out.get(start..)?.to_vec();
          out.extend(frame);
        } else if seed.len() == width {
          out.extend_from_slice(seed);
        } else {
          return None;
        }
      },
      FrameOp::Copy(n) => {
        let from = src_frame.saturating_mul(width);
        let to = from.saturating_add(n.saturating_mul(width));
        out.extend_from_slice(source.get(from..to)?);
        src_frame = src_frame.saturating_add(n);
      },
    }
  }
  Some(out)
}

/// Identifies one HTTP body inside a [`SyncTimeline`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BodyToken(u64);

#[derive(Debug)]
struct TimelineInner {
  sample_rate: u32,
  /// Bumped before each sync LOAD. The body that should supply the next timed start.
  required: u64,
  /// Body currently encoded.
  body: u64,
  preroll: u64,
  real_started: bool,
  d0: Option<Instant>,
  markers: VecDeque<(u64, Instant)>,
  applied: VecDeque<(u64, i8)>,
  applied_net: i64,
  queued: i64,
  encoded: u64,
}

/// Shared stream-frame → due map and nudge queue for one Cast session.
#[derive(Debug)]
pub(crate) struct SyncTimeline {
  device_id: String,
  model: String,
  inner: Mutex<TimelineInner>,
}

impl SyncTimeline {
  pub(crate) fn new(sample_rate: u32, device_id: &str, model: &str) -> Self {
    Self {
      device_id: device_id.to_owned(),
      model: model.to_owned(),
      inner: Mutex::new(TimelineInner {
        sample_rate: sample_rate.max(1),
        required: 0,
        body: 0,
        preroll: 0,
        real_started: false,
        d0: None,
        markers: VecDeque::new(),
        applied: VecDeque::new(),
        applied_net: 0,
        queued: 0,
        encoded: 0,
      }),
    }
  }

  /// Next LOAD should wait for a new body. The body already encoding keeps its markers
  /// until [`Self::begin_body`].
  pub(crate) fn prepare_for_load(&self) {
    let mut inner = self.inner.lock();
    inner.required = inner.required.saturating_add(1);
  }

  /// Reset stream mapping for the HTTP body that is starting. Returns the token that body must pass back.
  pub(crate) fn begin_body(&self) -> BodyToken {
    let mut inner = self.inner.lock();
    inner.body = inner.required;
    inner.preroll = 0;
    inner.real_started = false;
    inner.d0 = None;
    inner.markers.clear();
    inner.applied.clear();
    inner.queued = 0;
    inner.encoded = 0;
    BodyToken(inner.body)
  }

  pub(crate) fn accepts(&self, token: BodyToken) -> bool {
    self.inner.lock().body == token.0
  }

  /// Due time of stream frame 0 for the body a timed start is waiting on.
  pub(crate) fn d0_ready(&self) -> Option<Instant> {
    let inner = self.inner.lock();
    if inner.body == inner.required {
      inner.d0
    } else {
      None
    }
  }

  /// Whether the current body already mapped its first real frame (stream frame 0 is fixed).
  pub(crate) fn real_started(&self) -> bool {
    self.inner.lock().real_started
  }

  pub(crate) fn note_preroll(&self, token: BodyToken, frames: u64) {
    let mut inner = self.inner.lock();
    if inner.body != token.0 || frames == 0 {
      return;
    }
    inner.preroll = inner.preroll.saturating_add(frames);
  }

  /// Record the due time of a real block. The first real block also back-dates silence preroll.
  pub(crate) fn note_real(&self, token: BodyToken, stream_frame: u64, due: Instant) {
    let mut inner = self.inner.lock();
    if inner.body != token.0 {
      return;
    }
    if inner.real_started {
      let duplicate = inner.markers.back().is_some_and(|(idx, _)| *idx == stream_frame);
      if !duplicate {
        inner.markers.push_back((stream_frame, due));
      }
    } else {
      inner.real_started = true;
      let preroll = i64::try_from(inner.preroll).unwrap_or(i64::MAX);
      let d0 = offset_instant(due, -preroll, inner.sample_rate);
      inner.d0 = Some(d0);
      inner.markers.push_back((0, d0));
      if stream_frame > 0 {
        inner.markers.push_back((stream_frame, due));
      }
    }
    trim_markers(&mut inner.markers);
  }

  pub(crate) fn note_emitted(&self, token: BodyToken, encoded: u64) {
    let mut inner = self.inner.lock();
    if inner.body != token.0 {
      return;
    }
    if encoded > inner.encoded {
      inner.encoded = encoded;
    }
  }

  /// Due instant of fractional stream frame `frame` (extrapolates outside the markers).
  pub(crate) fn due_at(&self, frame: f64) -> Option<Instant> {
    if !frame.is_finite() {
      return None;
    }
    let inner = self.inner.lock();
    let (idx, due) = anchor_marker(&inner.markers, frame)?;
    let delta = frame - frame_as_f64(idx);
    let rate = inner.sample_rate;
    drop(inner);
    offset_instant_f64(due, delta, rate)
  }

  pub(crate) fn take_queued(&self) -> i64 {
    let mut inner = self.inner.lock();
    let queued = inner.queued;
    inner.queued = 0;
    queued
  }

  pub(crate) fn restore(&self, samples: i64) {
    if samples == 0 {
      return;
    }
    let mut inner = self.inner.lock();
    inner.queued = inner.queued.saturating_add(samples).clamp(-1000, 1000);
  }

  /// Add this tick's request onto whatever the encoder has not consumed, clamped to one tick.
  pub(crate) fn add_request(&self, samples: i64) {
    if samples == 0 {
      return;
    }
    let mut inner = self.inner.lock();
    inner.queued = inner.queued.saturating_add(samples).clamp(-1000, 1000);
  }

  pub(crate) fn record_applied(&self, token: BodyToken, items: &[(u64, i8)]) {
    if items.is_empty() {
      return;
    }
    let mut inner = self.inner.lock();
    if inner.body != token.0 {
      return;
    }
    for &(frame, sign) in items {
      inner.applied.push_back((frame, sign));
      inner.applied_net = inner.applied_net.saturating_add(i64::from(sign));
    }
    let encoded = inner.encoded;
    let rate = inner.sample_rate;
    trim_applied(&mut inner.applied, encoded, rate);
    drop(inner);
    self.log_adjust(items);
  }

  /// Net applied sign in stream frames `(s_aud, encoded]`.
  pub(crate) fn pending_net(&self, s_aud: f64) -> i64 {
    let mut net = 0_i64;
    let inner = self.inner.lock();
    for &(frame, sign) in &inner.applied {
      if frame_as_f64(frame) > s_aud {
        net = net.saturating_add(i64::from(sign));
      }
    }
    drop(inner);
    net
  }

  pub(crate) fn applied_net(&self) -> i64 {
    self.inner.lock().applied_net
  }

  pub(crate) fn log_start(&self, d0: Instant, play_sent: Instant, late_ms: f64, ack_ms: f64) {
    tracing::info!(
      device = %self.device_id,
      model = %self.model,
      d0_unix_ns = instant_to_unix_ns(d0),
      play_sent_unix_ns = instant_to_unix_ns(play_sent),
      late_ms,
      ack_ms,
      "sync start"
    );
  }

  pub(crate) fn log_tick(&self, tick: &TickLog) {
    tracing::debug!(
      device = %self.device_id,
      t_unix_ns = tick.t_unix_ns,
      current_time = tick.current_time,
      s_aud = tick.s_aud,
      due_unix_ns = tick.due_unix_ns,
      e_ms = tick.e_ms,
      e_pred_ms = tick.e_pred_ms,
      ema_ms = tick.ema_ms,
      request = tick.request,
      applied_total = tick.applied_total,
      "sync tick"
    );
  }

  pub(crate) fn log_step(&self, ema_ms: f64) {
    tracing::info!(device = %self.device_id, ema_ms, "sync step needed");
  }

  fn log_adjust(&self, items: &[(u64, i8)]) {
    tracing::debug!(device = %self.device_id, count = items.len(), "sync adjust");
  }
}

const fn frame_as_f64(frame: u64) -> f64 {
  frame as f64
}

fn trim_markers(markers: &mut VecDeque<(u64, Instant)>) {
  while markers.len() > MAX_TIMELINE_MARKERS {
    let _ = markers.pop_front();
  }
}

fn trim_applied(applied: &mut VecDeque<(u64, i8)>, encoded: u64, rate: u32) {
  let keep_after = encoded.saturating_sub(u64::from(rate).saturating_mul(APPLIED_KEEP_SECS));
  while applied.len() > 1 {
    let obsolete = applied.front().is_some_and(|(frame, _)| *frame < keep_after);
    let over = applied.len() > MAX_APPLIED;
    if !obsolete && !over {
      break;
    }
    let _ = applied.pop_front();
  }
}

fn anchor_marker(markers: &VecDeque<(u64, Instant)>, frame: f64) -> Option<(u64, Instant)> {
  let mut chosen = None;
  for &(idx, due) in markers {
    if frame_as_f64(idx) <= frame {
      chosen = Some((idx, due));
    } else {
      break;
    }
  }
  chosen.or_else(|| markers.front().copied())
}

fn offset_instant_f64(base: Instant, frames: f64, rate: u32) -> Option<Instant> {
  if !frames.is_finite() {
    return None;
  }
  let hz = f64::from(rate.max(1));
  let seconds = frames.abs() / hz;
  let step = Duration::try_from_secs_f64(seconds).ok()?;
  if frames >= 0.0 {
    base.checked_add(step)
  } else {
    base.checked_sub(step)
  }
}

/// Fields of one drift `tick` log line.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TickLog {
  pub(crate) t_unix_ns: i64,
  pub(crate) current_time: f64,
  pub(crate) s_aud: f64,
  pub(crate) due_unix_ns: i64,
  pub(crate) e_ms: f64,
  pub(crate) e_pred_ms: f64,
  pub(crate) ema_ms: f64,
  pub(crate) request: i64,
  pub(crate) applied_total: i64,
}

struct ClockPair {
  instant: Instant,
  unix_ns: i64,
}

fn clock() -> &'static ClockPair {
  static CLOCK: OnceLock<ClockPair> = OnceLock::new();
  CLOCK.get_or_init(|| ClockPair {
    instant: Instant::now(),
    unix_ns: system_unix_ns(),
  })
}

fn system_unix_ns() -> i64 {
  match SystemTime::now().duration_since(UNIX_EPOCH) {
    Ok(elapsed) => i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX),
    Err(err) => -i64::try_from(err.duration().as_nanos()).unwrap_or(i64::MAX),
  }
}

pub(crate) fn instant_to_unix_ns(t: Instant) -> i64 {
  let clock = clock();
  if t >= clock.instant {
    let delta = i64::try_from(t.saturating_duration_since(clock.instant).as_nanos()).unwrap_or(i64::MAX);
    clock.unix_ns.saturating_add(delta)
  } else {
    let delta = i64::try_from(clock.instant.saturating_duration_since(t).as_nanos()).unwrap_or(i64::MAX);
    clock.unix_ns.saturating_sub(delta)
  }
}

/// Signed seconds `later - earlier`.
pub(crate) fn signed_secs(later: Instant, earlier: Instant) -> f64 {
  if later >= earlier {
    later.saturating_duration_since(earlier).as_secs_f64()
  } else {
    -earlier.saturating_duration_since(later).as_secs_f64()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn plan_block_drops_at_most_one_per_48_output_frames() {
    let ops = plan_block(2048, ADJUST_SPACING, 1000);
    let mut since = ADJUST_SPACING;
    let mut produced = 0_usize;
    let mut drops = 0_usize;
    for op in &ops {
      match *op {
        FrameOp::Drop => {
          assert!(since >= ADJUST_SPACING, "drop spacing {since}");
          drops += 1;
          since = 0;
        },
        FrameOp::Dup => panic!("budget was positive"),
        FrameOp::Copy(n) => {
          produced += n;
          since = since.saturating_add(u32::try_from(n).unwrap_or(u32::MAX));
        },
      }
    }
    assert_eq!(produced, 2048);
    assert!(drops >= 1);
    assert!(drops <= 2048 / 48 + 2, "drops {drops}");
    assert_eq!(source_frames(&ops), 2048 + drops);
    assert_eq!(budget_used(&ops), i64::try_from(drops).unwrap_or(0));
  }

  #[test]
  fn plan_block_duplicates_do_not_consume_source() {
    let ops = plan_block(96, ADJUST_SPACING, -10);
    let mut dups = 0_usize;
    for op in &ops {
      if matches!(op, FrameOp::Dup) {
        dups += 1;
      }
    }
    assert!(dups >= 1);
    assert_eq!(source_frames(&ops) + dups, 96);
    let pcm: Vec<i16> = (0..source_frames(&ops)).map(|i| i16::try_from(i).unwrap_or(0)).collect();
    let out = apply_frame_ops(&pcm, 1, &ops, &[0]).expect("apply");
    assert_eq!(out.len(), 96);
  }

  #[test]
  fn drift_action_deadband_step_and_clamp() {
    assert_eq!(drift_action(0.001, 48_000), DriftAction::None);
    assert_eq!(drift_action(0.15, 48_000), DriftAction::Adjust(1000));
    assert_eq!(drift_action(-0.15, 48_000), DriftAction::Adjust(-1000));
    // 0.2 * 0.01 * 48000 = 96
    assert_eq!(drift_action(0.01, 48_000), DriftAction::Adjust(96));
    assert_eq!(drift_action(-0.01, 48_000), DriftAction::Adjust(-96));
    // 0.2 * 0.05 * 48000 = 480, under the cap
    assert_eq!(drift_action(0.05, 48_000), DriftAction::Adjust(480));
    // 0.2 * 0.09 * 48000 = 864
    assert_eq!(drift_action(0.09, 48_000), DriftAction::Adjust(864));
  }

  #[test]
  fn predicted_error_subtracts_pending_drops() {
    let error = predicted_error(0.010, 480, 48_000);
    assert!((error - 0.0).abs() < 1e-9);
  }

  #[test]
  fn ema_starts_at_the_sample_then_smooths() {
    let first = smooth_ema(None, 0.010);
    assert!((first - 0.010).abs() < 1e-12);
    let next = smooth_ema(Some(first), 0.0);
    assert!((next - 0.008).abs() < 1e-12);
  }

  #[test]
  fn preroll_extrapolates_stream_zero() {
    let timeline = SyncTimeline::new(48_000, "dev", NEST_AUDIO_MODEL);
    timeline.prepare_for_load();
    let token = timeline.begin_body();
    timeline.note_preroll(token, 48_000);
    let real = Instant::now();
    timeline.note_real(token, 48_000, real);
    let d0 = timeline.d0_ready().expect("d0");
    let delta = real.saturating_duration_since(d0);
    assert!((delta.as_secs_f64() - 1.0).abs() < 0.001);
    let due = timeline.due_at(0.0).expect("frame 0");
    let gap = due.saturating_duration_since(d0).max(d0.saturating_duration_since(due));
    assert!(gap.as_micros() < 50);
  }

  #[test]
  fn pending_net_is_only_adjustments_after_audible() {
    let timeline = SyncTimeline::new(48_000, "dev", NEST_MINI_MODEL);
    timeline.prepare_for_load();
    let token = timeline.begin_body();
    timeline.record_applied(token, &[(10, 1), (20, 1), (30, -1)]);
    assert_eq!(timeline.pending_net(20.0), -1);
    assert_eq!(timeline.pending_net(9.0), 1);
    assert_eq!(timeline.applied_net(), 1);
  }

  #[test]
  fn resolve_sync_only_for_calibrated_speakers() {
    // Exact advertised `md` strings. A near miss must not borrow another model's timing,
    // and a Cast group stays on the unsynced autoplay path.
    assert!(resolve_sync(NEST_AUDIO_MODEL).is_some());
    assert!(resolve_sync(NEST_MINI_MODEL).is_some());
    assert!(resolve_sync("Nest Mini").is_none());
    assert!(resolve_sync("Google Nest Audio").is_none());
    assert!(resolve_sync(CAST_GROUP_MODEL).is_none());
    assert!(resolve_sync("Chromecast").is_none());
  }
}
