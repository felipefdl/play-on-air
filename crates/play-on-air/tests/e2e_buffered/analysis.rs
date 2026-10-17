//! Signal analysis of the decoded Cast stream: which fixture tone plays where.
//!
//! The fixtures are a 441.4 Hz tone (period ≈ 99.9 samples at 44.1 kHz) and a 1001.3 Hz
//! tone (period ≈ 44.0 samples). Upward zero crossings with hysteresis give one period per
//! interval, so tone boundaries resolve to about one period and gaps (silence, glitches)
//! show up as intervals that fit neither tone.

/// Which fixture an interval of output belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tone {
  /// 441.4 Hz fixture.
  Low,
  /// 1001.3 Hz fixture.
  High,
}

/// A maximal stretch of output classified the same way (`None` = neither tone).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Run {
  pub(crate) tone: Option<Tone>,
  pub(crate) start: usize,
  pub(crate) len: usize,
}

/// Samples per tone-pattern unit (one 4-frame fixture loop).
pub(crate) const UNIT_SAMPLES: usize = 4096;
/// Allowed deviation of a measured run from whole units: crossing quantization (about one
/// low-tone period, ±100) plus half a fade-in transient. A skip or repeat of audio that is
/// not a whole number of units lands well outside it.
const UNIT_TOLERANCE: usize = 400;
/// Hysteresis level for zero crossings (fixtures peak near 5600 in 16-bit).
const CROSSING_LEVEL: i32 = 1_000;
/// Non-tone stretches up to this long are transients, not gaps: a tone switch, or the
/// 30 ms fade-in a receiver applies at every anchored start (spec section 1) dipping below
/// [`CROSSING_LEVEL`]. They hold samples, so they count toward the neighboring runs.
const TRANSIENT_SAMPLES: usize = 1_500;

const fn classify(period: usize) -> Option<Tone> {
  match period {
    85..=115 => Some(Tone::Low),
    38..=50 => Some(Tone::High),
    _ => None,
  }
}

/// Classify `samples` into tone runs; short transients count toward their neighbors.
pub(crate) fn runs(samples: &[i32]) -> Vec<Run> {
  let mut crossings = Vec::new();
  let mut armed = false;
  for (index, &sample) in samples.iter().enumerate() {
    if sample < -CROSSING_LEVEL {
      armed = true;
    } else if armed && sample >= CROSSING_LEVEL {
      crossings.push(index);
      armed = false;
    }
  }
  let mut raw: Vec<Run> = Vec::new();
  let mut push = |tone: Option<Tone>, start: usize, len: usize| match raw.last_mut() {
    Some(last) if last.tone == tone => last.len += len,
    _ => raw.push(Run { tone, start, len }),
  };
  let first = crossings.first().copied().unwrap_or(samples.len());
  if first > 0 {
    push(None, 0, first);
  }
  for pair in crossings.windows(2) {
    push(classify(pair[1] - pair[0]), pair[0], pair[1] - pair[0]);
  }
  let last = crossings.last().copied().unwrap_or(samples.len());
  if last < samples.len() {
    push(None, last, samples.len() - last);
  }

  // Fold transients (short non-tone runs) into their neighbors, half each, so a tone
  // switch or fade-in does not read as a gap and run lengths stay sample-accurate.
  let mut merged: Vec<Run> = Vec::new();
  let mut carry: Option<(usize, usize)> = None;
  for mut run in raw {
    if run.tone.is_none() && run.len <= TRANSIENT_SAMPLES {
      carry = Some(carry.map_or((run.start, run.len), |(start, len)| (start, len + run.len)));
      continue;
    }
    if let Some((start, len)) = carry.take() {
      let to_prev = match merged.last_mut() {
        Some(prev) if prev.tone.is_some() && run.tone.is_some() => {
          prev.len += len / 2;
          len / 2
        },
        _ => 0,
      };
      run.start = start + to_prev;
      run.len += len - to_prev;
    }
    match merged.last_mut() {
      Some(prev) if prev.tone == run.tone => prev.len += run.len,
      _ => merged.push(run),
    }
  }
  if let Some((start, len)) = carry {
    merged.push(Run { tone: None, start, len });
  }
  merged
}

/// Seconds of `tone` in `samples`.
pub(crate) fn tone_seconds(runs: &[Run], tone: Tone, sample_rate: u32) -> f64 {
  runs
    .iter()
    .filter(|run| run.tone == Some(tone))
    .map(|run| run.len)
    .sum::<usize>() as f64
    / f64::from(sample_rate)
}

/// Run-length encoding of a unit pattern: `(tone, units)` per run.
pub(crate) fn pattern_rle(units: &[Tone]) -> Vec<(Tone, usize)> {
  let mut out: Vec<(Tone, usize)> = Vec::new();
  for &tone in units {
    match out.last_mut() {
      Some((last, count)) if *last == tone => *count += 1,
      _ => out.push((tone, 1)),
    }
  }
  out
}

/// Interior tone runs of the output as `(tone, units)`, rejecting gaps and odd lengths.
///
/// The first and last tone runs are partial (start/end of observation) and are dropped;
/// a leading non-tone run (silence preroll) is allowed. Any later non-tone run is a
/// discontinuity, as is a run whose length is not a whole number of pattern units.
pub(crate) fn interior_units(runs: &[Run]) -> Result<Vec<(Tone, usize)>, String> {
  let start = runs.iter().position(|run| run.tone.is_some()).ok_or("no tone in output")?;
  let toned = &runs[start..];
  let end = toned.iter().rposition(|run| run.tone.is_some()).unwrap_or(0);
  let body = &toned[..=end];
  if let Some(gap) = body.iter().find(|run| run.tone.is_none()) {
    return Err(format!("gap of {} samples at output sample {}", gap.len, gap.start));
  }
  if body.len() < 3 {
    return Err(format!("only {} tone runs observed", body.len()));
  }
  body[1..body.len() - 1]
    .iter()
    .map(|run| {
      let units = (run.len + UNIT_SAMPLES / 2) / UNIT_SAMPLES;
      let error = run.len.abs_diff(units * UNIT_SAMPLES);
      if units == 0 || error > UNIT_TOLERANCE {
        Err(format!(
          "run of {} samples at output sample {} is not a whole pattern unit",
          run.len, run.start
        ))
      } else {
        Ok((run.tone.unwrap_or(Tone::Low), units))
      }
    })
    .collect()
}

/// Whether `needle` occurs as a contiguous slice of `haystack`.
pub(crate) fn contains_slice(haystack: &[(Tone, usize)], needle: &[(Tone, usize)]) -> bool {
  needle.is_empty() || haystack.windows(needle.len()).any(|window| window == needle)
}

/// Position-coding tone pattern: one tone per unit from a 9-bit maximal LFSR.
pub(crate) fn lfsr_units(count: usize) -> Vec<Tone> {
  let mut state: u16 = 0x1A5;
  (0..count)
    .map(|_| {
      let bit = ((state >> 8) ^ (state >> 4)) & 1;
      state = ((state << 1) | bit) & 0x1FF;
      if bit == 1 { Tone::High } else { Tone::Low }
    })
    .collect()
}
