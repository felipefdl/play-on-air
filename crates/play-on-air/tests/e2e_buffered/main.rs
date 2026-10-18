//! End-to-end buffered AirPlay 2 → Cast: a synthetic sender drives a real PlayOnAir
//! receiver, and a fake Cast device on localhost pulls and decodes the FLAC egress.
//!
//! Everything runs in-process on loopback: no mDNS, ephemeral PTP ports (never 319/320,
//! so non-root and beside a host AirPlay receiver), no LAN traffic. The
//! fake device is injected straight into the registry and Cast pool (discovery is not
//! started), and receivers are built with mDNS advertising off.
//!
//! Fixtures (`tests/fixtures/tone_*hz_loop.aac`) are four AAC-LC ADTS frames (4096
//! samples, 44.1 kHz stereo) cut from the middle of a longer encode of a pure tone with
//! a whole number of cycles per 4096 samples, so the four frames loop seamlessly and two
//! loops splice at unit boundaries. Generated (no third-party content) with:
//!
//! ```text
//! ffmpeg -f lavfi -i "sine=frequency=<k*44100/4096>:sample_rate=44100:duration=2" \
//!   -af volume=2 -ac 2 -c:a aac -b:a 128k -f adts tone.adts   # k = 41 and 93
//! # keep ADTS frames 40..44 of tone.adts
//! ```

#![allow(
  clippy::expect_used,
  clippy::unwrap_used,
  clippy::panic,
  clippy::indexing_slicing,
  clippy::unreachable,
  reason = "integration test harness: failures abort the scenario with context"
)]

mod analysis;
mod fake_cast;
mod log_capture;
mod sender;

use std::sync::Arc;
use std::time::{Duration, Instant};

use play_on_air::airplay::{AirPlayManager, AirPlaySessionEvent, ReceiverOptions, stable_raop_port};
use play_on_air::bridge::{Bridge, RingLookup};
use play_on_air::cast::{CastControl, CastPool, CastStreamKind, MediaLoadRequest, PlaybackDevice, SetPlaybackDevices};
use play_on_air::media::CAST_READ_AHEAD;
use play_on_air::registry::{Device, DeviceRegistry};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use self::analysis::{Run, Tone};
use self::fake_cast::{Capture, FakeCast};
use self::sender::{AacLoop, Cursor, FRAME_SAMPLES, SAMPLE_RATE, Sender};

const LOW_TONE: &[u8] = include_bytes!("../fixtures/tone_441hz_loop.aac");
const HIGH_TONE: &[u8] = include_bytes!("../fixtures/tone_1001hz_loop.aac");

/// Frames per pattern unit (one fixture loop).
const FRAMES_PER_UNIT: u32 = 4;
/// Bound for the bridge to start Cast and audio to reach the fake device.
const START_DEADLINE: Duration = Duration::from_secs(20);
/// Warning substrings that mean audio was dropped or playout stalled.
const AUDIO_LOSS_MARKERS: &[&str] = &["overflow", "dropped", "stall", "backstop", "purg", "underrun"];

/// One scenario at a time per process (`cargo test` threads); nextest isolates anyway.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Real receiver stack (AirPlay manager, Cast pool, bridge) wired to a fake Cast device.
struct Harness {
  device_id: String,
  cast: FakeCast,
  airplay: Arc<AirPlayManager>,
  cast_pool: Arc<CastPool>,
  bridge: Arc<Bridge>,
  bridge_task: JoinHandle<()>,
  rtsp_port: u16,
}

impl Harness {
  async fn start(tag: &str) -> Self {
    log_capture::install();
    log_capture::clear();
    let cast = FakeCast::start();
    let device_id = format!("e2e-{tag}-{}", std::process::id());
    let name = format!("E2E {tag}");
    let device = Device::new(&device_id, &name, "127.0.0.1", "localhost", cast.port(), &device_id);

    let registry = Arc::new(DeviceRegistry::new());
    assert!(registry.appear(device.clone()), "device appears");
    let (event_tx, event_rx) = mpsc::unbounded_channel::<AirPlaySessionEvent>();
    let airplay = Arc::new(AirPlayManager::new(Some(event_tx)).with_receiver_options(ReceiverOptions {
      advertise: false,
      ptp_ports: Some((0, 0)),
    }));
    let cast_pool = Arc::new(CastPool::new(None));
    cast_pool.ensure(&device);
    let cast_control: Arc<dyn CastControl> = Arc::<CastPool>::clone(&cast_pool);
    let bridge = Arc::new(Bridge::new(registry, cast_control).with_airplay(Arc::clone(&airplay)));
    let rings: Arc<dyn RingLookup> = Arc::<AirPlayManager>::clone(&airplay);
    let bridge_task = tokio::spawn(Arc::clone(&bridge).run(event_rx, rings));
    airplay.ensure(&device_id, &name).await.expect("start AirPlay receiver");

    Self {
      rtsp_port: stable_raop_port(&device_id),
      device_id,
      cast,
      airplay,
      cast_pool,
      bridge,
      bridge_task,
    }
  }

  /// PCM frames the host ring dropped on overflow for the live stream.
  fn ring_overflow_frames(&self) -> u64 {
    self
      .airplay
      .pcm_ring(&self.device_id)
      .map_or(0, |ring| ring.frames_dropped_overflow())
  }

  /// Latest LOAD capture (the stream a real device would be playing).
  fn latest(&self) -> Option<Capture> {
    self.cast.captures().pop()
  }

  /// Wait until the latest capture holds `seconds` of `tone`.
  async fn wait_for_tone(&self, tone: Tone, seconds: f64, deadline: Duration) {
    let started = Instant::now();
    loop {
      if let Some(capture) = self.latest()
        && analysis::tone_seconds(&analysis::runs(&capture.samples), tone, SAMPLE_RATE) >= seconds
      {
        return;
      }
      assert!(
        started.elapsed() < deadline,
        "{seconds} s of {tone:?} not observed within {deadline:?}; bridge session: {}; host ring frames: {:?}; \
         Cast commands: {:?}; captures: {}; warnings: {:#?}",
        self.bridge.has_session(&self.device_id),
        self.airplay.pcm_ring(&self.device_id).map(|ring| ring.available_frames()),
        self.cast.commands().iter().map(|cmd| cmd.kind.as_str()).collect::<Vec<_>>(),
        describe_captures(&self.cast.captures()),
        log_capture::warnings()
      );
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  }

  async fn stop(self) {
    self.bridge.request_shutdown();
    tokio::time::timeout(Duration::from_secs(12), self.bridge_task)
      .await
      .expect("bridge shuts down")
      .expect("bridge task");
    self.airplay.remove(&self.device_id);
    let pool = self.cast_pool;
    tokio::task::spawn_blocking(move || pool.shutdown())
      .await
      .expect("Cast pool shutdown");
    let cast = self.cast;
    tokio::task::spawn_blocking(move || cast.stop()).await.expect("fake Cast stop");
  }
}

fn describe_captures(captures: &[Capture]) -> String {
  captures
    .iter()
    .map(|capture| {
      let runs = analysis::runs(&capture.samples);
      format!(
        "[{} {} Hz, {} samples, error {:?}, runs {:?}]",
        capture.content_type,
        capture.sample_rate,
        capture.samples.len(),
        capture.error,
        runs.iter().map(|run| (run.tone, run.len)).collect::<Vec<_>>()
      )
    })
    .collect::<Vec<_>>()
    .join(" ")
}

fn assert_no_audio_loss_warnings() {
  let warnings = log_capture::warnings();
  let loss: Vec<&String> = warnings
    .iter()
    .filter(|line| {
      let lower = line.to_lowercase();
      AUDIO_LOSS_MARKERS.iter().any(|marker| lower.contains(marker))
    })
    .collect();
  assert!(loss.is_empty(), "audio loss warnings: {loss:#?}");
}

fn fixtures() -> (AacLoop, AacLoop) {
  let low = AacLoop::from_adts(LOW_TONE);
  let high = AacLoop::from_adts(HIGH_TONE);
  assert_eq!(low.len(), FRAMES_PER_UNIT as usize, "low fixture is one unit");
  assert_eq!(high.len(), FRAMES_PER_UNIT as usize, "high fixture is one unit");
  (low, high)
}

/// AAC frames covering `seconds` of audio.
const fn frames_for(seconds: u32) -> u32 {
  (seconds * SAMPLE_RATE).div_ceil(FRAME_SAMPLES)
}

/// Sender RTP position playing `elapsed` after an anchor at `anchor_rtp`.
fn rtp_after(anchor_rtp: u32, elapsed: Duration) -> u32 {
  let samples = (elapsed.as_secs_f64() * f64::from(SAMPLE_RATE)) as u32;
  anchor_rtp.wrapping_add(samples)
}

/// Received channel-0 sample count at `at` (blocks decoded by then).
fn samples_at(capture: &Capture, at: Instant) -> usize {
  capture
    .marks
    .iter()
    .take_while(|(instant, _)| *instant <= at)
    .last()
    .map_or(0, |(_, total)| *total)
}

/// Playout rate of `capture` relative to real time, from `from` to the last block.
fn realtime_ratio(capture: &Capture, from: Instant) -> f64 {
  let first = capture.marks.iter().find(|(instant, _)| *instant >= from).copied();
  let last = capture.marks.last().copied();
  let (Some((t1, s1)), Some((t2, s2))) = (first, last) else {
    panic!("no FLAC blocks after {from:?}");
  };
  let wall = t2.duration_since(t1).as_secs_f64();
  assert!(wall > 1.0, "observation window too short: {wall:.2} s");
  (s2 - s1) as f64 / f64::from(capture.sample_rate) / wall
}

/// Instant the first sample of `run` arrived.
fn arrival_of(capture: &Capture, sample: usize) -> Instant {
  capture
    .marks
    .iter()
    .find(|(_, total)| *total > sample)
    .map(|(instant, _)| *instant)
    .expect("sample arrived")
}

fn first_tone_run(runs: &[Run], tone: Tone, min_len: usize) -> Option<Run> {
  runs.iter().copied().find(|run| run.tone == Some(tone) && run.len >= min_len)
}

/// (a) The sender bursts 60 s ahead of its anchor: the receiver takes it without
/// throttling or dropping, and the Cast stream plays it in real time, gapless and in
/// order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn burst_ahead_plays_continuously_in_real_time() {
  let _serial = SERIAL.lock().await;
  let harness = Harness::start("burst").await;
  let (low, high) = fixtures();
  let units = analysis::lfsr_units(frames_for(60).div_ceil(FRAMES_PER_UNIT) as usize);
  let mut sender = Sender::connect(harness.rtsp_port).await;

  let start = Cursor { seq: 1_000, rtp: 0x1234_5678 };
  let burst_started = Instant::now();
  let _end = sender
    .send_frames(start, frames_for(60), |i| {
      let unit = (i / FRAMES_PER_UNIT) as usize;
      let tone = if units[unit] == Tone::Low {
        &low
      } else {
        &high
      };
      (tone, (i % FRAMES_PER_UNIT) as usize)
    })
    .await;
  let burst = burst_started.elapsed();
  assert!(burst < Duration::from_secs(5), "receiver throttled a 60 s burst for {burst:?}");
  sender.play_at_now(start.rtp).await;
  let anchored = Instant::now();

  harness.wait_for_tone(Tone::Low, 3.0, START_DEADLINE).await;
  harness.wait_for_tone(Tone::High, 3.0, START_DEADLINE).await;
  // Long enough for a slow CI runner to finish the LOAD burst and show steady real time.
  tokio::time::sleep(Duration::from_secs(10)).await;
  let overflow = harness.ring_overflow_frames();

  let captures = harness.cast.captures();
  assert_eq!(captures.len(), 1, "one LOAD, no re-LOAD: {}", describe_captures(&captures));
  let capture = &captures[0];
  assert_eq!(capture.content_type, "audio/flac", "FLAC egress");
  assert_eq!(capture.sample_rate, SAMPLE_RATE, "no resample");
  assert!(capture.error.is_none(), "stream error: {:?}", capture.error);
  let runs = analysis::runs(&capture.samples);
  let observed = analysis::interior_units(&runs).unwrap_or_else(|err| panic!("{err}; runs: {runs:?}"));
  let sent = analysis::pattern_rle(&units);
  assert!(
    observed.len() >= 4 && analysis::contains_slice(&sent, &observed),
    "output is not a contiguous slice of the sent pattern: observed {observed:?}"
  );
  let onset = runs.iter().find(|run| run.tone.is_some()).expect("tone onset");
  let onset_at = arrival_of(capture, onset.start);
  let rate = f64::from(SAMPLE_RATE);
  let lead_s = CAST_READ_AHEAD.as_secs_f64();
  let ahead_s =
    |at: Instant| (samples_at(capture, at) - onset.start) as f64 / rate - at.duration_since(onset_at).as_secs_f64();
  // The sender queued far ahead, so the fresh LOAD gets the read-ahead as a burst. No PTP
  // grandmaster here: the unlocked fallback presents the anchor sample `Audio-Latency` (2 s)
  // after receipt, so Cast can be CAST_READ_AHEAD - 2 s ahead of real time. How fast the burst
  // fills is bounded by debug-build decode and encode speed (a slow CI runner reached 3.6 s at
  // 3 s), so this only checks that it ran ahead of real time, which a real-time feed never does.
  let expected_ahead = lead_s - 2.0;
  let burst_ahead = ahead_s(onset_at + Duration::from_secs(3));
  assert!(
    burst_ahead >= 1.0,
    "Cast only {burst_ahead:.2} s ahead 3 s after onset; a LOAD burst must run ahead of real time"
  );
  // Once the burst has filled the read-ahead, playout is real time within scheduling jitter of
  // a loaded CI box: releasing the 60 s burst on arrival would read as many times real time (or
  // overflow the host ring). The burst ends at a speed-dependent time, so measure after it.
  let settled_at = capture
    .marks
    .iter()
    .map(|(at, _)| *at)
    .find(|at| *at >= onset_at && ahead_s(*at) >= expected_ahead - 0.5)
    .unwrap_or_else(|| panic!("Cast never reached ~{expected_ahead} s of read-ahead"));
  let window = capture
    .marks
    .last()
    .map_or(Duration::ZERO, |(last, _)| last.duration_since(settled_at));
  assert!(
    window >= Duration::from_secs(3),
    "only {window:?} of playout after the burst settled"
  );
  let ratio = realtime_ratio(capture, settled_at);
  assert!(
    (0.8..=1.25).contains(&ratio),
    "playout rate {ratio:.3}× real time after the burst"
  );
  // Steady state holds Cast near the read-ahead, never unbounded.
  let steady_ahead = ahead_s(Instant::now());
  assert!(
    (expected_ahead - 1.0..=lead_s + 0.5).contains(&steady_ahead),
    "Cast {steady_ahead:.2} s ahead of real time in steady state; expected ~{expected_ahead} s"
  );
  assert!(
    onset_at.duration_since(anchored) < Duration::from_secs(3),
    "first audio reached Cast {:?} after the anchor",
    onset_at.duration_since(anchored)
  );
  assert_eq!(overflow, 0, "host PCM ring dropped {overflow} frames");
  assert_no_audio_loss_warnings();

  sender.teardown().await;
  harness.stop().await;
}

/// (b) Pause (rate 0) keeps the buffered audio; resuming with a new anchor at the paused
/// position continues exactly where playback stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_then_resume_continues_where_it_stopped() {
  let _serial = SERIAL.lock().await;
  let harness = Harness::start("pause").await;
  let (low, high) = fixtures();
  let units = analysis::lfsr_units(frames_for(40).div_ceil(FRAMES_PER_UNIT) as usize);
  let mut sender = Sender::connect(harness.rtsp_port).await;

  let start = Cursor { seq: 7, rtp: 0xFFF0_0000 };
  let _end = sender
    .send_frames(start, frames_for(40), |i| {
      let unit = (i / FRAMES_PER_UNIT) as usize;
      let tone = if units[unit] == Tone::Low {
        &low
      } else {
        &high
      };
      (tone, (i % FRAMES_PER_UNIT) as usize)
    })
    .await;
  sender.play_at_now(start.rtp).await;
  let anchored = Instant::now();
  harness.wait_for_tone(Tone::Low, 1.5, START_DEADLINE).await;
  harness.wait_for_tone(Tone::High, 1.5, START_DEADLINE).await;

  sender.pause().await;
  let paused_at = Instant::now();
  let paused_rtp = rtp_after(start.rtp, paused_at.duration_since(anchored));
  tokio::time::sleep(Duration::from_millis(2_500)).await;
  let resumed_at = Instant::now();
  sender.play_at_now(paused_rtp).await;
  let before = harness.latest().expect("capture");
  // Audio released before rate 0 (up to CAST_READ_AHEAD past its presentation time) may still
  // flow to Cast while paused; Cast holds it paused. Nothing past that may be released: the
  // unlocked fallback presents the anchor sample 2 s after receipt.
  let paused_runs = analysis::runs(&before.samples);
  let onset = paused_runs.iter().find(|run| run.tone.is_some()).expect("tone onset");
  let rate = f64::from(SAMPLE_RATE);
  let released_s = paused_at.duration_since(anchored).as_secs_f64() - 2.0 + CAST_READ_AHEAD.as_secs_f64();
  let release_bound = onset.start + ((released_s + 0.5) * rate) as usize;
  let delivered = samples_at(&before, resumed_at);
  tokio::time::sleep(Duration::from_secs(4)).await;
  let overflow = harness.ring_overflow_frames();

  assert!(
    delivered <= release_bound,
    "{delivered} samples reached Cast by resume; at most {release_bound} were releasable before the pause"
  );
  let commands = harness.cast.commands();
  assert!(
    commands.iter().any(|cmd| cmd.kind == "PAUSE" && cmd.at >= paused_at),
    "Cast PAUSE after rate 0: {commands:?}"
  );
  let captures = harness.cast.captures();
  let sent = analysis::pattern_rle(&units);
  for capture in &captures {
    assert!(capture.error.is_none(), "stream error: {:?}", capture.error);
    let runs = analysis::runs(&capture.samples);
    let observed = analysis::interior_units(&runs).unwrap_or_else(|err| panic!("{err}; runs: {runs:?}"));
    assert!(
      analysis::contains_slice(&sent, &observed),
      "output skipped or repeated audio across pause/resume: observed {observed:?}"
    );
  }
  let last = captures.last().expect("capture");
  let after_resume = samples_at(last, Instant::now()) - samples_at(last, resumed_at);
  assert!(
    after_resume as f64 > 2.0 * f64::from(SAMPLE_RATE),
    "only {after_resume} samples after resume"
  );
  assert_eq!(overflow, 0, "host PCM ring dropped {overflow} frames");
  assert_no_audio_loss_warnings();

  sender.teardown().await;
  harness.stop().await;
}

/// (c) Track skip: rate 0, FLUSHBUFFERED (flush-all), then a new RTP epoch with a new anchor.
/// Nothing from the old track reaches Cast after the flush; the new track starts promptly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flush_then_new_anchor_drops_old_track() {
  let _serial = SERIAL.lock().await;
  let harness = Harness::start("skip").await;
  let (low, high) = fixtures();
  let mut sender = Sender::connect(harness.rtsp_port).await;

  let first = Cursor { seq: 0x00FF_FFF0, rtp: 0x7000_0000 };
  let first_end = sender
    .send_frames(first, frames_for(30), |i| (&low, (i % FRAMES_PER_UNIT) as usize))
    .await;
  sender.play_at_now(first.rtp).await;
  harness.wait_for_tone(Tone::Low, 2.0, START_DEADLINE).await;

  // iOS track skip: rate 0, flush-all up to the next track's first packet, new epoch, anchor.
  sender.pause().await;
  sender.flush_all(first_end).await;
  let flushed_at = Instant::now();
  let second = Cursor { seq: first_end.seq, rtp: 0x0BAD_0000 };
  let _second_end = sender
    .send_frames(second, frames_for(20), |i| (&high, (i % FRAMES_PER_UNIT) as usize))
    .await;
  sender.play_at_now(second.rtp).await;
  harness.wait_for_tone(Tone::High, 2.0, START_DEADLINE).await;
  let overflow = harness.ring_overflow_frames();

  let captures = harness.cast.captures();
  let grace = flushed_at + Duration::from_millis(250);
  let mut high_onset = None;
  let mut high_get = None;
  for capture in &captures {
    assert!(capture.error.is_none(), "stream error: {:?}", capture.error);
    let runs = analysis::runs(&capture.samples);
    let after_flush = samples_at(capture, grace);
    let stale: Vec<&Run> = runs
      .iter()
      .filter(|run| run.tone == Some(Tone::Low) && run.start + run.len > after_flush)
      .collect();
    let stale_samples: usize = stale.iter().map(|run| run.start + run.len - after_flush.max(run.start)).sum();
    assert!(
      stale_samples < analysis::UNIT_SAMPLES,
      "old track reached Cast after the flush: {stale_samples} samples ({stale:?})"
    );
    if let Some(run) = first_tone_run(&runs, Tone::High, analysis::UNIT_SAMPLES) {
      let _onset = high_onset.get_or_insert_with(|| arrival_of(capture, run.start));
      let _get = high_get.get_or_insert_with(|| capture.requested_at.expect("GET time"));
      let late_low = runs
        .iter()
        .filter(|later| later.start > run.start && later.tone == Some(Tone::Low))
        .count();
      assert_eq!(late_low, 0, "old track after new track started: {runs:?}");
    }
  }
  let onset = high_onset.expect("new track reached Cast");
  let skip = onset.duration_since(flushed_at);
  let from_get = onset.saturating_duration_since(high_get.expect("new track GET"));
  assert!(skip < Duration::from_secs(5), "new track took {skip:?} after the flush");
  // The re-LOAD's pull gets the new track's queued audio at once, not silence first.
  assert!(
    from_get < Duration::from_secs(1),
    "new track reached Cast {from_get:?} after the re-LOAD GET"
  );
  assert_eq!(overflow, 0, "host PCM ring dropped {overflow} frames");
  assert_no_audio_loss_warnings();

  sender.teardown().await;
  harness.stop().await;
}

/// Cast pool against the fake device: `SET_PLAYBACK_DEVICES` carries the receiver app session
/// id and the full member list to `receiver-0`, and after the answered session move media
/// control runs on the session host, on the app's new transport (a message to the old one
/// would close the connection), while the old worker no longer owns the session.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multizone_expansion_moves_media_control_to_the_session_host() {
  let _serial = SERIAL.lock().await;
  let cast = FakeCast::start();
  let port = cast.port();
  // Holds the media URL's port open; the fake only connects to it.
  let media = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("media listener");
  let media_url = format!("http://127.0.0.1:{}/stream", media.local_addr().expect("media addr").port());
  let device = Device::new("mz-leader", "Leader", "127.0.0.1", "localhost", port, "mz-leader");
  let pool = Arc::new(CastPool::new(None));
  pool.ensure(&device);

  let flow_pool = Arc::clone(&pool);
  let (session, update, leader_still_owns) = tokio::task::spawn_blocking(move || {
    let session = flow_pool.load("mz-leader", MediaLoadRequest::flac(media_url.clone(), CastStreamKind::Live))?;
    let update = flow_pool.set_playback_devices(
      "mz-leader",
      SetPlaybackDevices {
        devices: vec![
          PlaybackDevice {
            device_id: "mz-leader".to_owned(),
            ip_address: "127.0.0.1".to_owned(),
            port,
          },
          PlaybackDevice {
            device_id: "56cb7fe7e13f325625325c4a304c57fa".to_owned(),
            ip_address: "127.0.0.2".to_owned(),
            port: 8009,
          },
        ],
        session_name: Some("Leader + 1".to_owned()),
      },
    )?;
    let endpoint = update.endpoint.clone().expect("session moved");
    flow_pool.open_session_host("mz-host", &endpoint.ip_address, endpoint.port);
    flow_pool.move_session("mz-leader", "mz-host", endpoint.session_id.as_deref())?;
    let paused = flow_pool.pause("mz-host")?;
    assert_eq!(paused.player_state, rust_cast::channels::media::PlayerState::Paused);
    // A re-LOAD (skip, stall) on the session host joins the running receiver: no LAUNCH.
    drop(flow_pool.load("mz-host", MediaLoadRequest::flac(media_url, CastStreamKind::Live))?);
    let leader_still_owns = flow_pool.pause("mz-leader").is_ok();
    Ok::<_, play_on_air::error::Error>((session, update, leader_still_owns))
  })
  .await
  .expect("join multizone flow")
  .expect("multizone flow");

  let endpoint = update.endpoint.expect("endpoint");
  assert_eq!(endpoint.device_id, fake_cast::GROUP_ENDPOINT_ID);
  assert_eq!(endpoint.port, port);
  assert_eq!(update.failed_devices, Vec::<String>::new());
  assert!(!leader_still_owns, "the leader's worker handed its session over");
  assert!(
    !pool.device_ids().contains(&"mz-host".to_owned()),
    "session hosts are not registry speakers"
  );

  let commands = cast.commands();
  let set = commands
    .iter()
    .find(|cmd| cmd.kind == "SET_PLAYBACK_DEVICES")
    .expect("SET_PLAYBACK_DEVICES sent");
  assert_eq!(set.destination, "receiver-0");
  assert_eq!(set.payload["sessionId"], session.session_id.as_str());
  assert_eq!(set.payload["sessionName"], "Leader + 1");
  assert_eq!(set.payload["deviceList"][1]["deviceId"], "56cb7fe7e13f325625325c4a304c57fa");
  let pauses: Vec<&str> = commands
    .iter()
    .filter(|cmd| cmd.kind == "PAUSE")
    .map(|cmd| cmd.destination.as_str())
    .collect();
  assert_eq!(
    pauses,
    vec!["fake-transport-2"],
    "one PAUSE, from the session host, on the moved app's transport"
  );
  let launches = commands.iter().filter(|cmd| cmd.kind == "LAUNCH").count();
  assert_eq!(launches, 1, "only the first LOAD launches the receiver");
  let loads = commands.iter().filter(|cmd| cmd.kind == "LOAD").count();
  assert_eq!(loads, 2);

  tokio::task::spawn_blocking(move || pool.shutdown())
    .await
    .expect("Cast pool shutdown");
  tokio::task::spawn_blocking(move || cast.stop()).await.expect("fake Cast stop");
  drop(media);
}

/// A moved session the new host does not run (here: the session id the move reported is not
/// there) is not adopted, and the old worker does not take it back: its host no longer plays it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_session_takeover_leaves_no_worker_in_control() {
  let _serial = SERIAL.lock().await;
  let cast = FakeCast::start();
  let port = cast.port();
  let media = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("media listener");
  let media_url = format!("http://127.0.0.1:{}/stream", media.local_addr().expect("media addr").port());
  let device = Device::new("mz-gone", "Gone", "127.0.0.1", "localhost", port, "mz-gone");
  let pool = Arc::new(CastPool::new(None));
  pool.ensure(&device);

  let flow_pool = Arc::clone(&pool);
  let (moved, leader_owns, host_owns) = tokio::task::spawn_blocking(move || {
    drop(flow_pool.load("mz-gone", MediaLoadRequest::flac(media_url, CastStreamKind::Live))?);
    flow_pool.open_session_host("mz-gone-host", "127.0.0.1", port);
    let moved = flow_pool.move_session("mz-gone", "mz-gone-host", Some("not-running-here"));
    let leader_owns = flow_pool.pause("mz-gone").is_ok();
    let host_owns = flow_pool.pause("mz-gone-host").is_ok();
    Ok::<_, play_on_air::error::Error>((moved, leader_owns, host_owns))
  })
  .await
  .expect("join takeover flow")
  .expect("takeover flow");

  let err = moved.expect_err("takeover must fail");
  assert!(err.to_string().contains("no receiver app runs session"), "{err}");
  assert!(!leader_owns, "no hand-back to the old host");
  assert!(!host_owns, "the new host did not adopt a session it does not run");
  assert!(
    !cast.commands().iter().any(|cmd| cmd.kind == "PAUSE"),
    "no PAUSE reached any transport"
  );

  tokio::task::spawn_blocking(move || pool.shutdown())
    .await
    .expect("Cast pool shutdown");
  tokio::task::spawn_blocking(move || cast.stop()).await.expect("fake Cast stop");
  drop(media);
}
