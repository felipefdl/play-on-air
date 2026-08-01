//! Axum-based media HTTP server binding `0.0.0.0:0` and serving stream bytes.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use futures_util::stream;
use parking_lot::RwLock;
use tokio::net::TcpListener;
use tokio::sync::{Notify, oneshot};
use tokio::time::sleep;

use crate::audio::{PcmRing, continuous_wav_header};
use crate::error::{Error, Result};

/// Frames requested per `LiveWav` chunk (~21 ms at 48 kHz).
const LIVE_CHUNK_FRAMES: usize = 1024;
/// Sleep when the PCM ring underruns so the HTTP body stays open.
const LIVE_UNDERRUN_SLEEP: Duration = Duration::from_millis(5);
/// Silence frames prepended so Nest/Chromecast can buffer before real PCM.
const SILENCE_PREROLL_FRAMES: usize = 24_000; // ~0.5 s at 48 kHz; scaled by rate in stream
/// Content-Length matches continuous WAV data size + 44-byte header (avoids chunked TE).
pub(crate) const LIVE_CONTENT_LENGTH: u64 = (u32::MAX / 2) as u64 + 44;
/// Stop serving this many bytes before [`LIVE_CONTENT_LENGTH`] so hyper never hits the hard cap.
///
/// Sized for several max PCM chunks (`LIVE_CHUNK_FRAMES` × channels × 2).
const LIVE_ROLLOVER_MARGIN: u64 = 65_536;

/// What the media server currently serves at `/stream`.
#[derive(Debug, Clone, Default)]
pub enum MediaContent {
  /// Finite static body (tests / short clips / optional FLAC snapshot helper).
  Static {
    /// HTTP Content-Type (e.g. `audio/flac`).
    content_type: String,
    /// Full response body.
    body: Bytes,
  },
  /// Continuous interleaved s16le WAV pulled from a live PCM ring.
  LiveWav {
    /// Shared decode ring (AirPlay → bridge).
    ring: Arc<PcmRing>,
    /// Channel count advertised in the WAV header.
    channels: u16,
    /// Sample rate (Hz) advertised in the WAV header.
    sample_rate: u32,
  },
  /// Empty placeholder until a session attaches real bytes.
  #[default]
  Empty,
}

/// Signals that a `LiveWav` body ended near its Content-Length cap and needs Cast re-LOAD.
#[derive(Debug, Default)]
pub struct RolloverSignal {
  count: AtomicU64,
  notify: Notify,
}

impl RolloverSignal {
  /// Bump the rollover counter and wake waiters.
  pub fn signal(&self) {
    let _prev = self.count.fetch_add(1, Ordering::AcqRel);
    self.notify.notify_waiters();
  }

  /// Current rollover count (monotonic for this media server lifetime).
  pub fn count(&self) -> u64 {
    self.count.load(Ordering::Acquire)
  }

  /// Wait until the rollover count is strictly greater than `seen`, then return the new count.
  ///
  /// Registers the `Notify` waiter **before** the second count check so a `signal()` between
  /// the first check and registration cannot be lost (`notify_waiters` stores no permit).
  pub async fn wait_past(&self, seen: u64) -> u64 {
    loop {
      let before = self.count.load(Ordering::Acquire);
      if before > seen {
        return before;
      }
      // Register first, then re-check — standard Notify lost-wakeup avoidance.
      let notified = self.notify.notified();
      let after = self.count.load(Ordering::Acquire);
      if after > seen {
        return after;
      }
      notified.await;
    }
  }
}

#[derive(Clone)]
struct AppState {
  content: Arc<RwLock<MediaContent>>,
  /// Bumped per `LiveWav` GET so the newest request supersedes older bodies.
  ///
  /// Cast clients sometimes probe `/stream` and then issue the real request;
  /// two live bodies popping the same ring would split frames between them.
  live_generation: Arc<AtomicU64>,
  /// Max bytes a single `LiveWav` body will produce before clean end + rollover signal.
  ///
  /// Production default is [`LIVE_CONTENT_LENGTH`]; tests may lower it.
  max_body_bytes: Arc<AtomicU64>,
  /// Bytes emitted by the active `LiveWav` body (reset on each new generation).
  bytes_served: Arc<AtomicU64>,
  /// Notifies the bridge when a `LiveWav` body ends for Content-Length rollover.
  rollover: Arc<RolloverSignal>,
}

/// Running media server handle with public base URL.
#[derive(Debug)]
pub struct MediaServerHandle {
  /// Base URL such as `http://192.168.1.10:54321`.
  pub base_url: String,
  /// Bound local address.
  pub addr: SocketAddr,
  /// Shared content slot for the active stream.
  content: Arc<RwLock<MediaContent>>,
  max_body_bytes: Arc<AtomicU64>,
  bytes_served: Arc<AtomicU64>,
  rollover: Arc<RolloverSignal>,
  shutdown_tx: Option<oneshot::Sender<()>>,
  serve_task: tokio::task::JoinHandle<()>,
}

impl MediaServerHandle {
  /// URL of the continuous media path Cast should load.
  pub fn stream_url(&self) -> String {
    format!("{}/stream", self.base_url)
  }

  /// Replace the body served at `/stream`.
  pub fn set_content(&self, content: MediaContent) {
    *self.content.write() = content;
  }

  /// Shared rollover signal for bridge re-LOAD loops.
  pub fn rollover_signal(&self) -> Arc<RolloverSignal> {
    Arc::clone(&self.rollover)
  }

  /// Bytes emitted by the current `LiveWav` body (observability; reset on each GET generation).
  pub fn bytes_served(&self) -> u64 {
    self.bytes_served.load(Ordering::Acquire)
  }

  /// Override the `LiveWav` body byte cap (tests inject a tiny limit; production keeps default).
  pub fn set_max_body_bytes(&self, max_body_bytes: u64) {
    self.max_body_bytes.store(max_body_bytes.max(44), Ordering::Release);
  }

  /// Shut down the HTTP server task.
  ///
  /// Graceful shutdown alone would wait on in-flight requests, and a `LiveWav`
  /// body never completes — abort so the stream (and its PCM ring) is released.
  pub fn shutdown(self) {
    drop(self);
  }
}

impl Drop for MediaServerHandle {
  fn drop(&mut self) {
    if let Some(tx) = self.shutdown_tx.take() {
      let _sent = tx.send(());
    }
    self.serve_task.abort();
  }
}

/// Factory for ephemeral local media servers.
#[derive(Debug, Default)]
pub struct MediaServer;

impl MediaServer {
  /// Bind `0.0.0.0:0`, spawn the serve loop, return a handle with a LAN-facing URL.
  ///
  /// `advertise_host` is the host/IP embedded in `base_url` (e.g. LAN IPv4 or `127.0.0.1` for tests).
  pub async fn start(advertise_host: &str) -> Result<MediaServerHandle> {
    let listener = TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], 0)))
      .await
      .map_err(|err| Error::Media(format!("bind failed: {err}")))?;

    let addr = listener
      .local_addr()
      .map_err(|err| Error::Media(format!("local_addr failed: {err}")))?;

    let content = Arc::new(RwLock::new(MediaContent::Empty));
    let max_body_bytes = Arc::new(AtomicU64::new(LIVE_CONTENT_LENGTH));
    let bytes_served = Arc::new(AtomicU64::new(0));
    let rollover = Arc::new(RolloverSignal::default());
    let state = AppState {
      content: Arc::clone(&content),
      live_generation: Arc::new(AtomicU64::new(0)),
      max_body_bytes: Arc::clone(&max_body_bytes),
      bytes_served: Arc::clone(&bytes_served),
      rollover: Arc::clone(&rollover),
    };

    let app = Router::new()
      .route("/stream", get(serve_stream))
      .route("/health", get(|| async { StatusCode::OK }))
      .with_state(state);

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let serve_task = tokio::spawn(async move {
      let serve = axum::serve(listener, app).with_graceful_shutdown(async {
        let _shutdown = shutdown_rx.await;
      });
      if let Err(err) = serve.await {
        tracing::error!(error = %err, "media HTTP server exited with error");
      }
    });

    let trimmed = advertise_host.trim();
    let advertise = if trimmed.is_empty() {
      "127.0.0.1"
    } else {
      trimmed
    };
    let base_url = format!("http://{advertise}:{}", addr.port());
    tracing::info!(%base_url, "media HTTP server listening");

    Ok(MediaServerHandle {
      base_url,
      addr,
      content,
      max_body_bytes,
      bytes_served,
      rollover,
      shutdown_tx: Some(shutdown_tx),
      serve_task,
    })
  }
}

async fn serve_stream(State(state): State<AppState>) -> Response {
  let snapshot = state.content.read().clone();
  match snapshot {
    MediaContent::Static { content_type, body } => {
      let mut response = Body::from(body).into_response();
      if let Ok(val) = content_type.parse() {
        drop(response.headers_mut().insert(header::CONTENT_TYPE, val));
      }
      response
    },
    MediaContent::LiveWav { ring, channels, sample_rate } => {
      let generation = state.live_generation.fetch_add(1, Ordering::AcqRel) + 1;
      if generation > 1 {
        tracing::info!(generation, "new LiveWav request supersedes previous body");
      }
      // Fresh body accounting for this GET generation.
      state.bytes_served.store(0, Ordering::Release);
      let max_body_bytes = state.max_body_bytes.load(Ordering::Acquire);
      live_wav_response(
        ring,
        channels,
        sample_rate,
        Arc::clone(&state.live_generation),
        generation,
        max_body_bytes,
        Arc::clone(&state.bytes_served),
        Arc::clone(&state.rollover),
      )
    },
    MediaContent::Empty => (StatusCode::NO_CONTENT, "no stream").into_response(),
  }
}

#[expect(
  clippy::too_many_arguments,
  reason = "live stream wiring carries generation, caps, and rollover signal explicitly"
)]
fn live_wav_response(
  ring: Arc<PcmRing>,
  channels: u16,
  sample_rate: u32,
  live_generation: Arc<AtomicU64>,
  generation: u64,
  max_body_bytes: u64,
  bytes_served: Arc<AtomicU64>,
  rollover: Arc<RolloverSignal>,
) -> Response {
  let ch = channels.max(1);
  let rate = sample_rate.max(1);
  let header = match continuous_wav_header(ch, rate) {
    Ok(h) => h,
    Err(err) => {
      tracing::error!(error = %err, "failed to build continuous WAV header");
      return (StatusCode::INTERNAL_SERVER_ERROR, "wav header").into_response();
    },
  };

  // ~0.5 s of silence at the stream sample rate (Nest needs a buffer burst).
  let preroll_frames = silence_preroll_frames(rate);
  let threshold = live_body_threshold(max_body_bytes);
  tracing::info!(
    channels = ch,
    sample_rate = rate,
    preroll_frames,
    max_body_bytes,
    threshold,
    "Cast client pulling LiveWav stream"
  );

  let stream = live_wav_byte_stream(
    ring,
    ch,
    header,
    preroll_frames,
    live_generation,
    generation,
    threshold,
    bytes_served,
    rollover,
  );
  let mut response = Body::from_stream(stream).into_response();
  drop(
    response
      .headers_mut()
      .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("audio/wav")),
  );
  // Nest/Chromecast often fail on chunked-only progressive audio. Advertise a
  // large Content-Length matching the continuous WAV header data size.
  //
  // The body ends at `threshold` (slightly before this value) so hyper never hits
  // the hard cap mid-chunk; the bridge then re-LOADs for a fresh GET.
  if let Ok(val) = header::HeaderValue::from_str(&LIVE_CONTENT_LENGTH.to_string()) {
    drop(response.headers_mut().insert(header::CONTENT_LENGTH, val));
  }
  // Discourage range probes that restart the progressive body mid-stream.
  drop(
    response
      .headers_mut()
      .insert(header::ACCEPT_RANGES, header::HeaderValue::from_static("none")),
  );
  response
}

/// Byte count at which a `LiveWav` body ends cleanly and signals Cast re-LOAD.
pub(crate) fn live_body_threshold(max_body_bytes: u64) -> u64 {
  let capped = max_body_bytes.max(44);
  let margin = LIVE_ROLLOVER_MARGIN.min(capped / 4);
  capped.saturating_sub(margin).max(44)
}

/// ~0.5 s of silence frames at `sample_rate`.
fn silence_preroll_frames(sample_rate: u32) -> usize {
  // Scale default 48 kHz constant to the stream rate.
  let base = u64::try_from(SILENCE_PREROLL_FRAMES).unwrap_or(24_000);
  let n = (base * u64::from(sample_rate)) / 48_000;
  usize::try_from(n).unwrap_or(SILENCE_PREROLL_FRAMES).max(1024)
}

/// Progressive async stream: WAV header, silence preroll, then PCM from the ring.
#[expect(
  clippy::too_many_arguments,
  reason = "stream state is assembled once; splitting would obscure the data path"
)]
fn live_wav_byte_stream(
  ring: Arc<PcmRing>,
  channels: u16,
  header: [u8; 44],
  preroll_frames: usize,
  live_generation: Arc<AtomicU64>,
  generation: u64,
  threshold: u64,
  bytes_served: Arc<AtomicU64>,
  rollover: Arc<RolloverSignal>,
) -> impl stream::Stream<Item = std::result::Result<Bytes, Infallible>> + Send {
  let header_bytes = Bytes::copy_from_slice(&header);
  let initial = LiveStreamState {
    ring,
    header: Some(header_bytes),
    preroll_frames_left: preroll_frames,
    i16_buf: Vec::with_capacity(LIVE_CHUNK_FRAMES.saturating_mul(usize::from(channels))),
    channels,
    live_generation,
    generation,
    bytes_sent: 0,
    threshold,
    bytes_served,
    rollover,
  };

  stream::unfold(initial, |mut live| async move {
    if live.is_superseded() {
      return None;
    }
    if live.bytes_sent >= live.threshold {
      live.signal_rollover();
      return None;
    }

    if let Some(hdr) = live.header.take() {
      return Some(live.emit_chunk(hdr));
    }

    if live.preroll_frames_left > 0 {
      let n = live.preroll_frames_left.min(LIVE_CHUNK_FRAMES);
      let samples = n.saturating_mul(usize::from(live.channels));
      let chunk_bytes = samples.saturating_mul(2) as u64;
      if live.bytes_sent.saturating_add(chunk_bytes) > live.threshold {
        live.signal_rollover();
        return None;
      }
      live.preroll_frames_left = live.preroll_frames_left.saturating_sub(n);
      let silence = vec![0_i16; samples];
      return Some(live.emit_chunk(i16_slice_to_le_bytes(&silence)));
    }

    // Re-check before pop so a superseded body stops instead of stealing frames.
    if live.is_superseded() {
      return None;
    }
    if live.bytes_sent >= live.threshold {
      live.signal_rollover();
      return None;
    }
    // Budget frames *before* pop so we never drop already-popped PCM at the cap.
    let remaining = live.threshold.saturating_sub(live.bytes_sent);
    let bytes_per_frame = u64::from(live.channels).saturating_mul(2);
    if bytes_per_frame == 0 {
      return None;
    }
    let max_frames_fit = usize::try_from(remaining / bytes_per_frame).unwrap_or(0);
    if max_frames_fit == 0 {
      live.signal_rollover();
      return None;
    }
    let want_frames = LIVE_CHUNK_FRAMES.min(max_frames_fit);
    let frames = live.ring.pop_i16(want_frames, &mut live.i16_buf);
    if frames == 0 {
      // Nest progressive pull underruns if the body stalls. Feed silence so the
      // Cast buffer keeps filling while AirPlay is paused or briefly late.
      // Pace with a short sleep so we do not spin when the client is idle.
      let samples = want_frames.saturating_mul(usize::from(live.channels));
      let chunk_bytes = samples.saturating_mul(2) as u64;
      if live.bytes_sent.saturating_add(chunk_bytes) > live.threshold {
        live.signal_rollover();
        return None;
      }
      sleep(LIVE_UNDERRUN_SLEEP).await;
      if live.is_superseded() {
        return None;
      }
      let silence = vec![0_i16; samples];
      return Some(live.emit_chunk(i16_slice_to_le_bytes(&silence)));
    }
    // After pop: if a newer GET owns the ring, drop this chunk (unavoidable on supersede).
    if live.is_superseded() {
      return None;
    }
    let chunk = i16_slice_to_le_bytes(&live.i16_buf);
    Some(live.emit_chunk(chunk))
  })
}

struct LiveStreamState {
  ring: Arc<PcmRing>,
  header: Option<Bytes>,
  preroll_frames_left: usize,
  i16_buf: Vec<i16>,
  channels: u16,
  live_generation: Arc<AtomicU64>,
  generation: u64,
  bytes_sent: u64,
  threshold: u64,
  bytes_served: Arc<AtomicU64>,
  rollover: Arc<RolloverSignal>,
}

impl LiveStreamState {
  /// True when a newer `/stream` GET owns the ring, so this body must end.
  fn is_superseded(&self) -> bool {
    self.live_generation.load(Ordering::Acquire) != self.generation
  }

  fn signal_rollover(&self) {
    tracing::info!(
      bytes_sent = self.bytes_sent,
      threshold = self.threshold,
      generation = self.generation,
      "LiveWav body ending for Content-Length rollover"
    );
    self.rollover.signal();
  }

  fn emit_chunk(mut self, chunk: Bytes) -> (std::result::Result<Bytes, Infallible>, Self) {
    let n = chunk.len() as u64;
    self.bytes_sent = self.bytes_sent.saturating_add(n);
    self.bytes_served.store(self.bytes_sent, Ordering::Release);
    (Ok(chunk), self)
  }
}

fn i16_slice_to_le_bytes(samples: &[i16]) -> Bytes {
  let mut bytes = Vec::with_capacity(samples.len().saturating_mul(2));
  for &s in samples {
    bytes.extend_from_slice(&s.to_le_bytes());
  }
  Bytes::from(bytes)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn live_body_threshold_keeps_margin_under_content_length() {
    let threshold = live_body_threshold(LIVE_CONTENT_LENGTH);
    assert!(threshold < LIVE_CONTENT_LENGTH);
    assert!(LIVE_CONTENT_LENGTH - threshold <= LIVE_ROLLOVER_MARGIN);
    assert!(threshold >= 44);
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn wait_past_does_not_lose_wakeup_under_concurrent_signal() {
    // Stress the register-then-recheck pattern. A lost wakeup hangs until timeout.
    let signal = Arc::new(RolloverSignal::default());
    for seen in 0..64_u64 {
      let waiter_signal = Arc::clone(&signal);
      let waiter = tokio::spawn(async move { waiter_signal.wait_past(seen).await });
      // Give the waiter a chance to observe `seen` and arm Notify before we signal.
      tokio::task::yield_now().await;
      signal.signal();
      let advanced = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("wait_past must not hang (lost Notify wakeup)")
        .expect("waiter join");
      assert!(advanced > seen, "count must advance past {seen}, got {advanced}");
    }
  }

  #[test]
  fn live_body_threshold_scales_for_tiny_test_caps() {
    let threshold = live_body_threshold(1_000);
    assert!(threshold < 1_000);
    assert!(threshold >= 44);
  }

  #[tokio::test]
  async fn serves_static_flac_bytes() {
    let handle = MediaServer::start("127.0.0.1").await.expect("start");
    let payload = Bytes::from_static(b"fLaCtest");
    handle.set_content(MediaContent::Static {
      content_type: "audio/flac".to_owned(),
      body: payload.clone(),
    });

    let url = handle.stream_url();
    let client = http_get_body(&url, 64 * 1024).await;
    assert_eq!(client, payload.as_ref());
    handle.shutdown();
  }

  #[tokio::test]
  async fn live_wav_serves_wav_magic_when_ring_has_samples() {
    let ring = Arc::new(PcmRing::new(2, 48_000));
    // ~100 ms of stereo silence + tone-ish samples.
    let mut samples = Vec::with_capacity(4800 * 2);
    for n in 0..4800 {
      let t = n as f32 / 48_000.0;
      let s = (2.0 * std::f32::consts::PI * 440.0 * t).sin() * 0.25;
      samples.push(s);
      samples.push(s);
    }
    ring.push_f32(&samples);

    let handle = MediaServer::start("127.0.0.1").await.expect("start");
    assert!(handle.base_url.starts_with("http://127.0.0.1:"));
    handle.set_content(MediaContent::LiveWav {
      ring: Arc::clone(&ring),
      channels: 2,
      sample_rate: 48_000,
    });

    let url = handle.stream_url();
    // Continuous stream: read a prefix and close.
    let body = http_get_body(&url, 512).await;
    assert!(body.len() >= 44, "expected at least WAV header, got {}", body.len());
    assert_eq!(&body[0..4], b"RIFF");
    assert_eq!(&body[8..12], b"WAVE");
    assert_eq!(&body[36..40], b"data");
    handle.shutdown();
  }

  #[tokio::test]
  async fn live_wav_byte_stream_ends_at_threshold_and_signals_rollover() {
    use crate::audio::continuous_wav_header;
    use futures_util::StreamExt;

    let ring = Arc::new(PcmRing::new(2, 4_096));
    let live_generation = Arc::new(AtomicU64::new(1));
    let bytes_served = Arc::new(AtomicU64::new(0));
    let rollover = Arc::new(RolloverSignal::default());
    let header = continuous_wav_header(2, 48_000).expect("header");
    let threshold = live_body_threshold(800);
    let stream = live_wav_byte_stream(
      ring,
      2,
      header,
      silence_preroll_frames(48_000),
      live_generation,
      1,
      threshold,
      Arc::clone(&bytes_served),
      Arc::clone(&rollover),
    );
    tokio::pin!(stream);

    let mut total = 0_u64;
    let mut first_chunk: Option<Bytes> = None;
    while let Some(item) = stream.next().await {
      let chunk = item.expect("infallible stream");
      if first_chunk.is_none() {
        first_chunk = Some(chunk.clone());
      }
      total = total.saturating_add(chunk.len() as u64);
    }

    let header_chunk = first_chunk.expect("at least the WAV header");
    assert!(header_chunk.len() >= 44);
    assert_eq!(&header_chunk[..4], b"RIFF");
    assert!(total >= 44 && total <= threshold, "total={total} threshold={threshold}");
    assert_eq!(rollover.count(), 1, "exactly one rollover signal when body ends at cap");
    assert_eq!(bytes_served.load(Ordering::Acquire), total);
  }

  #[tokio::test]
  async fn live_wav_does_not_discard_pcm_when_nearing_threshold() {
    use crate::audio::continuous_wav_header;
    use futures_util::StreamExt;

    // Header only + tiny PCM budget: old code popped a full chunk then discarded it at cap.
    let ring = Arc::new(PcmRing::new(2, 16_384));
    let frames_pushed = 1_000_usize;
    let mut samples = Vec::with_capacity(frames_pushed.saturating_mul(2));
    for _ in 0..frames_pushed {
      samples.push(0.25);
      samples.push(-0.25);
    }
    ring.push_f32(&samples);

    let live_generation = Arc::new(AtomicU64::new(1));
    let bytes_served = Arc::new(AtomicU64::new(0));
    let rollover = Arc::new(RolloverSignal::default());
    let header = continuous_wav_header(2, 48_000).expect("header");
    // 44-byte header + 100 bytes PCM = 25 stereo frames max after header.
    let threshold = 44 + 100;
    let stream = live_wav_byte_stream(
      Arc::clone(&ring),
      2,
      header,
      0, // no silence preroll — exercise real PCM near the cap
      live_generation,
      1,
      threshold,
      bytes_served,
      Arc::clone(&rollover),
    );
    tokio::pin!(stream);
    let mut total = 0_u64;
    while let Some(item) = stream.next().await {
      total = total.saturating_add(item.expect("infallible").len() as u64);
    }

    assert!(total <= threshold, "total={total} threshold={threshold}");
    assert_eq!(rollover.count(), 1);
    let left = ring.available_frames();
    // 25 frames fit after header; must not have dropped a full 1024-frame pop.
    assert!(
      left > 500,
      "nearing threshold must pop only what fits; discarded PCM left ring empty-ish (left={left})"
    );
  }

  #[tokio::test]
  async fn live_wav_http_with_tiny_cap_signals_rollover() {
    let ring = Arc::new(PcmRing::new(2, 4_096));
    let handle = MediaServer::start("127.0.0.1").await.expect("start");
    // Tiny cap so preroll silence alone ends the body without multi-hour runtime.
    handle.set_max_body_bytes(800);
    handle.set_content(MediaContent::LiveWav { ring, channels: 2, sample_rate: 48_000 });

    let before = handle.rollover_signal().count();
    let rollover = handle.rollover_signal();
    let url = handle.stream_url();

    // Drive the body over HTTP; with a tiny cap the stream ends and signals rollover.
    // Peer may RST when Content-Length exceeds the early-ended body — that is fine;
    // the bridge observes the rollover signal, not a perfect HTTP body length.
    let pull = tokio::spawn(async move {
      drop(http_get_body_until_eof(&url, Duration::from_secs(5)).await);
    });
    let signaled = tokio::time::timeout(Duration::from_secs(5), rollover.wait_past(before)).await;
    assert!(signaled.is_ok(), "HTTP LiveWav must signal rollover under tiny cap");
    assert!(handle.bytes_served() > 0, "server must have emitted at least the WAV header");
    assert!(handle.bytes_served() <= live_body_threshold(800));
    drop(pull.await);
    handle.shutdown();
  }

  #[tokio::test]
  async fn second_live_get_ends_first_body() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let ring = Arc::new(PcmRing::new(2, 4096));
    let handle = MediaServer::start("127.0.0.1").await.expect("start");
    handle.set_content(MediaContent::LiveWav { ring, channels: 2, sample_rate: 48_000 });

    let host_port = format!("127.0.0.1:{}", handle.addr.port());
    let req = format!("GET /stream HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");

    let mut first = TcpStream::connect(&host_port).await.expect("connect first");
    first.write_all(req.as_bytes()).await.expect("write first");
    let mut prefix = [0_u8; 1024];
    let n = first.read(&mut prefix).await.expect("read first prefix");
    assert!(n > 0, "first stream must produce bytes");

    let mut second = TcpStream::connect(&host_port).await.expect("connect second");
    second.write_all(req.as_bytes()).await.expect("write second");
    let mut second_prefix = [0_u8; 1024];
    let m = second.read(&mut second_prefix).await.expect("read second prefix");
    assert!(m > 0, "second stream must produce bytes");

    // The superseded first body must terminate (EOF or reset) instead of
    // continuing to pull PCM from the shared ring.
    let ended = tokio::time::timeout(Duration::from_secs(5), async move {
      let mut sink = [0_u8; 8192];
      loop {
        match first.read(&mut sink).await {
          Ok(0) | Err(_) => break,
          Ok(_) => {},
        }
      }
    })
    .await;
    assert!(ended.is_ok(), "superseded LiveWav body must end");
    // Supersede must not look like a Content-Length rollover.
    assert_eq!(handle.rollover_signal().count(), 0, "supersede must not signal rollover");
    handle.shutdown();
  }

  /// Minimal HTTP GET that returns up to `max_body` decoded body bytes (for infinite streams).
  async fn http_get_body(url: &str, max_body: usize) -> Vec<u8> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let without_scheme = url.strip_prefix("http://").expect("http");
    let (host_port, raw_path) = without_scheme.split_once('/').expect("path");
    let request_path = format!("/{raw_path}");
    let mut stream = TcpStream::connect(host_port).await.expect("connect");
    let req = format!("GET {request_path} HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.expect("write");

    let mut buf = Vec::new();
    let mut tmp = [0_u8; 2048];
    loop {
      let n = stream.read(&mut tmp).await.expect("read");
      if n == 0 {
        break;
      }
      buf.extend_from_slice(&tmp[..n]);
      // Cap total read so LiveWav tests always terminate.
      if buf.len() >= max_body + 8192 {
        break;
      }
      if let Some(split) = find_header_end(&buf) {
        let raw_body = buf.get(split..).unwrap_or(&[]);
        // Enough raw bytes to decode a useful prefix (chunk framing overhead).
        if raw_body.len() >= max_body + 64 {
          break;
        }
      }
    }
    let split = find_header_end(&buf).expect("headers");
    let headers = buf.get(..split).unwrap_or(&[]);
    let raw_body = buf.get(split..).unwrap_or(&[]);
    if headers_indicate_chunked(headers) {
      decode_chunked_prefix(raw_body, max_body)
    } else if raw_body.len() > max_body {
      raw_body[..max_body].to_vec()
    } else {
      raw_body.to_vec()
    }
  }

  /// Read until the server ends the body (EOF), with a timeout.
  ///
  /// Returns `None` if the deadline expires or the peer closes before HTTP headers arrive
  /// (hyper may reset when `Content-Length` exceeds the early-ended `LiveWav` body).
  async fn http_get_body_until_eof(url: &str, timeout: Duration) -> Option<Vec<u8>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let without_scheme = url.strip_prefix("http://").expect("http");
    let (host_port, raw_path) = without_scheme.split_once('/').expect("path");
    let request_path = format!("/{raw_path}");

    tokio::time::timeout(timeout, async {
      let mut stream = TcpStream::connect(host_port).await.expect("connect");
      let req = format!("GET {request_path} HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");
      stream.write_all(req.as_bytes()).await.expect("write");

      let mut buf = Vec::new();
      let mut tmp = [0_u8; 4096];
      loop {
        match stream.read(&mut tmp).await {
          Ok(0) | Err(_) => break,
          Ok(n) => buf.extend_from_slice(&tmp[..n]),
        }
      }
      let split = find_header_end(&buf)?;
      let headers = buf.get(..split).unwrap_or(&[]);
      let raw_body = buf.get(split..).unwrap_or(&[]);
      if headers_indicate_chunked(headers) {
        Some(decode_chunked_prefix(raw_body, raw_body.len()))
      } else {
        Some(raw_body.to_vec())
      }
    })
    .await
    .ok()
    .flatten()
  }

  fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
  }

  fn headers_indicate_chunked(headers: &[u8]) -> bool {
    let text = String::from_utf8_lossy(headers).to_ascii_lowercase();
    text.contains("transfer-encoding: chunked")
  }

  /// Decode enough chunked-transfer data to yield up to `max_body` payload bytes.
  fn decode_chunked_prefix(raw: &[u8], max_body: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(max_body);
    let mut rest = raw;
    while out.len() < max_body {
      let Some(line_end) = rest.windows(2).position(|w| w == b"\r\n") else {
        break;
      };
      let size_line = rest.get(..line_end).unwrap_or(&[]);
      let after_size = rest.get(line_end + 2..).unwrap_or(&[]);
      let size_str = std::str::from_utf8(size_line).unwrap_or("");
      // Ignore chunk extensions after ';'
      let hex = size_str.split(';').next().unwrap_or("").trim();
      let Ok(size) = usize::from_str_radix(hex, 16) else {
        break;
      };
      if size == 0 {
        break;
      }
      if after_size.len() < size + 2 {
        // Incomplete chunk; take what we have.
        let take = size.min(after_size.len()).min(max_body.saturating_sub(out.len()));
        out.extend_from_slice(after_size.get(..take).unwrap_or(&[]));
        break;
      }
      let chunk = after_size.get(..size).unwrap_or(&[]);
      let need = max_body.saturating_sub(out.len());
      let take = chunk.len().min(need);
      out.extend_from_slice(chunk.get(..take).unwrap_or(&[]));
      rest = after_size.get(size + 2..).unwrap_or(&[]);
    }
    out
  }
}
