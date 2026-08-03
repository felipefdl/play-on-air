//! Axum-based media HTTP server binding `0.0.0.0:0` and serving stream bytes.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::serve::ListenerExt;
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::stream;
use parking_lot::RwLock;
use tokio::net::TcpListener;
use tokio::sync::{Notify, oneshot};
use tokio::time::sleep;

use crate::audio::{
  FLAC_BLOCK_SIZE, FlacByteSink, PcmRing, continuous_wav_header, encode_i16_block_to_frame, live_frame_buf,
  live_stream_header_bytes, live_stream_info, round_up_to_flac_blocks, verified_encoder_config,
};
use crate::error::{Error, Result};

// ── Live stream pacing / cushion constants ──────────────────────────────────
//
// Three layers keep Nest from underrunning:
// 1. Bridge prebuffer (~0.5 s real PCM) before Cast LOAD — first real chunks.
// 2. Silence preroll (~2 s) at body start, unpaced — Cast-side cushion; counts
//    toward `frames_emitted` so the pacer starts ~LIVE_LEAD ahead.
// 3. LIVE_LEAD (~2 s) steady-state pace cap — Nest may not pull further ahead.
//
// Shared by `LiveWav` and `LiveFlac`. FLAC has no Content-Length rollover.

/// Frames requested per `LiveWav` chunk (~21 ms at 48 kHz).
const LIVE_CHUNK_FRAMES: usize = 1024;
/// Sleep when the PCM ring underruns so the HTTP body stays open without injecting silence.
const LIVE_UNDERRUN_SLEEP: Duration = Duration::from_millis(5);
/// Silence duration prepended before real PCM (unpaced; TCP as fast as it accepts).
///
/// Counts toward `frames_emitted` so the realtime pacer starts ~[`LIVE_LEAD`] ahead —
/// that **is** the Cast cushion. Do not inject silence mid-stream on underrun.
const SILENCE_PREROLL: Duration = Duration::from_secs(2);
/// How far ahead of wall-clock audio time Nest may pull (maintains the cushion).
const LIVE_LEAD: Duration = Duration::from_secs(2);
/// When the schedule is late by more than this, advance pace origin by the lateness.
const PACE_LATE_SLACK: Duration = Duration::from_millis(50);
/// Minimum interval between ring-overflow drop log lines (hot path stays quiet).
const DROP_LOG_INTERVAL: Duration = Duration::from_secs(1);
/// Content-Length matches continuous WAV data size + 44-byte header (avoids chunked TE).
pub(crate) const LIVE_CONTENT_LENGTH: u64 = (u32::MAX / 2) as u64 + 44;

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
  /// Continuous FLAC pulled from a live PCM ring (chunked; no Content-Length).
  LiveFlac {
    /// Shared decode ring (AirPlay → bridge).
    ring: Arc<PcmRing>,
    /// Channel count for STREAMINFO / encode.
    channels: u16,
    /// Sample rate (Hz) for STREAMINFO / encode.
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
  /// HEAD must not bump this (see [`serve_stream_head`]).
  live_generation: Arc<AtomicU64>,
  /// Max bytes a single `LiveWav` body will produce before clean end + rollover signal.
  ///
  /// Production default is [`LIVE_CONTENT_LENGTH`]; tests may lower it.
  max_body_bytes: Arc<AtomicU64>,
  /// Bytes emitted by the active `LiveWav` body (reset on each new generation).
  bytes_served: Arc<AtomicU64>,
  /// Wall time of the last body write for the current generation.
  last_body_write: Arc<RwLock<Option<Instant>>>,
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
  last_body_write: Arc<RwLock<Option<Instant>>>,
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

  /// Instant of the last body write for the current generation, if any.
  pub fn last_body_write(&self) -> Option<Instant> {
    *self.last_body_write.read()
  }

  /// Cheap progress snapshot: `(bytes_served, last_body_write)` for the current generation.
  pub fn progress(&self) -> (u64, Option<Instant>) {
    (self.bytes_served(), self.last_body_write())
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

    // Disable Nagle so small WAV chunks (header, paced audio) flush promptly to Cast.
    let listener_nodelay = listener.tap_io(|tcp_stream| {
      if let Err(err) = tcp_stream.set_nodelay(true) {
        tracing::trace!(error = %err, "failed to set TCP_NODELAY");
      }
    });

    let content = Arc::new(RwLock::new(MediaContent::Empty));
    let max_body_bytes = Arc::new(AtomicU64::new(LIVE_CONTENT_LENGTH));
    let bytes_served = Arc::new(AtomicU64::new(0));
    let last_body_write = Arc::new(RwLock::new(None));
    let rollover = Arc::new(RolloverSignal::default());
    let state = AppState {
      content: Arc::clone(&content),
      live_generation: Arc::new(AtomicU64::new(0)),
      max_body_bytes: Arc::clone(&max_body_bytes),
      bytes_served: Arc::clone(&bytes_served),
      last_body_write: Arc::clone(&last_body_write),
      rollover: Arc::clone(&rollover),
    };

    let app = Router::new()
      .route("/stream", get(serve_stream).head(serve_stream_head))
      .route("/health", get(|| async { StatusCode::OK }))
      .with_state(state);

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let serve_task = tokio::spawn(async move {
      let serve = axum::serve(listener_nodelay, app).with_graceful_shutdown(async {
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
      last_body_write,
      rollover,
      shutdown_tx: Some(shutdown_tx),
      serve_task,
    })
  }
}

/// GET `/stream` — may supersede a prior live body (generation bump).
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
      *state.last_body_write.write() = None;
      let max_body_bytes = state.max_body_bytes.load(Ordering::Acquire);
      live_wav_response(
        ring,
        channels,
        sample_rate,
        Arc::clone(&state.live_generation),
        generation,
        max_body_bytes,
        Arc::clone(&state.bytes_served),
        Arc::clone(&state.last_body_write),
        Arc::clone(&state.rollover),
      )
    },
    MediaContent::LiveFlac { ring, channels, sample_rate } => {
      let generation = state.live_generation.fetch_add(1, Ordering::AcqRel) + 1;
      if generation > 1 {
        tracing::info!(generation, "new LiveFlac request supersedes previous body");
      }
      state.bytes_served.store(0, Ordering::Release);
      *state.last_body_write.write() = None;
      live_flac_response(
        ring,
        channels,
        sample_rate,
        Arc::clone(&state.live_generation),
        generation,
        Arc::clone(&state.bytes_served),
        Arc::clone(&state.last_body_write),
      )
    },
    MediaContent::Empty => (StatusCode::NOT_FOUND, "no stream").into_response(),
  }
}

/// HEAD `/stream` — same headers as GET, empty body, **no** generation bump.
async fn serve_stream_head(State(state): State<AppState>) -> Response {
  let snapshot = state.content.read().clone();
  match snapshot {
    MediaContent::Static { content_type, body } => {
      let mut response = StatusCode::OK.into_response();
      if let Ok(val) = content_type.parse() {
        drop(response.headers_mut().insert(header::CONTENT_TYPE, val));
      }
      if let Ok(val) = header::HeaderValue::from_str(&body.len().to_string()) {
        drop(response.headers_mut().insert(header::CONTENT_LENGTH, val));
      }
      response
    },
    MediaContent::LiveWav { .. } => {
      let mut response = StatusCode::OK.into_response();
      drop(
        response
          .headers_mut()
          .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("audio/wav")),
      );
      if let Ok(val) = header::HeaderValue::from_str(&LIVE_CONTENT_LENGTH.to_string()) {
        drop(response.headers_mut().insert(header::CONTENT_LENGTH, val));
      }
      drop(
        response
          .headers_mut()
          .insert(header::ACCEPT_RANGES, header::HeaderValue::from_static("none")),
      );
      response
    },
    MediaContent::LiveFlac { .. } => {
      // Empty stream (not empty fixed body) so hyper does not invent Content-Length: 0 —
      // GET is unbounded/chunked and HEAD must match (no Content-Length).
      let mut response = Body::from_stream(stream::empty::<std::result::Result<Bytes, Infallible>>()).into_response();
      *response.status_mut() = StatusCode::OK;
      drop(
        response
          .headers_mut()
          .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("audio/flac")),
      );
      drop(
        response
          .headers_mut()
          .insert(header::ACCEPT_RANGES, header::HeaderValue::from_static("none")),
      );
      let _removed = response.headers_mut().remove(header::CONTENT_LENGTH);
      response
    },
    MediaContent::Empty => (StatusCode::NOT_FOUND, "no stream").into_response(),
  }
}

#[expect(
  clippy::too_many_arguments,
  reason = "live stream wiring carries generation, caps, progress, and rollover explicitly"
)]
fn live_wav_response(
  ring: Arc<PcmRing>,
  channels: u16,
  sample_rate: u32,
  live_generation: Arc<AtomicU64>,
  generation: u64,
  max_body_bytes: u64,
  bytes_served: Arc<AtomicU64>,
  last_body_write: Arc<RwLock<Option<Instant>>>,
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

  let preroll_frames = silence_preroll_frames(rate);
  // Body ends at exactly max_body_bytes (Content-Length); pad the final partial region.
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
    rate,
    header,
    preroll_frames,
    live_generation,
    generation,
    threshold,
    bytes_served,
    last_body_write,
    rollover,
  );
  let mut response = Body::from_stream(stream).into_response();
  drop(
    response
      .headers_mut()
      .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("audio/wav")),
  );
  // Nest/Chromecast often fail on chunked-only progressive audio. Advertise a
  // Content-Length that matches the body end so hyper closes cleanly.
  if let Ok(val) = header::HeaderValue::from_str(&threshold.to_string()) {
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

/// Chunked live FLAC response: STREAMINFO once, silence preroll as FLAC frames, then paced PCM.
///
/// No Content-Length and no rollover — the stream is unbounded until superseded or the peer drops.
#[expect(
  clippy::too_many_arguments,
  reason = "live stream wiring carries generation and progress atomics explicitly"
)]
fn live_flac_response(
  ring: Arc<PcmRing>,
  channels: u16,
  sample_rate: u32,
  live_generation: Arc<AtomicU64>,
  generation: u64,
  bytes_served: Arc<AtomicU64>,
  last_body_write: Arc<RwLock<Option<Instant>>>,
) -> Response {
  let ch = channels.max(1);
  let rate = sample_rate.max(1);
  let stream_info = match live_stream_info(rate, ch) {
    Ok(info) => info,
    Err(err) => {
      tracing::error!(error = %err, "failed to build live FLAC StreamInfo");
      return (StatusCode::INTERNAL_SERVER_ERROR, "flac streaminfo").into_response();
    },
  };
  let header = match live_stream_header_bytes(&stream_info) {
    Ok(h) => h,
    Err(err) => {
      tracing::error!(error = %err, "failed to build live FLAC header");
      return (StatusCode::INTERNAL_SERVER_ERROR, "flac header").into_response();
    },
  };
  let encoder_config = match verified_encoder_config() {
    Ok(cfg) => cfg,
    Err(err) => {
      tracing::error!(error = %err, "failed to build live FLAC encoder config");
      return (StatusCode::INTERNAL_SERVER_ERROR, "flac config").into_response();
    },
  };
  let framebuf = match live_frame_buf(ch) {
    Ok(fb) => fb,
    Err(err) => {
      tracing::error!(error = %err, "failed to allocate live FLAC FrameBuf");
      return (StatusCode::INTERNAL_SERVER_ERROR, "flac framebuf").into_response();
    },
  };

  // Fixed-block live stream: round silence preroll up so every frame is FLAC_BLOCK_SIZE.
  let preroll_frames = round_up_to_flac_blocks(silence_preroll_frames(rate));
  tracing::info!(
    channels = ch,
    sample_rate = rate,
    preroll_frames,
    flac_block_size = FLAC_BLOCK_SIZE,
    "Cast client pulling LiveFlac stream"
  );

  let stream = live_flac_byte_stream(
    ring,
    ch,
    rate,
    header,
    preroll_frames,
    stream_info,
    encoder_config,
    framebuf,
    live_generation,
    generation,
    bytes_served,
    last_body_write,
  );
  let mut response = Body::from_stream(stream).into_response();
  drop(
    response
      .headers_mut()
      .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("audio/flac")),
  );
  // Chunked transfer: do not set Content-Length. Unbounded stream (no rollover).
  drop(
    response
      .headers_mut()
      .insert(header::ACCEPT_RANGES, header::HeaderValue::from_static("none")),
  );
  response
}

/// Byte count at which a `LiveWav` body ends cleanly and signals Cast re-LOAD.
///
/// Equals the Content-Length: the body is padded with silence up to this boundary.
pub(crate) fn live_body_threshold(max_body_bytes: u64) -> u64 {
  max_body_bytes.max(44)
}

/// Silence frames for [`SILENCE_PREROLL`] at `sample_rate` (integer math, no float).
fn silence_preroll_frames(sample_rate: u32) -> usize {
  let rate = u64::from(sample_rate.max(1));
  let millis = u64::try_from(SILENCE_PREROLL.as_millis()).unwrap_or(0);
  // frames = rate * millis / 1000
  let n = rate.saturating_mul(millis) / 1_000;
  usize::try_from(n).unwrap_or(usize::MAX)
}

/// Progressive async stream: WAV header, silence preroll, then paced PCM from the ring.
///
/// Nest BUFFERED pull can drain the ring faster than AirPlay fills it. We must **not**
/// inject silence on underrun (that becomes constant audible cuts). Instead: wait for
/// real PCM, and pace emission so Nest stays at most [`LIVE_LEAD`] ahead of realtime.
#[expect(
  clippy::too_many_arguments,
  reason = "stream state is assembled once; splitting would obscure the data path"
)]
fn live_wav_byte_stream(
  ring: Arc<PcmRing>,
  channels: u16,
  sample_rate: u32,
  header: [u8; 44],
  preroll_frames: usize,
  live_generation: Arc<AtomicU64>,
  generation: u64,
  threshold: u64,
  bytes_served: Arc<AtomicU64>,
  last_body_write: Arc<RwLock<Option<Instant>>>,
  rollover: Arc<RolloverSignal>,
) -> impl stream::Stream<Item = std::result::Result<Bytes, Infallible>> + Send {
  let header_bytes = Bytes::copy_from_slice(&header);
  let chunk_cap = LIVE_CHUNK_FRAMES.saturating_mul(usize::from(channels)).saturating_mul(2);
  let initial = LiveStreamState {
    ring,
    header: Some(header_bytes),
    preroll_frames_left: preroll_frames,
    i16_buf: Vec::with_capacity(LIVE_CHUNK_FRAMES.saturating_mul(usize::from(channels))),
    bytes_buf: BytesMut::with_capacity(chunk_cap),
    channels,
    sample_rate: sample_rate.max(1),
    live_generation,
    generation,
    bytes_sent: 0,
    frames_emitted: 0,
    pace_origin: None,
    threshold,
    bytes_served,
    last_body_write,
    rollover,
    last_drop_log: None,
    last_drops_seen: 0,
  };

  stream::unfold(initial, |mut live| async move {
    if live.is_superseded() {
      return None;
    }
    // Cap already reached (and rollover signaled) on the previous emit — end cleanly.
    if live.bytes_sent >= live.threshold {
      return None;
    }

    if let Some(hdr) = live.header.take() {
      return Some(live.emit_chunk(hdr, 0));
    }

    if live.preroll_frames_left > 0 {
      let remaining = live.threshold.saturating_sub(live.bytes_sent);
      let bytes_per_frame = u64::from(live.channels).saturating_mul(2);
      if bytes_per_frame == 0 {
        return None;
      }
      let max_frames_fit = usize::try_from(remaining / bytes_per_frame).unwrap_or(0);
      if max_frames_fit == 0 {
        return Some(live.emit_silence_pad(remaining));
      }
      let n = live.preroll_frames_left.min(LIVE_CHUNK_FRAMES).min(max_frames_fit);
      let samples = n.saturating_mul(usize::from(live.channels));
      live.preroll_frames_left = live.preroll_frames_left.saturating_sub(n);
      live.i16_buf.clear();
      live.i16_buf.resize(samples, 0);
      // Preroll silence does not sleep — Nest should buffer it fast.
      // Counts toward frames_emitted so the pacer starts ~LIVE_LEAD ahead.
      let chunk = live.i16_to_le_bytes();
      return Some(live.emit_chunk(chunk, n));
    }

    loop {
      // Supersede check BEFORE pop so we never drop already-popped PCM.
      if live.is_superseded() {
        return None;
      }
      if live.bytes_sent >= live.threshold {
        return None;
      }
      live.maybe_log_ring_drops();

      let remaining = live.threshold.saturating_sub(live.bytes_sent);
      let bytes_per_frame = u64::from(live.channels).saturating_mul(2);
      if bytes_per_frame == 0 {
        return None;
      }
      let max_frames_fit = usize::try_from(remaining / bytes_per_frame).unwrap_or(0);
      if max_frames_fit == 0 {
        // Partial-frame region at Content-Length: pad zeros so hyper sees exact CL.
        return Some(live.emit_silence_pad(remaining));
      }
      let want_frames = LIVE_CHUNK_FRAMES.min(max_frames_fit);
      // Check supersede again immediately before pop (no work between check and take).
      if live.is_superseded() {
        return None;
      }
      let frames = live.ring.pop_i16(want_frames, &mut live.i16_buf);
      if frames == 0 {
        // Wait for real PCM. Injecting silence here caused constant Nest Mini cuts.
        sleep(LIVE_UNDERRUN_SLEEP).await;
        continue;
      }
      live.pace_realtime(frames).await;
      let chunk = live.i16_to_le_bytes();
      return Some(live.emit_chunk(chunk, frames));
    }
  })
}

/// Progressive async FLAC stream: STREAMINFO, silence preroll frames, then paced PCM blocks.
///
/// Unbounded (no Content-Length / no rollover). Full [`FLAC_BLOCK_SIZE`] blocks in steady state;
/// wait for real PCM on underrun (no mid-stream silence inject). Generation check **before** pop.
#[expect(
  clippy::too_many_arguments,
  reason = "stream state is assembled once; splitting would obscure the data path"
)]
fn live_flac_byte_stream(
  ring: Arc<PcmRing>,
  channels: u16,
  sample_rate: u32,
  header: Vec<u8>,
  preroll_frames: usize,
  stream_info: flacenc::component::StreamInfo,
  encoder_config: flacenc::error::Verified<flacenc::config::Encoder>,
  framebuf: flacenc::source::FrameBuf,
  live_generation: Arc<AtomicU64>,
  generation: u64,
  bytes_served: Arc<AtomicU64>,
  last_body_write: Arc<RwLock<Option<Instant>>>,
) -> impl stream::Stream<Item = std::result::Result<Bytes, Infallible>> + Send {
  let ch = channels.max(1);
  let initial = LiveFlacState {
    ring,
    header: Some(Bytes::from(header)),
    preroll_frames_left: preroll_frames,
    i16_buf: Vec::with_capacity(FLAC_BLOCK_SIZE.saturating_mul(usize::from(ch))),
    i32_scratch: Vec::with_capacity(FLAC_BLOCK_SIZE.saturating_mul(usize::from(ch))),
    frame_sink: FlacByteSink::new(),
    frame_bytes: BytesMut::new(),
    channels: ch,
    sample_rate: sample_rate.max(1),
    live_generation,
    generation,
    bytes_sent: 0,
    frames_emitted: 0,
    pace_origin: None,
    bytes_served,
    last_body_write,
    last_drop_log: None,
    last_drops_seen: 0,
    stream_info,
    encoder_config,
    framebuf,
    frame_number: 0,
  };

  stream::unfold(initial, |mut live| async move {
    if live.is_superseded() {
      return None;
    }

    if let Some(hdr) = live.header.take() {
      return Some(live.emit_chunk(hdr, 0));
    }

    if live.preroll_frames_left > 0 {
      // Fixed-size blocks: preroll is rounded up so this is always a full block.
      let n = live.preroll_frames_left.min(FLAC_BLOCK_SIZE);
      live.preroll_frames_left = live.preroll_frames_left.saturating_sub(n);
      let samples = n.saturating_mul(usize::from(live.channels));
      live.i16_buf.clear();
      live.i16_buf.resize(samples, 0);
      // Preroll silence does not sleep — Nest should buffer it fast.
      // Counts toward frames_emitted so the pacer starts ~LIVE_LEAD ahead.
      match live.encode_current_i16_buf() {
        Ok(chunk) => return Some(live.emit_chunk(chunk, n)),
        Err(err) => {
          tracing::error!(error = %err, generation = live.generation, "LiveFlac preroll encode failed");
          return None;
        },
      }
    }

    loop {
      // Supersede check BEFORE pop so we never drop already-popped PCM.
      if live.is_superseded() {
        return None;
      }
      live.maybe_log_ring_drops();

      // Steady state: wait for a full FLAC block of real PCM (no short mid-stream frames).
      if live.ring.available_frames() < FLAC_BLOCK_SIZE {
        sleep(LIVE_UNDERRUN_SLEEP).await;
        continue;
      }
      if live.is_superseded() {
        return None;
      }
      let frames = live.ring.pop_i16(FLAC_BLOCK_SIZE, &mut live.i16_buf);
      if frames == 0 {
        // Wait for real PCM. Injecting silence here would be audible cuts.
        sleep(LIVE_UNDERRUN_SLEEP).await;
        continue;
      }
      if frames < FLAC_BLOCK_SIZE {
        // STREAMINFO is fixed min=max=FLAC_BLOCK_SIZE: never encode a short mid-stream frame.
        // Supersede race after pop: end body without encoding (avoid short frame on dead socket).
        // Non-superseded partial is a rare TOCTOU after available_frames; re-push is unavailable
        // and mid-stream silence padding is forbidden — drop the partial samples and wait for a
        // full block (unbounded live stream has no legitimate last short frame until disconnect).
        if live.is_superseded() {
          return None;
        }
        tracing::debug!(
          frames,
          generation = live.generation,
          "LiveFlac dropping partial pop below FLAC_BLOCK_SIZE (fixed STREAMINFO)"
        );
        continue;
      }
      // Re-check supersede before encode so a superseded body does not emit another full frame.
      if live.is_superseded() {
        return None;
      }
      live.pace_realtime(frames).await;
      match live.encode_current_i16_buf() {
        Ok(chunk) => return Some(live.emit_chunk(chunk, frames)),
        Err(err) => {
          tracing::error!(error = %err, generation = live.generation, "LiveFlac frame encode failed");
          return None;
        },
      }
    }
  })
}

struct LiveFlacState {
  ring: Arc<PcmRing>,
  header: Option<Bytes>,
  preroll_frames_left: usize,
  i16_buf: Vec<i16>,
  i32_scratch: Vec<i32>,
  /// Reusable flacenc bit sink (cleared each frame).
  frame_sink: FlacByteSink,
  /// Reusable frame byte buffer; `split().freeze()` for each HTTP chunk.
  frame_bytes: BytesMut,
  channels: u16,
  sample_rate: u32,
  live_generation: Arc<AtomicU64>,
  generation: u64,
  bytes_sent: u64,
  /// PCM frames emitted including silence preroll (used for realtime pacing).
  frames_emitted: u64,
  pace_origin: Option<Instant>,
  bytes_served: Arc<AtomicU64>,
  last_body_write: Arc<RwLock<Option<Instant>>>,
  last_drop_log: Option<Instant>,
  last_drops_seen: u64,
  stream_info: flacenc::component::StreamInfo,
  encoder_config: flacenc::error::Verified<flacenc::config::Encoder>,
  framebuf: flacenc::source::FrameBuf,
  frame_number: usize,
}

impl LiveFlacState {
  fn is_superseded(&self) -> bool {
    self.live_generation.load(Ordering::Acquire) != self.generation
  }

  fn encode_current_i16_buf(&mut self) -> Result<Bytes> {
    encode_i16_block_to_frame(
      &self.i16_buf,
      self.channels,
      self.frame_number,
      &self.encoder_config,
      &self.stream_info,
      &mut self.framebuf,
      &mut self.i32_scratch,
      &mut self.frame_sink,
      &mut self.frame_bytes,
    )?;
    self.frame_number = self.frame_number.saturating_add(1);
    Ok(self.frame_bytes.split().freeze())
  }

  fn maybe_log_ring_drops(&mut self) {
    let drops = self.ring.frames_dropped_overflow();
    if drops <= self.last_drops_seen {
      return;
    }
    let now = Instant::now();
    let due = self
      .last_drop_log
      .is_none_or(|t| now.saturating_duration_since(t) >= DROP_LOG_INTERVAL);
    if !due {
      return;
    }
    let delta = drops.saturating_sub(self.last_drops_seen);
    tracing::warn!(
      frames_dropped = delta,
      frames_dropped_total = drops,
      underrun_polls = self.ring.underrun_polls(),
      occupancy = self.ring.occupancy_frames(),
      generation = self.generation,
      "PCM ring overflow drops while serving LiveFlac"
    );
    self.last_drops_seen = drops;
    self.last_drop_log = Some(now);
  }

  async fn pace_realtime(&mut self, next_frames: usize) {
    pace_realtime_shared(
      self.frames_emitted,
      &mut self.pace_origin,
      self.sample_rate,
      next_frames,
      &self.ring,
      FLAC_BLOCK_SIZE,
    )
    .await;
  }

  fn emit_chunk(mut self, chunk: Bytes, pcm_frames: usize) -> (std::result::Result<Bytes, Infallible>, Self) {
    let n = chunk.len() as u64;
    self.bytes_sent = self.bytes_sent.saturating_add(n);
    self.bytes_served.store(self.bytes_sent, Ordering::Release);
    *self.last_body_write.write() = Some(Instant::now());
    if pcm_frames > 0 {
      self.frames_emitted = self.frames_emitted.saturating_add(u64::try_from(pcm_frames).unwrap_or(0));
    }
    (Ok(chunk), self)
  }
}

struct LiveStreamState {
  ring: Arc<PcmRing>,
  header: Option<Bytes>,
  preroll_frames_left: usize,
  i16_buf: Vec<i16>,
  /// Reusable LE byte buffer (avoids per-chunk `Vec` alloc in steady state).
  bytes_buf: BytesMut,
  channels: u16,
  sample_rate: u32,
  live_generation: Arc<AtomicU64>,
  generation: u64,
  bytes_sent: u64,
  /// PCM frames emitted including silence preroll (used for realtime pacing).
  frames_emitted: u64,
  /// Wall clock when the first paced chunk was scheduled.
  pace_origin: Option<Instant>,
  threshold: u64,
  bytes_served: Arc<AtomicU64>,
  last_body_write: Arc<RwLock<Option<Instant>>>,
  rollover: Arc<RolloverSignal>,
  last_drop_log: Option<Instant>,
  last_drops_seen: u64,
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

  /// Encode `i16_buf` into `bytes_buf` and freeze a `Bytes` chunk.
  fn i16_to_le_bytes(&mut self) -> Bytes {
    self.bytes_buf.clear();
    let need = self.i16_buf.len().saturating_mul(2);
    self.bytes_buf.reserve(need);
    for &sample in &self.i16_buf {
      self.bytes_buf.put_i16_le(sample);
    }
    self.bytes_buf.split().freeze()
  }

  /// Zero-pad up to `byte_count` (final partial region before Content-Length).
  fn emit_silence_pad(mut self, byte_count: u64) -> (std::result::Result<Bytes, Infallible>, Self) {
    let n = usize::try_from(byte_count).unwrap_or(0);
    self.bytes_buf.clear();
    self.bytes_buf.resize(n, 0);
    let chunk = self.bytes_buf.split().freeze();
    self.emit_chunk(chunk, 0)
  }

  /// Rate-limited overflow drop log (consumer side; never per-push).
  fn maybe_log_ring_drops(&mut self) {
    let drops = self.ring.frames_dropped_overflow();
    if drops <= self.last_drops_seen {
      return;
    }
    let now = Instant::now();
    let due = self
      .last_drop_log
      .is_none_or(|t| now.saturating_duration_since(t) >= DROP_LOG_INTERVAL);
    if !due {
      return;
    }
    let delta = drops.saturating_sub(self.last_drops_seen);
    tracing::warn!(
      frames_dropped = delta,
      frames_dropped_total = drops,
      underrun_polls = self.ring.underrun_polls(),
      occupancy = self.ring.occupancy_frames(),
      generation = self.generation,
      "PCM ring overflow drops while serving LiveWav"
    );
    self.last_drops_seen = drops;
    self.last_drop_log = Some(now);
  }

  /// Hold Nest pull so it stays at most [`LIVE_LEAD`] ahead of wall-clock audio time.
  ///
  /// Shared with [`LiveFlacState`] via [`pace_realtime_shared`].
  async fn pace_realtime(&mut self, next_frames: usize) {
    pace_realtime_shared(
      self.frames_emitted,
      &mut self.pace_origin,
      self.sample_rate,
      next_frames,
      &self.ring,
      LIVE_CHUNK_FRAMES,
    )
    .await;
  }

  fn emit_chunk(mut self, chunk: Bytes, pcm_frames: usize) -> (std::result::Result<Bytes, Infallible>, Self) {
    let before = self.bytes_sent;
    let n = chunk.len() as u64;
    self.bytes_sent = before.saturating_add(n);
    self.bytes_served.store(self.bytes_sent, Ordering::Release);
    *self.last_body_write.write() = Some(Instant::now());
    if pcm_frames > 0 {
      self.frames_emitted = self.frames_emitted.saturating_add(u64::try_from(pcm_frames).unwrap_or(0));
    }
    // Hyper stops polling once Content-Length is satisfied, so the next unfold tick
    // may never run — signal rollover when this chunk crosses the cap.
    if before < self.threshold && self.bytes_sent >= self.threshold {
      self.signal_rollover();
    }
    (Ok(chunk), self)
  }
}

/// Shared realtime pacing for `LiveWav` and `LiveFlac`.
///
/// When `now` is past the scheduled wake:
/// - no sleep (emit immediately);
/// - if late beyond [`PACE_LATE_SLACK`] **and** the ring has no backlog, advance the
///   pace origin by the lateness so the schedule reflects reality;
/// - if the ring still holds backlog (consumer was stalled while AirPlay filled), keep
///   the origin fixed so successive ticks stay unpaced until `frames_emitted` catches
///   wall clock — that rushes Nest's cushion back up; pacing re-engages when current.
async fn pace_realtime_shared(
  frames_emitted: u64,
  pace_origin: &mut Option<Instant>,
  sample_rate: u32,
  next_frames: usize,
  ring: &PcmRing,
  backlog_threshold: usize,
) {
  let origin = *pace_origin.get_or_insert_with(Instant::now);
  let now = Instant::now();
  let plan = pace_plan(
    frames_emitted,
    next_frames,
    sample_rate,
    origin,
    now,
    LIVE_LEAD,
    PACE_LATE_SLACK,
  );
  if let Some(lateness) = plan.rebaseline {
    // With backlog, prefer unpaced catch-up over snapping the schedule.
    let has_backlog = ring.occupancy_frames() >= backlog_threshold;
    if !has_backlog && let Some(origin_mut) = pace_origin.as_mut() {
      *origin_mut += lateness;
    }
  }
  if let Some(delay) = plan.sleep {
    sleep(delay).await;
  }
}

/// Pure pacing decision for unit tests and [`pace_realtime_shared`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PacePlan {
  /// Sleep this long before emitting (schedule is early).
  sleep: Option<Duration>,
  /// Advance pace origin by this lateness (schedule is late beyond slack).
  rebaseline: Option<Duration>,
}

/// Compute sleep / rebaseline for realtime pacing with a maintained lead.
///
/// Drift-free: audio time is `total_frames_after * 1000 / sample_rate` from origin.
#[expect(
  clippy::too_many_arguments,
  reason = "pure helper takes explicit schedule inputs for unit tests"
)]
fn pace_plan(
  frames_emitted: u64,
  next_frames: usize,
  sample_rate: u32,
  origin: Instant,
  now: Instant,
  live_lead: Duration,
  late_slack: Duration,
) -> PacePlan {
  let rate = u64::from(sample_rate.max(1));
  let total_after = frames_emitted.saturating_add(u64::try_from(next_frames).unwrap_or(0));
  let audio_ms = total_after.saturating_mul(1000) / rate;
  let due = origin + Duration::from_millis(audio_ms);
  let wake_at = due.checked_sub(live_lead).unwrap_or(origin);
  if wake_at > now {
    PacePlan {
      sleep: Some(wake_at.saturating_duration_since(now)),
      rebaseline: None,
    }
  } else {
    let lateness = now.saturating_duration_since(wake_at);
    // When late beyond slack, shift origin so the schedule matches wall clock.
    // Sleep stays None on this tick (emit immediately); after origin advances,
    // the next chunk paces at realtime again.
    let rebaseline = if lateness > late_slack {
      Some(lateness)
    } else {
      None
    };
    PacePlan { sleep: None, rebaseline }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn live_body_threshold_is_full_content_length() {
    let threshold = live_body_threshold(LIVE_CONTENT_LENGTH);
    assert_eq!(threshold, LIVE_CONTENT_LENGTH);
    assert!(threshold >= 44);
  }

  #[test]
  fn live_body_threshold_scales_for_tiny_test_caps() {
    let threshold = live_body_threshold(1_000);
    assert_eq!(threshold, 1_000);
  }

  #[test]
  fn silence_preroll_frames_at_48k_is_two_seconds() {
    assert_eq!(silence_preroll_frames(48_000), 96_000);
    assert_eq!(silence_preroll_frames(44_100), 88_200);
    assert_eq!(silence_preroll_frames(1), 2);
  }

  #[test]
  fn pace_plan_sleeps_when_ahead_of_schedule() {
    let origin = Instant::now();
    // 0 frames emitted, 1024 next @ 48 kHz ≈ 21 ms audio; lead 2 s → wake far in the past
    // Use large frames_emitted so wake_at is in the future relative to origin.
    // frames such that audio_ms - lead_ms > 0 relative to now≈origin.
    // audio_ms = frames * 1000 / 48000; want audio_ms > live_lead (2000) + sleep_target.
    // 48000 frames = 1000 ms; 144000 frames = 3000 ms → wake_at = origin + 1000 ms.
    let plan = pace_plan(
      144_000,
      0,
      48_000,
      origin,
      origin,
      Duration::from_secs(2),
      Duration::from_millis(50),
    );
    assert_eq!(plan.rebaseline, None);
    let sleep_for = plan.sleep.expect("should sleep when ahead");
    assert!(
      sleep_for >= Duration::from_millis(900) && sleep_for <= Duration::from_millis(1_100),
      "sleep={sleep_for:?}"
    );
  }

  #[test]
  fn pace_plan_rebaselines_when_late_beyond_slack() {
    let origin = Instant::now();
    // wake_at ≈ origin for 0 frames with 2 s lead: due=origin, wake=origin-2s.
    // now = origin + 100 ms → lateness ≈ 2.1 s > 50 ms slack.
    let now = origin + Duration::from_millis(100);
    let plan = pace_plan(0, 1_024, 48_000, origin, now, Duration::from_secs(2), Duration::from_millis(50));
    assert_eq!(plan.sleep, None);
    let late = plan.rebaseline.expect("should rebaseline when late");
    assert!(late >= Duration::from_secs(2), "lateness={late:?}");
  }

  #[test]
  fn pace_plan_no_rebaseline_within_slack() {
    let origin = Instant::now();
    // 96_000 frames = 2 s audio @ 48 kHz; lead 2 s → wake_at = origin.
    // now = origin + 20 ms → lateness 20 ms < 50 ms slack.
    let now = origin + Duration::from_millis(20);
    let plan = pace_plan(
      96_000,
      0,
      48_000,
      origin,
      now,
      Duration::from_secs(2),
      Duration::from_millis(50),
    );
    assert_eq!(plan.sleep, None);
    assert_eq!(plan.rebaseline, None);
  }

  #[test]
  fn pace_plan_rebaseline_then_pacing_reengages() {
    // Post-stall: wall clock advanced 500 ms past wake_at. First plan is unpaced + rebaseline;
    // applying the origin shift makes the next plan on-schedule (pacing re-engages).
    let origin = Instant::now();
    let rate = 48_000_u32;
    let lead = Duration::from_secs(2);
    let slack = Duration::from_millis(50);
    // 96k frames @ 48 kHz with 2 s lead → wake_at = origin.
    let frames = 96_000_u64;
    let stall = Duration::from_millis(500);
    let now = origin + stall;

    let plan = pace_plan(frames, 1_024, rate, origin, now, lead, slack);
    assert_eq!(plan.sleep, None, "late tick must not sleep");
    let late = plan.rebaseline.expect("late beyond slack must rebaseline");
    assert!(late >= Duration::from_millis(450), "lateness={late:?}");

    let new_origin = origin + late;
    // Same frozen `now` after rebaseline: schedule is current → no large sleep, no rebaseline.
    let plan_after = pace_plan(frames, 1_024, rate, new_origin, now, lead, slack);
    assert_eq!(plan_after.rebaseline, None, "rebaselined origin must be current");
    // Next chunk (~21 ms of audio) with wall clock still at `now` wants a short sleep.
    let plan_next = pace_plan(frames + 1_024, 1_024, rate, new_origin, now, lead, slack);
    let sleep_for = plan_next.sleep.expect("pacing re-engages after rebaseline");
    assert!(
      sleep_for > Duration::from_millis(10) && sleep_for < Duration::from_millis(50),
      "sleep={sleep_for:?}"
    );
  }

  #[test]
  fn pace_plan_without_rebaseline_drains_backlog_unpaced() {
    // Document catch-up math when origin is held fixed (no rebaseline applied): successive
    // late ticks stay unpaced until frames_emitted catches wall clock, then sleep returns.
    let origin = Instant::now();
    let rate = 48_000_u32;
    let lead = Duration::from_secs(2);
    let slack = Duration::from_millis(50);
    let mut frames = 96_000_u64;
    let now = origin + Duration::from_millis(500);

    let mut unpaced = 0_u32;
    let mut reengaged = false;
    for _ in 0..64 {
      let plan = pace_plan(frames, 1_024, rate, origin, now, lead, slack);
      if plan.sleep.is_some() {
        reengaged = true;
        break;
      }
      frames = frames.saturating_add(1_024);
      unpaced += 1;
    }
    assert!(unpaced >= 20, "expected multi-chunk unpaced catch-up, got {unpaced}");
    assert!(reengaged, "pacing re-engages after unpaced catch-up");
  }

  /// Async integration: `pace_realtime_shared` holds origin with backlog, rebaselines when empty,
  /// and re-engages sleep after catch-up.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn pace_realtime_shared_holds_origin_with_backlog_then_rebaselines() {
    let rate = 48_000_u32;
    let next_frames = LIVE_CHUNK_FRAMES;
    let backlog_threshold = LIVE_CHUNK_FRAMES;
    // Capacity large enough to hold a full backlog threshold of silence.
    let ring = PcmRing::new(1, backlog_threshold.saturating_mul(4));
    let silence = vec![0.0_f32; backlog_threshold];
    ring.push_f32(&silence);
    assert!(
      ring.occupancy_frames() >= backlog_threshold,
      "occupancy={} threshold={backlog_threshold}",
      ring.occupancy_frames()
    );

    // 96_000 frames @ 48 kHz with LIVE_LEAD 2 s → wake_at ≈ origin.
    // Origin 300 ms in the past → late beyond PACE_LATE_SLACK, rebaseline candidate.
    let origin = Instant::now()
      .checked_sub(Duration::from_millis(300))
      .expect("instant subtract");
    let mut pace_origin = Some(origin);
    let frames_emitted = 96_000_u64;

    // (a)+(b) Late + backlog: origin held, call returns unpaced (no long sleep).
    let t0 = Instant::now();
    pace_realtime_shared(frames_emitted, &mut pace_origin, rate, next_frames, &ring, backlog_threshold).await;
    let elapsed_backlog = t0.elapsed();
    assert_eq!(pace_origin, Some(origin), "origin must be held while ring has backlog");
    assert!(
      elapsed_backlog < Duration::from_millis(50),
      "late+backlog must be unpaced, elapsed={elapsed_backlog:?}"
    );

    // Drain ring below threshold; still late → origin advances (rebaseline).
    let mut drain = Vec::new();
    let drained = ring.pop_i16(backlog_threshold.saturating_mul(4), &mut drain);
    assert!(drained >= backlog_threshold, "drained={drained}");
    assert!(
      ring.occupancy_frames() < backlog_threshold,
      "occupancy after drain={}",
      ring.occupancy_frames()
    );

    let origin_before_empty = pace_origin;
    let t1 = Instant::now();
    pace_realtime_shared(frames_emitted, &mut pace_origin, rate, next_frames, &ring, backlog_threshold).await;
    let elapsed_empty = t1.elapsed();
    assert!(
      elapsed_empty < Duration::from_millis(50),
      "late+empty rebaseline tick is unpaced, elapsed={elapsed_empty:?}"
    );
    let after_rebaseline = pace_origin.expect("origin set");
    let before = origin_before_empty.expect("origin set");
    assert!(
      after_rebaseline > before,
      "late+empty must advance origin: before={before:?} after={after_rebaseline:?}"
    );

    // After rebaseline, schedule is current; advance frames far enough that plan wants sleep,
    // then the shared call must actually sleep (pacing re-engages).
    // 96_000 + several chunks of next_frames with fixed wall ≈ rebaselined origin → ahead.
    let mut frames = frames_emitted;
    // Push schedule ~80 ms ahead of wall: audio_ms grows while wall barely moves.
    // wake_at = origin + audio_ms - lead; after rebaseline wake ≈ now; adding frames
    // makes wake_at > now → sleep.
    let advance_frames = u64::from(rate) * 80 / 1_000; // ~80 ms of audio
    frames = frames.saturating_add(advance_frames);

    let t2 = Instant::now();
    pace_realtime_shared(frames, &mut pace_origin, rate, next_frames, &ring, backlog_threshold).await;
    let elapsed_pace = t2.elapsed();
    assert!(
      elapsed_pace >= Duration::from_millis(20),
      "pacing must re-engage with sleep after rebaseline catch-up, elapsed={elapsed_pace:?}"
    );
    assert!(
      elapsed_pace < Duration::from_millis(200),
      "sleep should be ~tens of ms, elapsed={elapsed_pace:?}"
    );
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
  async fn empty_content_returns_404() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let handle = MediaServer::start("127.0.0.1").await.expect("start");
    let host_port = format!("127.0.0.1:{}", handle.addr.port());
    let mut stream = TcpStream::connect(&host_port).await.expect("connect");
    let req = format!("GET /stream HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.expect("write");
    let mut buf = Vec::new();
    let mut tmp = [0_u8; 512];
    loop {
      match stream.read(&mut tmp).await {
        Ok(0) | Err(_) => break,
        Ok(n) => buf.extend_from_slice(&tmp[..n]),
      }
    }
    let text = String::from_utf8_lossy(&buf);
    assert!(text.starts_with("HTTP/1.1 404"), "got: {text}");
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
  async fn live_wav_byte_stream_ends_at_exact_threshold_and_signals_rollover() {
    use crate::audio::continuous_wav_header;
    use futures_util::StreamExt;

    let ring = Arc::new(PcmRing::new(2, 4_096));
    let live_generation = Arc::new(AtomicU64::new(1));
    let bytes_served = Arc::new(AtomicU64::new(0));
    let last_body_write = Arc::new(RwLock::new(None));
    let rollover = Arc::new(RolloverSignal::default());
    let header = continuous_wav_header(2, 48_000).expect("header");
    let threshold = live_body_threshold(800);
    // Explicit silence preroll so an empty ring still ends at the tiny threshold.
    let stream = live_wav_byte_stream(
      ring,
      2,
      48_000,
      header,
      4_096,
      live_generation,
      1,
      threshold,
      Arc::clone(&bytes_served),
      Arc::clone(&last_body_write),
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
    assert_eq!(total, threshold, "body must end at exact Content-Length");
    assert_eq!(rollover.count(), 1, "exactly one rollover signal when body ends at cap");
    assert_eq!(bytes_served.load(Ordering::Acquire), total);
    assert!(last_body_write.read().is_some(), "progress must record last body write");
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
    let last_body_write = Arc::new(RwLock::new(None));
    let rollover = Arc::new(RolloverSignal::default());
    let header = continuous_wav_header(2, 48_000).expect("header");
    // 44-byte header + 100 bytes PCM = 25 stereo frames max after header.
    let threshold = 44 + 100;
    let stream = live_wav_byte_stream(
      Arc::clone(&ring),
      2,
      48_000,
      header,
      0, // no silence preroll — exercise real PCM near the cap
      live_generation,
      1,
      threshold,
      bytes_served,
      last_body_write,
      Arc::clone(&rollover),
    );
    tokio::pin!(stream);
    let mut total = 0_u64;
    while let Some(item) = stream.next().await {
      total = total.saturating_add(item.expect("infallible").len() as u64);
    }

    assert_eq!(total, threshold, "exact Content-Length including pad");
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
    // Fill the ring so the body can progress without relying on silence preroll.
    ring.push_f32(&[0.0; 4_096 * 2]);
    let handle = MediaServer::start("127.0.0.1").await.expect("start");
    // Tiny cap so a short progressive body ends without multi-hour runtime.
    handle.set_max_body_bytes(800);
    handle.set_content(MediaContent::LiveWav { ring, channels: 2, sample_rate: 48_000 });

    let before = handle.rollover_signal().count();
    let rollover = handle.rollover_signal();
    let url = handle.stream_url();

    // Drive the body over HTTP; with a tiny cap the stream ends at exact CL and signals rollover.
    let pull = tokio::spawn(async move { http_get_body_until_eof(&url, Duration::from_secs(5)).await });
    let signaled = tokio::time::timeout(Duration::from_secs(5), rollover.wait_past(before)).await;
    assert!(signaled.is_ok(), "HTTP LiveWav must signal rollover under tiny cap");
    assert!(handle.bytes_served() > 0, "server must have emitted at least the WAV header");
    assert_eq!(handle.bytes_served(), live_body_threshold(800));
    let body = pull.await.expect("pull join");
    if let Some(b) = body {
      assert_eq!(b.len() as u64, live_body_threshold(800), "HTTP body matches Content-Length");
    }
    handle.shutdown();
  }

  #[tokio::test]
  async fn head_does_not_supersede_live_get() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let ring = Arc::new(PcmRing::new(2, 48_000));
    // Continuous soft tone so the GET body keeps flowing.
    let mut samples = Vec::with_capacity(24_000 * 2);
    for n in 0..24_000 {
      let t = n as f32 / 48_000.0;
      let s = (2.0 * std::f32::consts::PI * 440.0 * t).sin() * 0.1;
      samples.push(s);
      samples.push(s);
    }
    ring.push_f32(&samples);

    let handle = MediaServer::start("127.0.0.1").await.expect("start");
    handle.set_content(MediaContent::LiveWav {
      ring: Arc::clone(&ring),
      channels: 2,
      sample_rate: 48_000,
    });

    let host_port = format!("127.0.0.1:{}", handle.addr.port());
    let get_req = format!("GET /stream HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");
    let head_req = format!("HEAD /stream HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");

    let mut get_stream = TcpStream::connect(&host_port).await.expect("connect get");
    get_stream.write_all(get_req.as_bytes()).await.expect("write get");

    // Read enough of the GET response to know the live body is active.
    let mut prefix = vec![0_u8; 0];
    let mut tmp = [0_u8; 2048];
    while prefix.len() < 512 {
      let n = get_stream.read(&mut tmp).await.expect("read get");
      assert!(n > 0, "GET body must produce bytes");
      prefix.extend_from_slice(&tmp[..n]);
    }
    let served_before = handle.bytes_served();
    assert!(served_before > 0);

    // HEAD mid-stream must not kill the GET or bump generation.
    let mut head_stream = TcpStream::connect(&host_port).await.expect("connect head");
    head_stream.write_all(head_req.as_bytes()).await.expect("write head");
    let mut head_buf = Vec::new();
    let mut head_tmp = [0_u8; 1024];
    loop {
      match head_stream.read(&mut head_tmp).await {
        Ok(0) | Err(_) => break,
        Ok(n) => head_buf.extend_from_slice(&head_tmp[..n]),
      }
    }
    let head_text = String::from_utf8_lossy(&head_buf);
    assert!(head_text.starts_with("HTTP/1.1 200"), "HEAD status: {head_text}");
    assert!(
      head_text.to_ascii_lowercase().contains("content-type: audio/wav"),
      "HEAD must advertise audio/wav"
    );

    // GET body continues after HEAD.
    let mut more = 0_usize;
    let continued = tokio::time::timeout(Duration::from_secs(3), async {
      loop {
        match get_stream.read(&mut tmp).await {
          Ok(0) | Err(_) => break,
          Ok(n) => {
            more = more.saturating_add(n);
            if more > 256 {
              break;
            }
          },
        }
      }
      more
    })
    .await
    .expect("GET must keep flowing after HEAD");
    assert!(continued > 0, "GET body must keep flowing after HEAD");
    assert!(
      handle.bytes_served() >= served_before,
      "generation must stay with the GET body (bytes keep counting up)"
    );
    handle.shutdown();
  }

  #[tokio::test]
  async fn progress_tracks_bytes_and_last_write() {
    let ring = Arc::new(PcmRing::new(2, 8_192));
    ring.push_f32(&[0.0; 4_096 * 2]);
    let handle = MediaServer::start("127.0.0.1").await.expect("start");
    handle.set_max_body_bytes(2_048);
    handle.set_content(MediaContent::LiveWav { ring, channels: 2, sample_rate: 48_000 });

    let url = handle.stream_url();
    let _body = http_get_body_until_eof(&url, Duration::from_secs(5)).await;
    let (bytes, last) = handle.progress();
    assert!(bytes > 0);
    assert!(last.is_some());
    assert_eq!(bytes, handle.bytes_served());
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
      raw_body.get(..max_body).unwrap_or(&[]).to_vec()
    } else {
      raw_body.to_vec()
    }
  }

  /// Read until the server ends the body (EOF), with a timeout.
  ///
  /// Returns `None` if the deadline expires or the peer closes before HTTP headers arrive.
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

  /// Decode as many complete FLAC samples as claxon can from a (possibly truncated) bitstream.
  fn claxon_decode_prefix(flac: &[u8]) -> (u32, u32, Vec<i32>) {
    let mut reader = claxon::FlacReader::new(std::io::Cursor::new(flac)).expect("claxon open live flac");
    let sample_rate = reader.streaminfo().sample_rate;
    let channels = reader.streaminfo().channels;
    let mut samples = Vec::new();
    for sample in reader.samples() {
      match sample {
        Ok(s) => samples.push(s),
        Err(_) => break, // truncated last frame is expected for live prefix reads
      }
    }
    (sample_rate, channels, samples)
  }

  /// Read a live body for up to `duration`, returning decoded payload bytes (chunked-aware).
  ///
  /// Live FLAC is unbounded and highly compressible; a fixed max-body read can hang once the
  /// ring underruns. Timed reads match production (peer closes when it has enough).
  async fn http_get_body_timed(url: &str, duration: Duration) -> Vec<u8> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let without_scheme = url.strip_prefix("http://").expect("http");
    let (host_port, raw_path) = without_scheme.split_once('/').expect("path");
    let request_path = format!("/{raw_path}");

    let mut stream = TcpStream::connect(host_port).await.expect("connect");
    let req = format!("GET {request_path} HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.expect("write");

    let mut buf = Vec::new();
    let mut tmp = [0_u8; 4096];
    let deadline = tokio::time::Instant::now() + duration;
    loop {
      let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
      if remaining.is_zero() {
        break;
      }
      match tokio::time::timeout(remaining, stream.read(&mut tmp)).await {
        Ok(Ok(n)) if n > 0 => buf.extend_from_slice(&tmp[..n]),
        _ => break,
      }
    }
    let split = find_header_end(&buf).expect("headers");
    let headers = buf.get(..split).unwrap_or(&[]);
    let raw_body = buf.get(split..).unwrap_or(&[]);
    if headers_indicate_chunked(headers) {
      decode_chunked_prefix(raw_body, raw_body.len())
    } else {
      raw_body.to_vec()
    }
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn live_flac_preroll_then_bit_exact_pcm() {
    use crate::audio::{FLAC_BLOCK_SIZE, round_up_to_flac_blocks};

    let sample_rate = 8_000_u32;
    let channels = 1_u16;
    // Real SILENCE_PREROLL (2 s) at 8 kHz, rounded up to fixed FLAC blocks.
    let preroll = round_up_to_flac_blocks(silence_preroll_frames(sample_rate));
    assert!(preroll >= silence_preroll_frames(sample_rate));
    assert_eq!(preroll % FLAC_BLOCK_SIZE, 0);

    // One full FLAC block of known pattern after silence (f32 → ring → i16 path).
    let pattern_frames = FLAC_BLOCK_SIZE;
    let mut pattern_f32 = Vec::with_capacity(pattern_frames);
    for i in 0..pattern_frames {
      let t = i as f32 / sample_rate as f32;
      let s = (2.0 * std::f32::consts::PI * 440.0 * t).sin() * 0.5;
      pattern_f32.push(s);
    }
    // Capture expected i16 via the same ring conversion production uses.
    let probe = PcmRing::new(channels, pattern_frames);
    probe.push_f32(&pattern_f32);
    let mut expected_i16 = Vec::new();
    assert_eq!(probe.pop_i16(pattern_frames, &mut expected_i16), pattern_frames);

    let ring = Arc::new(PcmRing::new(channels, pattern_frames.saturating_mul(4)));
    ring.push_f32(&pattern_f32);

    let handle = MediaServer::start("127.0.0.1").await.expect("start");
    handle.set_content(MediaContent::LiveFlac {
      ring: Arc::clone(&ring),
      channels,
      sample_rate,
    });

    let url = handle.stream_url();
    // Preroll encode is unpaced; one paced audio block follows. A few seconds is plenty.
    let body = http_get_body_timed(&url, Duration::from_secs(8)).await;
    assert!(body.len() > 42, "expected STREAMINFO + frames, got {}", body.len());
    assert_eq!(&body[0..4], b"fLaC");

    let (got_rate, got_ch, decoded) = claxon_decode_prefix(&body);
    assert_eq!(got_rate, sample_rate);
    assert_eq!(got_ch, u32::from(channels));

    // Need full preroll silence + full pattern block.
    let need = preroll.saturating_add(pattern_frames);
    assert!(
      decoded.len() >= need,
      "decoded {} samples, need at least {need} (preroll={preroll} + pattern={pattern_frames}); body={}",
      decoded.len(),
      body.len()
    );

    // First ~SILENCE_PREROLL (and padding to block boundary) must be zero.
    for (i, &s) in decoded.iter().take(preroll).enumerate() {
      assert_eq!(s, 0, "preroll sample {i} must be silence, got {s}");
    }
    // At least the real SILENCE_PREROLL duration is silent.
    let bare_preroll = silence_preroll_frames(sample_rate);
    for (i, &s) in decoded.iter().take(bare_preroll).enumerate() {
      assert_eq!(s, 0, "bare preroll sample {i} must be silence");
    }

    // Bit-exact PCM after preroll.
    let after = &decoded[preroll..preroll + pattern_frames];
    for (i, (&orig, &dec)) in expected_i16.iter().zip(after.iter()).enumerate() {
      assert_eq!(i32::from(orig), dec, "PCM mismatch at sample {i}");
    }

    handle.shutdown();
  }

  /// Stereo `LiveFlac`: distinct L/R so a channel swap fails bit-exact claxon decode.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn live_flac_stereo_channel_order_bit_exact() {
    use crate::audio::{FLAC_BLOCK_SIZE, round_up_to_flac_blocks};

    let sample_rate = 8_000_u32;
    let channels = 2_u16;
    let preroll = round_up_to_flac_blocks(silence_preroll_frames(sample_rate));
    // preroll is in frames; decoded samples = preroll * channels.
    let preroll_samples = preroll.saturating_mul(usize::from(channels));

    let pattern_frames = FLAC_BLOCK_SIZE;
    // Distinct L/R: L = +0.5, R = -0.5 (interleaved f32).
    let mut pattern_f32 = Vec::with_capacity(pattern_frames.saturating_mul(2));
    for _ in 0..pattern_frames {
      pattern_f32.push(0.5);
      pattern_f32.push(-0.5);
    }
    let probe = PcmRing::new(channels, pattern_frames);
    probe.push_f32(&pattern_f32);
    let mut expected_i16 = Vec::new();
    assert_eq!(probe.pop_i16(pattern_frames, &mut expected_i16), pattern_frames);
    assert_ne!(expected_i16[0], expected_i16[1], "L/R must differ");

    let ring = Arc::new(PcmRing::new(channels, pattern_frames.saturating_mul(4)));
    ring.push_f32(&pattern_f32);

    let handle = MediaServer::start("127.0.0.1").await.expect("start");
    handle.set_content(MediaContent::LiveFlac {
      ring: Arc::clone(&ring),
      channels,
      sample_rate,
    });

    let url = handle.stream_url();
    let body = http_get_body_timed(&url, Duration::from_secs(8)).await;
    assert!(body.len() > 42, "expected STREAMINFO + frames, got {}", body.len());
    assert_eq!(&body[0..4], b"fLaC");

    let (got_rate, got_ch, decoded) = claxon_decode_prefix(&body);
    assert_eq!(got_rate, sample_rate);
    assert_eq!(got_ch, u32::from(channels));

    let need = preroll_samples.saturating_add(expected_i16.len());
    assert!(
      decoded.len() >= need,
      "decoded {} samples, need at least {need}; body={}",
      decoded.len(),
      body.len()
    );

    for (i, &s) in decoded.iter().take(preroll_samples).enumerate() {
      assert_eq!(s, 0, "preroll sample {i} must be silence, got {s}");
    }

    let after = &decoded[preroll_samples..preroll_samples + expected_i16.len()];
    for (i, (&orig, &dec)) in expected_i16.iter().zip(after.iter()).enumerate() {
      assert_eq!(i32::from(orig), dec, "PCM mismatch at sample {i}");
    }
    // Channel order: even = L (+), odd = R (−). Swap would fail here.
    for frame in 0..pattern_frames {
      let left = after[frame * 2];
      let right = after[frame * 2 + 1];
      assert!(left > 0, "L must be positive at frame {frame}, got {left}");
      assert!(right < 0, "R must be negative at frame {frame}, got {right}");
    }

    handle.shutdown();
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn live_flac_second_get_reemits_header_and_supersedes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let sample_rate = 8_000_u32;
    let channels = 1_u16;
    let ring = Arc::new(PcmRing::new(channels, 48_000));
    // Continuous soft tone so bodies keep flowing after preroll.
    let mut samples = Vec::with_capacity(16_000);
    for n in 0..16_000 {
      let t = n as f32 / sample_rate as f32;
      let s = (2.0 * std::f32::consts::PI * 440.0 * t).sin() * 0.1;
      samples.push(s);
    }
    ring.push_f32(&samples);

    let handle = MediaServer::start("127.0.0.1").await.expect("start");
    handle.set_content(MediaContent::LiveFlac {
      ring: Arc::clone(&ring),
      channels,
      sample_rate,
    });

    let host_port = format!("127.0.0.1:{}", handle.addr.port());
    let req = format!("GET /stream HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");

    let mut first = TcpStream::connect(&host_port).await.expect("connect first");
    first.write_all(req.as_bytes()).await.expect("write first");
    let mut first_buf = Vec::new();
    let mut tmp = [0_u8; 4096];
    while first_buf.len() < 128 {
      let n = first.read(&mut tmp).await.expect("read first");
      assert!(n > 0, "first LiveFlac body must produce bytes");
      first_buf.extend_from_slice(&tmp[..n]);
    }
    let first_split = find_header_end(&first_buf).expect("first headers");
    let first_raw = first_buf.get(first_split..).unwrap_or(&[]);
    let first_body = if headers_indicate_chunked(first_buf.get(..first_split).unwrap_or(&[])) {
      decode_chunked_prefix(first_raw, 64)
    } else {
      first_raw.get(..64.min(first_raw.len())).unwrap_or(&[]).to_vec()
    };
    assert_eq!(&first_body[0..4], b"fLaC", "first GET must start with fLaC");

    let mut second = TcpStream::connect(&host_port).await.expect("connect second");
    second.write_all(req.as_bytes()).await.expect("write second");
    let mut second_buf = Vec::new();
    while second_buf.len() < 128 {
      let n = second.read(&mut tmp).await.expect("read second");
      assert!(n > 0, "second LiveFlac body must produce bytes");
      second_buf.extend_from_slice(&tmp[..n]);
    }
    let second_split = find_header_end(&second_buf).expect("second headers");
    let second_headers = second_buf.get(..second_split).unwrap_or(&[]);
    let second_raw = second_buf.get(second_split..).unwrap_or(&[]);
    let second_body = if headers_indicate_chunked(second_headers) {
      decode_chunked_prefix(second_raw, 64)
    } else {
      second_raw.get(..64.min(second_raw.len())).unwrap_or(&[]).to_vec()
    };
    assert_eq!(&second_body[0..4], b"fLaC", "second GET must re-emit fLaC header");

    // Superseded first body must terminate.
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
    assert!(ended.is_ok(), "superseded LiveFlac body must end");
    assert_eq!(handle.rollover_signal().count(), 0, "LiveFlac must not signal rollover");
    handle.shutdown();
  }

  #[tokio::test]
  async fn live_flac_headers_are_audio_flac_chunked_no_content_length() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let ring = Arc::new(PcmRing::new(1, 8_192));
    ring.push_f32(&[0.0; 4_096]);
    let handle = MediaServer::start("127.0.0.1").await.expect("start");
    handle.set_content(MediaContent::LiveFlac { ring, channels: 1, sample_rate: 8_000 });

    let host_port = format!("127.0.0.1:{}", handle.addr.port());
    let get_req = format!("GET /stream HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");
    let mut stream = TcpStream::connect(&host_port).await.expect("connect");
    stream.write_all(get_req.as_bytes()).await.expect("write");
    let mut buf = Vec::new();
    let mut tmp = [0_u8; 2048];
    while find_header_end(&buf).is_none() {
      let n = stream.read(&mut tmp).await.expect("read");
      assert!(n > 0, "headers must arrive");
      buf.extend_from_slice(&tmp[..n]);
    }
    let split = find_header_end(&buf).expect("headers");
    let headers = String::from_utf8_lossy(buf.get(..split).unwrap_or(&[]));
    let headers_lc = headers.to_ascii_lowercase();
    assert!(headers_lc.contains("content-type: audio/flac"), "headers: {headers}");
    assert!(
      headers_lc.contains("transfer-encoding: chunked") || !headers_lc.contains("content-length:"),
      "LiveFlac must be chunked (no Content-Length); headers: {headers}"
    );
    assert!(
      !headers_lc.contains("content-length:"),
      "LiveFlac must not set Content-Length: {headers}"
    );
    assert!(headers_lc.contains("accept-ranges: none"), "headers: {headers}");

    // HEAD must not bump generation or set Content-Length.
    let head_req = format!("HEAD /stream HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");
    let mut head = TcpStream::connect(&host_port).await.expect("connect head");
    head.write_all(head_req.as_bytes()).await.expect("write head");
    let mut head_buf = Vec::new();
    loop {
      match head.read(&mut tmp).await {
        Ok(0) | Err(_) => break,
        Ok(n) => head_buf.extend_from_slice(&tmp[..n]),
      }
    }
    let head_text = String::from_utf8_lossy(&head_buf);
    let head_lc = head_text.to_ascii_lowercase();
    assert!(head_text.starts_with("HTTP/1.1 200"), "HEAD: {head_text}");
    assert!(head_lc.contains("content-type: audio/flac"), "HEAD: {head_text}");
    assert!(
      !head_lc.contains("content-length:"),
      "HEAD LiveFlac must omit Content-Length: {head_text}"
    );

    handle.shutdown();
  }
}
