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
use tokio::sync::oneshot;
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
const LIVE_CONTENT_LENGTH: u64 = (u32::MAX / 2) as u64 + 44;

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

#[derive(Clone)]
struct AppState {
  content: Arc<RwLock<MediaContent>>,
  /// Bumped per `LiveWav` GET so the newest request supersedes older bodies.
  ///
  /// Cast clients sometimes probe `/stream` and then issue the real request;
  /// two live bodies popping the same ring would split frames between them.
  live_generation: Arc<AtomicU64>,
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
    let state = AppState {
      content: Arc::clone(&content),
      live_generation: Arc::new(AtomicU64::new(0)),
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
      live_wav_response(ring, channels, sample_rate, Arc::clone(&state.live_generation), generation)
    },
    MediaContent::Empty => (StatusCode::NO_CONTENT, "no stream").into_response(),
  }
}

fn live_wav_response(
  ring: Arc<PcmRing>,
  channels: u16,
  sample_rate: u32,
  live_generation: Arc<AtomicU64>,
  generation: u64,
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
  tracing::info!(
    channels = ch,
    sample_rate = rate,
    preroll_frames,
    "Cast client pulling LiveWav stream"
  );

  let stream = live_wav_byte_stream(ring, ch, header, preroll_frames, live_generation, generation);
  let mut response = Body::from_stream(stream).into_response();
  drop(
    response
      .headers_mut()
      .insert(header::CONTENT_TYPE, header::HeaderValue::from_static("audio/wav")),
  );
  // Nest/Chromecast often fail on chunked-only progressive audio. Advertise a
  // large Content-Length matching the continuous WAV header data size.
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

/// ~0.5 s of silence frames at `sample_rate`.
fn silence_preroll_frames(sample_rate: u32) -> usize {
  // Scale default 48 kHz constant to the stream rate.
  let base = u64::try_from(SILENCE_PREROLL_FRAMES).unwrap_or(24_000);
  let n = (base * u64::from(sample_rate)) / 48_000;
  usize::try_from(n).unwrap_or(SILENCE_PREROLL_FRAMES).max(1024)
}

/// Progressive async stream: WAV header, silence preroll, then PCM from the ring.
fn live_wav_byte_stream(
  ring: Arc<PcmRing>,
  channels: u16,
  header: [u8; 44],
  preroll_frames: usize,
  live_generation: Arc<AtomicU64>,
  generation: u64,
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
  };

  stream::unfold(initial, |mut live| async move {
    if live.is_superseded() {
      return None;
    }

    if let Some(hdr) = live.header.take() {
      return Some((Ok(hdr), live));
    }

    if live.preroll_frames_left > 0 {
      let n = live.preroll_frames_left.min(LIVE_CHUNK_FRAMES);
      live.preroll_frames_left = live.preroll_frames_left.saturating_sub(n);
      let samples = n.saturating_mul(usize::from(live.channels));
      let silence = vec![0_i16; samples];
      return Some((Ok(i16_slice_to_le_bytes(&silence)), live));
    }

    loop {
      // Re-check inside the underrun loop so a superseded body stops pulling
      // (and stops sleeping) instead of stealing frames from the new request.
      if live.is_superseded() {
        return None;
      }
      let frames = live.ring.pop_i16(LIVE_CHUNK_FRAMES, &mut live.i16_buf);
      if frames == 0 {
        // Brief underrun: keep the HTTP body open for Cast progressive pull.
        sleep(LIVE_UNDERRUN_SLEEP).await;
        continue;
      }
      let chunk = i16_slice_to_le_bytes(&live.i16_buf);
      return Some((Ok(chunk), live));
    }
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
}

impl LiveStreamState {
  /// True when a newer `/stream` GET owns the ring, so this body must end.
  fn is_superseded(&self) -> bool {
    self.live_generation.load(Ordering::Acquire) != self.generation
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
