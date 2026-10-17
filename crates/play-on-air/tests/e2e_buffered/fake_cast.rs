//! Fake Google Cast device: Cast v2 over TLS on localhost plus a FLAC stream consumer.
//!
//! Accepts the control-plane traffic the warm Cast pool sends (CONNECT, PING,
//! `GET_STATUS`, LAUNCH, LOAD, PAUSE, PLAY, STOP, `SET_VOLUME`) and answers with
//! `RECEIVER_STATUS` / `MEDIA_STATUS` / `PONG`. Each LOAD starts a consumer that GETs the
//! media URL (always dialed on `127.0.0.1`, keeping the test on loopback) and decodes
//! the chunked FLAC body with claxon, recording channel 0 and arrival times.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use serde_json::{Value, json};

const CERT: &[u8] = include_bytes!("../../src/cast/testdata/v3-self-signed-p256.der");
const KEY: &[u8] = include_bytes!("../../src/cast/testdata/v3-self-signed-p256.test-key.der");

const NS_HEARTBEAT: &str = "urn:x-cast:com.google.cast.tp.heartbeat";
const NS_RECEIVER: &str = "urn:x-cast:com.google.cast.receiver";
const NS_MEDIA: &str = "urn:x-cast:com.google.cast.media";
const APP_ID: &str = "CC1AD845";
const SESSION_ID: &str = "fake-session-1";
const TRANSPORT_ID: &str = "fake-transport-1";
/// Poll interval for stop flags on blocking sockets.
const POLL: Duration = Duration::from_millis(50);

/// Decoded FLAC body of one LOAD.
#[derive(Debug, Default, Clone)]
pub(crate) struct Capture {
  /// Content type the HTTP response declared.
  pub(crate) content_type: String,
  /// FLAC `STREAMINFO` sample rate.
  pub(crate) sample_rate: u32,
  /// Channel 0 samples (16-bit range).
  pub(crate) samples: Vec<i32>,
  /// `(arrival instant, total channel-0 samples decoded so far)` per FLAC block.
  pub(crate) marks: Vec<(Instant, usize)>,
  /// Decoder or transport failure that ended the body early.
  pub(crate) error: Option<String>,
}

/// One received Cast command (`type` field) with its arrival time.
#[derive(Debug, Clone)]
pub(crate) struct Command {
  pub(crate) at: Instant,
  pub(crate) kind: String,
}

#[derive(Debug, Default)]
struct DeviceState {
  launched: bool,
  media_session_id: i32,
  player_state: String,
  content_id: Option<String>,
  volume: f64,
}

#[derive(Debug, Default)]
struct Shared {
  state: Mutex<DeviceState>,
  captures: Mutex<Vec<Arc<Mutex<Capture>>>>,
  consumers: Mutex<Vec<Consumer>>,
  commands: Mutex<Vec<Command>>,
}

#[derive(Debug)]
struct Consumer {
  stream: TcpStream,
  thread: JoinHandle<()>,
}

/// Running fake Cast receiver on `127.0.0.1:<port>`.
#[derive(Debug)]
pub(crate) struct FakeCast {
  port: u16,
  shared: Arc<Shared>,
  stop: Arc<AtomicBool>,
  accept: Option<JoinHandle<()>>,
}

impl FakeCast {
  /// Bind an ephemeral loopback port and start accepting Cast control connections.
  pub(crate) fn start() -> Self {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let tls_config = ServerConfig::builder_with_provider(provider)
      .with_safe_default_protocol_versions()
      .expect("TLS versions")
      .with_no_client_auth()
      .with_single_cert(
        vec![CertificateDer::from(CERT.to_vec())],
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(KEY.to_vec())),
      )
      .expect("fake Cast certificate");
    let config = Arc::new(tls_config);
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind fake Cast");
    listener.set_nonblocking(true).expect("nonblocking listener");
    let port = listener.local_addr().expect("fake Cast addr").port();
    let shared = Arc::new(Shared {
      state: Mutex::new(DeviceState { volume: 0.5, ..DeviceState::default() }),
      ..Shared::default()
    });
    let stop = Arc::new(AtomicBool::new(false));
    let accept = {
      let thread_shared = Arc::clone(&shared);
      let thread_stop = Arc::clone(&stop);
      std::thread::Builder::new()
        .name("fake-cast-accept".into())
        .spawn(move || accept_loop(&listener, &config, &thread_shared, &thread_stop))
        .expect("spawn fake Cast accept")
    };
    Self { port, shared, stop, accept: Some(accept) }
  }

  /// Control-plane TCP port.
  pub(crate) const fn port(&self) -> u16 {
    self.port
  }

  /// Snapshot of every LOAD capture so far, oldest first.
  pub(crate) fn captures(&self) -> Vec<Capture> {
    let captures = self.shared.captures.lock().clone();
    captures.iter().map(|capture| capture.lock().clone()).collect()
  }

  /// Every Cast command received so far.
  pub(crate) fn commands(&self) -> Vec<Command> {
    self.shared.commands.lock().clone()
  }

  /// Stop consumers and the accept loop.
  pub(crate) fn stop(mut self) {
    self.stop.store(true, Ordering::SeqCst);
    stop_consumers(&self.shared);
    if let Some(accept) = self.accept.take() {
      accept.join().expect("join fake Cast accept");
    }
  }
}

fn accept_loop(listener: &TcpListener, config: &Arc<ServerConfig>, shared: &Arc<Shared>, stop: &Arc<AtomicBool>) {
  let mut sessions = Vec::new();
  while !stop.load(Ordering::SeqCst) {
    match listener.accept() {
      Ok((tcp, _)) => {
        let session_config = Arc::clone(config);
        let session_shared = Arc::clone(shared);
        let session_stop = Arc::clone(stop);
        sessions.push(
          std::thread::Builder::new()
            .name("fake-cast-session".into())
            .spawn(move || serve_session(tcp, session_config, &session_shared, &session_stop))
            .expect("spawn fake Cast session"),
        );
      },
      Err(err) if err.kind() == io::ErrorKind::WouldBlock => std::thread::sleep(POLL),
      Err(err) => panic!("fake Cast accept: {err}"),
    }
  }
  for session in sessions {
    session.join().expect("join fake Cast session");
  }
}

fn serve_session(tcp: TcpStream, config: Arc<ServerConfig>, shared: &Arc<Shared>, stop: &AtomicBool) {
  tcp.set_nonblocking(false).expect("blocking session socket");
  tcp.set_read_timeout(Some(POLL)).expect("session read timeout");
  let conn = ServerConnection::new(config).expect("TLS server connection");
  let mut tls = StreamOwned::new(conn, tcp);
  let mut inbox = Vec::new();
  let mut buf = vec![0_u8; 16 * 1024];
  while !stop.load(Ordering::SeqCst) {
    match tls.read(&mut buf) {
      Ok(0) => return,
      Ok(n) => inbox.extend_from_slice(&buf[..n]),
      Err(err) if matches!(err.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => continue,
      Err(_) => return,
    }
    while let Some(frame_len) = inbox
      .get(..4)
      .map(|len| u32::from_be_bytes(len.try_into().expect("4 bytes")) as usize)
    {
      if inbox.len() < 4 + frame_len {
        break;
      }
      let frame: Vec<u8> = inbox.drain(..4 + frame_len).skip(4).collect();
      let message = CastMessage::decode(&frame);
      for reply in handle(shared, &message) {
        let encoded = reply.encode();
        let mut wire = u32::try_from(encoded.len()).expect("frame fits").to_be_bytes().to_vec();
        wire.extend_from_slice(&encoded);
        if tls.write_all(&wire).and_then(|()| tls.flush()).is_err() {
          return;
        }
      }
    }
  }
}

fn handle(shared: &Arc<Shared>, message: &CastMessage) -> Vec<CastMessage> {
  let payload: Value = serde_json::from_str(&message.payload).unwrap_or(Value::Null);
  let kind = payload.get("type").and_then(Value::as_str).unwrap_or_default().to_owned();
  let request_id = payload.get("requestId").and_then(Value::as_u64).unwrap_or(0);
  shared.commands.lock().push(Command { at: Instant::now(), kind: kind.clone() });
  let reply = |namespace: &str, body: Value| CastMessage {
    source: message.destination.clone(),
    destination: message.source.clone(),
    namespace: namespace.to_owned(),
    payload: body.to_string(),
  };
  match (message.namespace.as_str(), kind.as_str()) {
    (NS_HEARTBEAT, "PING") => vec![reply(NS_HEARTBEAT, json!({ "type": "PONG" }))],
    (NS_RECEIVER, "LAUNCH" | "GET_STATUS" | "SET_VOLUME" | "STOP") => {
      let mut state = shared.state.lock();
      match kind.as_str() {
        "LAUNCH" => state.launched = true,
        "STOP" => state.launched = false,
        "SET_VOLUME" => {
          if let Some(level) = payload.pointer("/volume/level").and_then(Value::as_f64) {
            state.volume = level;
          }
        },
        _ => {},
      }
      let status = receiver_status(&state, request_id);
      drop(state);
      vec![reply(NS_RECEIVER, status)]
    },
    (NS_MEDIA, "LOAD") => {
      let url = payload
        .pointer("/media/contentId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
      let mut state = shared.state.lock();
      state.media_session_id = state.media_session_id.saturating_add(1);
      "PLAYING".clone_into(&mut state.player_state);
      state.content_id = Some(url.clone());
      let status = media_status(&state, request_id);
      drop(state);
      start_consumer(shared, &url);
      vec![reply(NS_MEDIA, status)]
    },
    (NS_MEDIA, "GET_STATUS" | "PAUSE" | "PLAY" | "STOP") => {
      let mut state = shared.state.lock();
      match kind.as_str() {
        "PAUSE" => "PAUSED".clone_into(&mut state.player_state),
        "PLAY" => "PLAYING".clone_into(&mut state.player_state),
        "STOP" => "IDLE".clone_into(&mut state.player_state),
        _ => {},
      }
      let status = media_status(&state, request_id);
      drop(state);
      if kind == "STOP" {
        stop_consumers(shared);
      }
      vec![reply(NS_MEDIA, status)]
    },
    // CONNECT / CLOSE on `tp.connection` need no reply; other requests are ignored.
    _ => Vec::new(),
  }
}

fn receiver_status(state: &DeviceState, request_id: u64) -> Value {
  let applications = if state.launched {
    json!([{
      "appId": APP_ID,
      "sessionId": SESSION_ID,
      "transportId": TRANSPORT_ID,
      "displayName": "Default Media Receiver",
      "statusText": "",
      "namespaces": [{ "name": NS_MEDIA }],
    }])
  } else {
    json!([])
  };
  json!({
    "type": "RECEIVER_STATUS",
    "requestId": request_id,
    "status": {
      "applications": applications,
      "volume": { "level": state.volume, "muted": false },
    },
  })
}

fn media_status(state: &DeviceState, request_id: u64) -> Value {
  let entries = state.content_id.as_ref().map_or_else(
    || json!([]),
    |content_id| {
      json!([{
        "mediaSessionId": state.media_session_id,
        "playbackRate": 1.0,
        "playerState": state.player_state,
        "currentTime": 0.0,
        "supportedMediaCommands": 274_447,
        "media": { "contentId": content_id, "streamType": "LIVE", "contentType": "audio/flac" },
      }])
    },
  );
  json!({ "type": "MEDIA_STATUS", "requestId": request_id, "status": entries })
}

fn stop_consumers(shared: &Shared) {
  let consumers: Vec<Consumer> = shared.consumers.lock().drain(..).collect();
  for consumer in consumers {
    drop(consumer.stream.shutdown(Shutdown::Both));
    consumer.thread.join().expect("join FLAC consumer");
  }
}

/// A real receiver drops its previous media when a new LOAD arrives.
fn start_consumer(shared: &Arc<Shared>, url: &str) {
  stop_consumers(shared);
  let capture = Arc::new(Mutex::new(Capture::default()));
  shared.captures.lock().push(Arc::clone(&capture));
  let (port, path) = split_url(url);
  let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect media server");
  let reader = stream.try_clone().expect("clone media socket");
  let thread = std::thread::Builder::new()
    .name("fake-cast-flac".into())
    .spawn(move || {
      if let Err(err) = consume(reader, &path, &capture) {
        capture.lock().error = Some(err);
      }
    })
    .expect("spawn FLAC consumer");
  shared.consumers.lock().push(Consumer { stream, thread });
}

/// `(port, path)` of an `http://host:port/path` URL (host ignored: always loopback).
fn split_url(url: &str) -> (u16, String) {
  let rest = url.strip_prefix("http://").expect("http URL");
  let (authority, path) = rest.split_once('/').expect("URL path");
  let port = authority.rsplit_once(':').expect("URL port").1.parse().expect("port number");
  (port, format!("/{path}"))
}

fn consume(mut stream: TcpStream, path: &str, capture: &Mutex<Capture>) -> Result<(), String> {
  let request = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nUser-Agent: fake-cast\r\nConnection: close\r\n\r\n");
  stream.write_all(request.as_bytes()).map_err(|err| err.to_string())?;
  let mut reader = BufReader::new(stream);
  let mut chunked = false;
  let mut status_line = String::new();
  let _status_len = reader.read_line(&mut status_line).map_err(|err| err.to_string())?;
  if !status_line.contains(" 200 ") {
    return Err(format!("media GET status: {status_line}"));
  }
  let mut header = String::new();
  loop {
    header.clear();
    let _header_len = reader.read_line(&mut header).map_err(|err| err.to_string())?;
    let trimmed = header.trim_end();
    if trimmed.is_empty() {
      break;
    }
    if let Some((name, raw_value)) = trimmed.split_once(':') {
      let value = raw_value.trim();
      if name.eq_ignore_ascii_case("transfer-encoding") && value.eq_ignore_ascii_case("chunked") {
        chunked = true;
      }
      if name.eq_ignore_ascii_case("content-type") {
        value.clone_into(&mut capture.lock().content_type);
      }
    }
  }
  let body: Box<dyn Read> = if chunked {
    Box::new(Chunked { inner: reader, left: 0, done: false })
  } else {
    Box::new(reader)
  };
  let mut flac = claxon::FlacReader::new(body).map_err(|err| format!("FLAC header: {err}"))?;
  capture.lock().sample_rate = flac.streaminfo().sample_rate;
  let mut blocks = flac.blocks();
  let mut buffer = Vec::new();
  loop {
    match blocks.read_next_or_eof(buffer) {
      Ok(Some(block)) => {
        let mut guard = capture.lock();
        guard.samples.extend_from_slice(block.channel(0));
        let total = guard.samples.len();
        guard.marks.push((Instant::now(), total));
        drop(guard);
        buffer = block.into_buffer();
      },
      // EOF, or our own shutdown on STOP / new LOAD / teardown.
      Ok(None) | Err(claxon::Error::IoError(_)) => return Ok(()),
      Err(err) => return Err(format!("FLAC decode: {err}")),
    }
  }
}

/// `Transfer-Encoding: chunked` body reader.
struct Chunked<R> {
  inner: R,
  left: usize,
  done: bool,
}

impl<R: BufRead> Read for Chunked<R> {
  fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
    if self.done || out.is_empty() {
      return Ok(0);
    }
    if self.left == 0 {
      let mut line = String::new();
      if self.inner.read_line(&mut line)? == 0 {
        return Ok(0);
      }
      if line.trim().is_empty() {
        // CRLF closing the previous chunk.
        line.clear();
        if self.inner.read_line(&mut line)? == 0 {
          return Ok(0);
        }
      }
      let size = line.trim().split(';').next().unwrap_or_default();
      self.left = usize::from_str_radix(size, 16).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
      if self.left == 0 {
        self.done = true;
        return Ok(0);
      }
    }
    let want = out.len().min(self.left);
    let n = self.inner.read(&mut out[..want])?;
    self.left -= n;
    Ok(n)
  }
}

/// Cast v2 `CastMessage` (string payloads only), hand-encoded protobuf.
#[derive(Debug)]
struct CastMessage {
  source: String,
  destination: String,
  namespace: String,
  payload: String,
}

impl CastMessage {
  fn decode(mut bytes: &[u8]) -> Self {
    let mut message = Self {
      source: String::new(),
      destination: String::new(),
      namespace: String::new(),
      payload: String::new(),
    };
    while !bytes.is_empty() {
      let key = read_varint(&mut bytes);
      match key & 0x07 {
        0 => {
          let _value = read_varint(&mut bytes);
        },
        2 => {
          let len = usize::try_from(read_varint(&mut bytes)).expect("length");
          let (field, rest) = bytes.split_at(len);
          bytes = rest;
          let text = String::from_utf8_lossy(field).into_owned();
          match key >> 3 {
            2 => message.source = text,
            3 => message.destination = text,
            4 => message.namespace = text,
            6 => message.payload = text,
            _ => {},
          }
        },
        wire => panic!("unexpected protobuf wire type {wire}"),
      }
    }
    message
  }

  fn encode(&self) -> Vec<u8> {
    let mut out = vec![0x08, 0x00]; // protocol_version = CASTV2_1_0
    for (tag, text) in [(0x12_u8, &self.source), (0x1A, &self.destination), (0x22, &self.namespace)] {
      out.push(tag);
      write_varint(&mut out, text.len() as u64);
      out.extend_from_slice(text.as_bytes());
    }
    out.extend_from_slice(&[0x28, 0x00]); // payload_type = STRING
    out.push(0x32);
    write_varint(&mut out, self.payload.len() as u64);
    out.extend_from_slice(self.payload.as_bytes());
    out
  }
}

fn read_varint(bytes: &mut &[u8]) -> u64 {
  let mut value = 0_u64;
  let mut shift = 0;
  loop {
    let (&byte, rest) = bytes.split_first().expect("varint byte");
    *bytes = rest;
    value |= u64::from(byte & 0x7F) << shift;
    if byte & 0x80 == 0 {
      return value;
    }
    shift += 7;
  }
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
  while value >= 0x80 {
    out.push((value as u8) | 0x80);
    value >>= 7;
  }
  out.push(value as u8);
}
