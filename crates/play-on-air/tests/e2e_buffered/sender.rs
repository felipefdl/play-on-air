//! Synthetic AirPlay 2 sender: transient pairing, encrypted RTSP, buffered (type 103) AAC.
//!
//! Speaks just enough of the iOS buffered-audio flow to drive a real receiver: transient
//! SRP pair-setup (M1–M4), encrypted RTSP control, the initial PTP `SETUP`, the
//! stream `SETUP` for type 103, `RECORD`, `SETRATEANCHORTIME`, `FLUSHBUFFERED`, and
//! `TEARDOWN`. Audio packets are ChaCha20-Poly1305 RTP frames over the data TCP port.

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use chacha20poly1305::aead::{Aead, Payload};
use chacha20poly1305::{ChaCha20Poly1305, KeyInit, Nonce};
use num_bigint::BigUint;
use sha2::{Digest, Sha512};
use shairplay::crypto::chacha_transport::EncryptedChannel;
use shairplay::crypto::tlv::{TlvType, TlvValues};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// AAC-LC frames carry 1024 samples each.
pub(crate) const FRAME_SAMPLES: u32 = 1024;
/// Fixture and stream sample rate.
pub(crate) const SAMPLE_RATE: u32 = 44_100;
/// RTP SSRC Apple uses for AAC 44.1 kHz stereo buffered audio.
const SSRC_AAC_44100_STEREO: u32 = 0x1600_0000;
/// `audioFormat` bit for AAC-LC 44.1 kHz stereo in the stream SETUP.
const AUDIO_FORMAT_AAC_LC_44100_STEREO: u64 = 0x40_0000;
/// Timeline id the sender stamps on every anchor (its own PTP clock identity).
const TIMELINE_ID: u64 = 0x00E0_4CFF_FE68_0001;
/// Bound for a single RTSP request/response exchange.
const RTSP_TIMEOUT: Duration = Duration::from_secs(5);

/// 3072-bit SRP group prime (RFC 5054) used by `HomeKit` pairing.
const SRP_N_HEX: &str = "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7EDEE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3DC2007CB8A163BF0598DA48361C55D39A69163FA8FD24CF5F83655D23DCA3AD961C62F356208552BB9ED529077096966D670C354E4ABC9804F1746C08CA18217C32905E462E36CE3BE39E772C180E86039B2783A2EC07A28FB5C55DF06F4C52C9DE2BCBF6955817183995497CEA956AE515D2261898FA051015728E5A8AAAC42DAD33170D04507A33A85521ABDF1CBA64ECFB850458DBEF0A8AEA71575D060C7DB3970F85A6E1E4C7ABF5AE8CDB0933D71E8C94E04A25619DCEE3D2261AD2EE6BF12FFA06D98A0864D87602733EC86A64521F2B18177B200CBBE117577A615D6C770988C0BAD946E208E24FA074E5AB3143DB5BFCE0FD108E4B82D120A93AD2CAFFFFFFFFFFFFFFFF";

/// Raw AAC-LC access units (ADTS headers stripped) of one looping tone fixture.
#[derive(Debug, Clone)]
pub(crate) struct AacLoop {
  frames: Vec<Vec<u8>>,
}

impl AacLoop {
  /// Split an ADTS file into raw access units.
  pub(crate) fn from_adts(adts: &[u8]) -> Self {
    let mut frames = Vec::new();
    let mut rest = adts;
    while rest.len() >= 7 {
      assert!(rest[0] == 0xFF && rest[1] & 0xF0 == 0xF0, "ADTS sync word");
      let protection_absent = rest[1] & 0x01 == 1;
      let header_len = if protection_absent { 7 } else { 9 };
      let frame_len = (usize::from(rest[3] & 0x03) << 11) | (usize::from(rest[4]) << 3) | (usize::from(rest[5]) >> 5);
      assert!(frame_len > header_len && frame_len <= rest.len(), "ADTS frame length");
      frames.push(rest[header_len..frame_len].to_vec());
      rest = &rest[frame_len..];
    }
    assert!(!frames.is_empty(), "fixture has AAC frames");
    Self { frames }
  }

  /// Access units in one loop period.
  pub(crate) const fn len(&self) -> usize {
    self.frames.len()
  }

  /// Access unit `index` of the endlessly repeated loop.
  pub(crate) fn frame(&self, index: usize) -> &[u8] {
    &self.frames[index % self.frames.len()]
  }
}

/// Where the next buffered packet goes on the sender's RTP timeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Cursor {
  /// 24-bit RTP sequence number of the next packet.
  pub(crate) seq: u32,
  /// RTP timestamp of the next packet's first sample.
  pub(crate) rtp: u32,
}

impl Cursor {
  /// Cursor `frames` AAC frames later.
  pub(crate) const fn advanced(self, frames: u32) -> Self {
    Self {
      seq: self.seq.wrapping_add(frames) & 0x00FF_FFFF,
      rtp: self.rtp.wrapping_add(frames.wrapping_mul(FRAME_SAMPLES)),
    }
  }
}

/// Live buffered-audio session against one receiver.
pub(crate) struct Sender {
  control: Control,
  rtsp_port: u16,
  data: TcpStream,
  audio_cipher: ChaCha20Poly1305,
  _event: TcpStream,
  epoch: Instant,
  next_nonce: u64,
}

impl std::fmt::Debug for Sender {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Sender")
      .field("cseq", &self.control.cseq)
      .field("rtsp_port", &self.rtsp_port)
      .finish_non_exhaustive()
  }
}

impl Sender {
  /// Pair, set up a PTP-timed buffered AAC stream, and `RECORD`.
  pub(crate) async fn connect(rtsp_port: u16) -> Self {
    let mut rtsp = TcpStream::connect(("127.0.0.1", rtsp_port)).await.expect("connect RTSP");
    rtsp.set_nodelay(true).expect("nodelay");
    let session_key = transient_pair_setup(&mut rtsp).await;
    let channel = EncryptedChannel::new(
      &session_key,
      "Control-Salt",
      "Control-Write-Encryption-Key",
      "Control-Salt",
      "Control-Read-Encryption-Key",
    )
    .expect("control channel keys");
    let shk = [0x5A_u8; 32];
    let mut control = Control { rtsp, channel, cseq: 2 };

    let local = || plist::Value::String("127.0.0.1".into());
    let peer = dict([("ID", local()), ("Addresses", plist::Value::Array(vec![local()]))]);
    let setup = dict([
      ("timingProtocol", plist::Value::String("PTP".into())),
      ("timingPeerInfo", plist::Value::Dictionary(peer)),
      ("groupContainsGroupLeader", plist::Value::Boolean(false)),
      ("isMultiSelectAirPlay", plist::Value::Boolean(true)),
    ]);
    let initial = control.request("SETUP", rtsp_port, Some(&setup)).await;
    assert_eq!(initial.status, 200, "initial SETUP");
    let event_port = initial.plist_u64("eventPort").expect("eventPort");
    let event = TcpStream::connect(("127.0.0.1", u16::try_from(event_port).expect("port")))
      .await
      .expect("connect event channel");

    let stream = dict([
      ("type", uint(103)),
      ("audioFormat", uint(AUDIO_FORMAT_AAC_LC_44100_STEREO)),
      ("ct", uint(4)),
      ("sr", uint(u64::from(SAMPLE_RATE))),
      ("spf", uint(u64::from(FRAME_SAMPLES))),
      ("audioMode", plist::Value::String("default".into())),
      ("controlPort", uint(0)),
      ("shk", plist::Value::Data(shk.to_vec())),
    ]);
    let streams = dict([("streams", plist::Value::Array(vec![plist::Value::Dictionary(stream)]))]);
    let stream_setup = control.request("SETUP", rtsp_port, Some(&streams)).await;
    assert_eq!(stream_setup.status, 200, "stream SETUP");
    let data_port = stream_setup.stream_u64("dataPort").expect("dataPort");
    let data = TcpStream::connect(("127.0.0.1", u16::try_from(data_port).expect("port")))
      .await
      .expect("connect buffered data port");
    data.set_nodelay(true).expect("nodelay");

    let record = control.request("RECORD", rtsp_port, None).await;
    assert_eq!(record.status, 200, "RECORD");

    Self {
      control,
      rtsp_port,
      data,
      audio_cipher: ChaCha20Poly1305::new((&shk).into()),
      _event: event,
      epoch: Instant::now(),
      next_nonce: 1,
    }
  }

  /// Sender network time "now" on its own timeline (seconds, 2^-64 fraction).
  fn network_now(&self) -> (u64, u64) {
    let elapsed = self.epoch.elapsed();
    let frac = (u128::from(elapsed.subsec_nanos()) << 64) / 1_000_000_000;
    (
      elapsed.as_secs().saturating_add(1_000),
      u64::try_from(frac).expect("fraction fits"),
    )
  }

  /// `SETRATEANCHORTIME` rate 1: sample `rtp` plays at the sender's "now".
  pub(crate) async fn play_at_now(&mut self, rtp: u32) {
    let (secs, frac) = self.network_now();
    let body = dict([
      ("rate", uint(1)),
      ("rtpTime", uint(u64::from(rtp))),
      ("networkTimeSecs", uint(secs)),
      ("networkTimeFrac", uint(frac)),
      ("networkTimeTimelineID", uint(TIMELINE_ID)),
      ("networkTimeFlags", uint(0)),
    ]);
    let status = self.control("SETRATEANCHORTIME", Some(&body)).await;
    assert_eq!(status, 200, "SETRATEANCHORTIME rate 1");
  }

  /// `SETRATEANCHORTIME` rate 0 (pause; buffered audio stays queued).
  pub(crate) async fn pause(&mut self) {
    let body = dict([("rate", uint(0))]);
    let status = self.control("SETRATEANCHORTIME", Some(&body)).await;
    assert_eq!(status, 200, "SETRATEANCHORTIME rate 0");
  }

  /// Flush-all `FLUSHBUFFERED`: drop everything before `until` (exclusive).
  pub(crate) async fn flush_all(&mut self, until: Cursor) {
    let body = dict([
      ("flushUntilSeq", uint(u64::from(until.seq))),
      ("flushUntilTS", uint(u64::from(until.rtp))),
    ]);
    let status = self.control("FLUSHBUFFERED", Some(&body)).await;
    assert_eq!(status, 200, "FLUSHBUFFERED");
  }

  /// `TEARDOWN` the whole session.
  pub(crate) async fn teardown(&mut self) {
    let status = self.control("TEARDOWN", None).await;
    assert_eq!(status, 200, "TEARDOWN");
  }

  /// Send `count` AAC frames of `tone` starting at `start`; returns the next cursor.
  ///
  /// `pattern(i)` picks the fixture for frame `i` (index from `start`), so callers can
  /// splice two tones into a position-coded signal.
  pub(crate) async fn send_frames<'a>(
    &mut self,
    start: Cursor,
    count: u32,
    mut pattern: impl FnMut(u32) -> (&'a AacLoop, usize),
  ) -> Cursor {
    let mut wire = Vec::with_capacity(512 * 64);
    let mut cursor = start;
    for i in 0..count {
      let (tone, frame_index) = pattern(i);
      self.append_packet(&mut wire, cursor, tone.frame(frame_index));
      cursor = cursor.advanced(1);
      if wire.len() >= 512 * 60 {
        self.data.write_all(&wire).await.expect("write buffered audio");
        wire.clear();
      }
    }
    if !wire.is_empty() {
      self.data.write_all(&wire).await.expect("write buffered audio");
    }
    cursor
  }

  fn append_packet(&mut self, wire: &mut Vec<u8>, cursor: Cursor, aac: &[u8]) {
    let mut header = [0_u8; 12];
    header[0] = 0x80;
    header[1..4].copy_from_slice(&cursor.seq.to_be_bytes()[1..4]);
    header[4..8].copy_from_slice(&cursor.rtp.to_be_bytes());
    header[8..12].copy_from_slice(&SSRC_AAC_44100_STEREO.to_be_bytes());
    let nonce_tail = self.next_nonce.to_le_bytes();
    self.next_nonce = self.next_nonce.wrapping_add(1);
    let mut nonce = [0_u8; 12];
    nonce[4..].copy_from_slice(&nonce_tail);
    let sealed = self
      .audio_cipher
      .encrypt(Nonce::from_slice(&nonce), Payload { msg: aac, aad: &header[4..12] })
      .expect("seal audio packet");
    let total = 2 + header.len() + sealed.len() + nonce_tail.len();
    wire.extend_from_slice(&u16::try_from(total).expect("packet fits u16").to_be_bytes());
    wire.extend_from_slice(&header);
    wire.extend_from_slice(&sealed);
    wire.extend_from_slice(&nonce_tail);
  }

  async fn control(&mut self, method: &str, body: Option<&plist::Dictionary>) -> u16 {
    self.control.request(method, self.rtsp_port, body).await.status
  }
}

/// Binary-plist dictionary from `(key, value)` pairs.
fn dict<const N: usize>(entries: [(&str, plist::Value); N]) -> plist::Dictionary {
  entries.into_iter().map(|(key, value)| (key.to_owned(), value)).collect()
}

/// Unsigned plist integer.
fn uint(value: u64) -> plist::Value {
  plist::Value::Integer(value.into())
}

/// Encrypted RTSP control connection of a [`Sender`].
struct Control {
  rtsp: TcpStream,
  channel: EncryptedChannel,
  cseq: u32,
}

/// Parsed RTSP response.
struct Response {
  status: u16,
  body: Vec<u8>,
}

impl Response {
  fn plist(&self) -> Option<plist::Dictionary> {
    plist::from_bytes::<plist::Value>(&self.body).ok()?.into_dictionary()
  }

  fn plist_u64(&self, key: &str) -> Option<u64> {
    self.plist()?.get(key)?.as_unsigned_integer()
  }

  fn stream_u64(&self, key: &str) -> Option<u64> {
    let dict = self.plist()?;
    let stream = dict.get("streams")?.as_array()?.first()?.as_dictionary()?;
    stream.get(key)?.as_unsigned_integer()
  }
}

impl Control {
  async fn request(&mut self, method: &str, rtsp_port: u16, body: Option<&plist::Dictionary>) -> Response {
    let mut payload = Vec::new();
    if let Some(dict) = body {
      plist::to_writer_binary(&mut payload, dict).expect("encode plist");
    }
    let cseq = self.cseq;
    self.cseq = self.cseq.wrapping_add(1);
    let mut head = format!("{method} rtsp://127.0.0.1/{rtsp_port} RTSP/1.0\r\nCSeq: {cseq}\r\n");
    if !payload.is_empty() {
      head.push_str("Content-Type: application/x-apple-binary-plist\r\n");
    }
    write!(head, "Content-Length: {}\r\n\r\n", payload.len()).expect("format RTSP head");
    let mut plain = head.into_bytes();
    plain.extend_from_slice(&payload);
    let wire = self.channel.encrypt_ctx.encrypt(&plain).expect("encrypt RTSP");
    self.rtsp.write_all(&wire).await.expect("write RTSP");
    tokio::time::timeout(RTSP_TIMEOUT, self.read_response())
      .await
      .unwrap_or_else(|_| panic!("{method} response timed out"))
  }

  async fn read_response(&mut self) -> Response {
    let mut wire = Vec::new();
    let mut plain = Vec::new();
    let mut buf = vec![0_u8; 16 * 1024];
    loop {
      if let Some(resp) = parse_response(&plain) {
        return resp;
      }
      let n = self.rtsp.read(&mut buf).await.expect("read RTSP");
      assert!(n > 0, "receiver closed RTSP connection");
      wire.extend_from_slice(&buf[..n]);
      let (decrypted, consumed) = self.channel.decrypt_ctx.decrypt(&wire).expect("decrypt RTSP");
      plain.extend_from_slice(&decrypted);
      drop(wire.drain(..consumed));
    }
  }
}

fn parse_response(plain: &[u8]) -> Option<Response> {
  let header_end = plain.windows(4).position(|w| w == b"\r\n\r\n")? + 4;
  let head = std::str::from_utf8(&plain[..header_end]).expect("RTSP head is UTF-8");
  let status = head.split_whitespace().nth(1)?.parse::<u16>().ok()?;
  let content_length = head
    .lines()
    .find_map(|line| {
      let (name, value) = line.split_once(':')?;
      name
        .eq_ignore_ascii_case("content-length")
        .then(|| value.trim().parse::<usize>().ok())?
    })
    .unwrap_or(0);
  if plain.len() < header_end + content_length {
    return None;
  }
  Some(Response {
    status,
    body: plain[header_end..header_end + content_length].to_vec(),
  })
}

/// Transient `HomeKit` pair-setup (M1–M4, PIN 3939) over plaintext RTSP; returns the SRP session key.
async fn transient_pair_setup(rtsp: &mut TcpStream) -> [u8; 64] {
  let prime = BigUint::parse_bytes(SRP_N_HEX.as_bytes(), 16).expect("SRP prime");
  let generator = BigUint::from(5_u32);

  let mut m1 = TlvValues::new();
  m1.add(TlvType::State as u8, &[1]);
  m1.add(TlvType::Method as u8, &[0]);
  m1.add(TlvType::Flags as u8, &[0x10]);
  let m2 = TlvValues::decode(&pair_setup_post(rtsp, 0, &m1.encode()).await).expect("M2 TLV");
  let salt = BigUint::from_bytes_be(m2.get_type(TlvType::Salt).expect("salt"));
  let big_b = BigUint::from_bytes_be(m2.get_type(TlvType::PublicKey).expect("B"));

  // Fixed client secret: the test is deterministic; SRP security is not under test.
  let secret = BigUint::from_bytes_be(&[0x42_u8; 32]);
  let big_a = generator.modpow(&secret, &prime);
  let multiplier = hash_padded(&prime, &generator, 384);
  let scramble = hash_padded(&big_a, &big_b, 384);
  let identity_hash = sha512(b"Pair-Setup:3939");
  let private_key = BigUint::from_bytes_be(&sha512(&[min_bytes(&salt), identity_hash.to_vec()].concat()));
  let kgx = (&multiplier * generator.modpow(&private_key, &prime)) % &prime;
  let base = (&big_b + &prime - kgx) % &prime;
  let big_s = base.modpow(&(&secret + &scramble * &private_key), &prime);
  let session_key = sha512(&min_bytes(&big_s));

  let h_n = sha512(&min_bytes(&prime));
  let h_g = sha512(&min_bytes(&generator));
  let h_xor: Vec<u8> = h_n.iter().zip(h_g.iter()).map(|(l, r)| l ^ r).collect();
  let mut proof = Sha512::new();
  proof.update(&h_xor);
  proof.update(sha512(b"Pair-Setup"));
  proof.update(min_bytes(&salt));
  proof.update(min_bytes(&big_a));
  proof.update(min_bytes(&big_b));
  proof.update(session_key);
  let client_proof: [u8; 64] = proof.finalize().into();

  let mut m3 = TlvValues::new();
  m3.add(TlvType::State as u8, &[3]);
  m3.add(TlvType::PublicKey as u8, &min_bytes(&big_a));
  m3.add(TlvType::Proof as u8, &client_proof);
  let m4 = TlvValues::decode(&pair_setup_post(rtsp, 1, &m3.encode()).await).expect("M4 TLV");
  assert_eq!(m4.get_type(TlvType::State), Some(&[4_u8][..]), "M4 state");
  assert!(m4.get_type(TlvType::Error).is_none(), "M4 error");
  session_key
}

async fn pair_setup_post(rtsp: &mut TcpStream, cseq: u32, body: &[u8]) -> Vec<u8> {
  let head = format!(
    "POST /pair-setup RTSP/1.0\r\nCSeq: {cseq}\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\n\r\n",
    body.len()
  );
  rtsp.write_all(head.as_bytes()).await.expect("write pair-setup");
  rtsp.write_all(body).await.expect("write pair-setup body");
  let mut plain = Vec::new();
  let mut buf = vec![0_u8; 8 * 1024];
  loop {
    if let Some(resp) = parse_response(&plain) {
      assert_eq!(resp.status, 200, "pair-setup status");
      return resp.body;
    }
    let n = tokio::time::timeout(RTSP_TIMEOUT, rtsp.read(&mut buf))
      .await
      .expect("pair-setup response timed out")
      .expect("read pair-setup");
    assert!(n > 0, "receiver closed during pair-setup");
    plain.extend_from_slice(&buf[..n]);
  }
}

fn sha512(data: &[u8]) -> [u8; 64] {
  Sha512::digest(data).into()
}

fn min_bytes(n: &BigUint) -> Vec<u8> {
  let bytes = n.to_bytes_be();
  if bytes.is_empty() { vec![0] } else { bytes }
}

fn hash_padded(left: &BigUint, right: &BigUint, len: usize) -> BigUint {
  let pad = |n: &BigUint| {
    let bytes = n.to_bytes_be();
    let mut out = vec![0_u8; len.saturating_sub(bytes.len())];
    out.extend_from_slice(&bytes);
    out
  };
  BigUint::from_bytes_be(&sha512(&[pad(left), pad(right)].concat()))
}
