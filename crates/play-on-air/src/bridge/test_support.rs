//! Shared fixtures for bridge unit tests (no live Cast worker).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use parking_lot::Mutex;

use crate::audio::PcmRing;
use crate::bridge::state::{DeviceState, EgressKind, PlayingSession, SessionTasks};
use crate::bridge::{Bridge, RingLookup};
use crate::cast::CastPool;
use crate::media::MediaServerHandle;

/// Minimal playing session for teardown / pause tests (no live Cast worker).
pub(super) fn test_playing_session(
  media: MediaServerHandle,
  device_id: &str,
  pool: Arc<CastPool>,
  ring: Arc<PcmRing>,
) -> PlayingSession {
  PlayingSession {
    media,
    device_id: device_id.to_owned(),
    pool,
    ring,
    stream_url: "http://127.0.0.1:9/stream".to_owned(),
    cast_name: "test".to_owned(),
    channels: 2,
    sample_rate: 48_000,
    egress: EgressKind::FlacLive,
    paused: false,
    playback_paused: Arc::new(AtomicBool::new(false)),
    paused_at: None,
    flush_reload_requested: 0,
    flush_reload_done: 0,
    last_stall_reload_at: None,
    started_at: Instant::now(),
    flac_strike: None,
    pause_debounce_arm: 0,
    pause_debounce_cancel: None,
    flush_debounce_arm: 0,
    flush_debounce_cancel: None,
    tasks: SessionTasks::default(),
    rollover_cancel: None,
    rollover_task: None,
    watchdog_cancel: None,
    watchdog_task: None,
    session_alive: Arc::new(AtomicBool::new(true)),
    late_stop_allowed: Arc::new(AtomicBool::new(true)),
    last_volume_linear: Arc::new(Mutex::new(None)),
    inflight_load: Arc::new(Mutex::new(None)),
    reload_flight: Arc::new(tokio::sync::Mutex::new(())),
  }
}

pub(super) fn insert_playing(bridge: &Bridge, device_id: &str, generation: u64, session: PlayingSession) {
  let mut devices = bridge.devices.lock();
  let slot = devices.entry(device_id.to_owned()).or_default();
  slot.generation = generation;
  slot.state = DeviceState::Playing { generation, session: Box::new(session) };
  drop(devices);
}

pub(super) async fn http_get_status_ok(url: &str) -> bool {
  use tokio::io::{AsyncReadExt, AsyncWriteExt};
  use tokio::net::TcpStream;

  let Some(without_scheme) = url.strip_prefix("http://") else {
    return false;
  };
  let Some((host_port, path)) = without_scheme.split_once('/') else {
    return false;
  };
  let Ok(mut stream) = TcpStream::connect(host_port).await else {
    return false;
  };
  let req = format!("GET /{path} HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");
  if stream.write_all(req.as_bytes()).await.is_err() {
    return false;
  }
  let mut buf = Vec::new();
  if stream.read_to_end(&mut buf).await.is_err() {
    return false;
  }
  let text = String::from_utf8_lossy(&buf);
  text.contains("200")
}

/// Ring lookup returning the same ring (or none) for every device.
pub(super) struct FixedRingLookup {
  pub(super) current: Option<Arc<PcmRing>>,
}

impl RingLookup for FixedRingLookup {
  fn ring_for(&self, _device_id: &str) -> Option<Arc<PcmRing>> {
    self.current.clone()
  }
}

/// Ring lookup keyed by device id.
pub(super) struct MapRingLookup {
  pub(super) rings: HashMap<String, Arc<PcmRing>>,
}

impl RingLookup for MapRingLookup {
  fn ring_for(&self, device_id: &str) -> Option<Arc<PcmRing>> {
    self.rings.get(device_id).map(Arc::clone)
  }
}
