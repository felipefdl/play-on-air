//! Shared fixtures for bridge unit tests (no live Cast worker).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use parking_lot::Mutex;

use crate::audio::PcmRing;
use crate::bridge::state::{CastRoute, DeviceState, EgressKind, PlayingSession, SessionTasks};
use crate::bridge::{Bridge, RingLookup};
use crate::cast::CastControl;
use crate::media::MediaServerHandle;

/// Minimal playing session for teardown / pause tests (no live Cast worker).
pub(super) fn test_playing_session(
  media: MediaServerHandle,
  device_id: &str,
  pool: Arc<dyn CastControl>,
  ring: Arc<PcmRing>,
) -> PlayingSession {
  PlayingSession {
    media,
    device_id: device_id.to_owned(),
    pool,
    route: Arc::new(CastRoute::new(device_id, device_id)),
    group: None,
    ring,
    source: None,
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
    stall_reload_owed: false,
    started_at: Instant::now(),
    flac_strike: None,
    transport_flight: Arc::new(tokio::sync::Mutex::new(())),
    tasks: SessionTasks::default(),
    rollover_cancel: None,
    rollover_task: None,
    watchdog_cancel: None,
    watchdog_task: None,
    session_alive: Arc::new(AtomicBool::new(true)),
    late_stop_allowed: Arc::new(AtomicBool::new(true)),
    last_volume_linear: Arc::new(Mutex::new(None)),
    track: Arc::new(Mutex::new(crate::cast::CastTrack::default())),
    inflight_load: Arc::new(Mutex::new(None)),
    reload_flight: Arc::new(tokio::sync::Mutex::new(())),
    sync: None,
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

/// A Cast command the bridge issued to [`FakeCast`].
#[derive(Debug, Clone, PartialEq)]
pub(super) enum CastCall {
  Load(String),
  SetVolume(String),
  GetVolume(String),
  Pause(String),
  Play(String),
  MediaStatus(String),
  Stop(String),
  SessionEnded(String),
  Metadata(String),
  SetPlaybackDevices {
    key: String,
    members: Vec<String>,
  },
  OpenHost {
    key: String,
    host: String,
    port: u16,
  },
  Move {
    from: String,
    to: String,
    session_id: Option<String>,
  },
  CloseHost(String),
  MultizoneStatus(String),
}

/// One-shot callback run inside the next [`FakeCast`] LOAD (simulates an event racing it).
pub(super) struct LoadHook(Box<dyn FnOnce() + Send>);

impl std::fmt::Debug for LoadHook {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str("LoadHook")
  }
}

/// Recording [`CastControl`] with scripted multizone answers (no network).
#[derive(Debug, Default)]
pub(super) struct FakeCast {
  calls: Mutex<Vec<CastCall>>,
  /// Answers to `SET_PLAYBACK_DEVICES`, in order; an empty queue answers success, no move.
  answers: Mutex<std::collections::VecDeque<crate::error::Result<crate::cast::PlaybackSessionUpdate>>>,
  /// Run (once) while the next LOAD is on the wire.
  load_hook: Mutex<Option<LoadHook>>,
  /// Results of the next `move_session` calls, in order; an empty queue moves successfully.
  moves: Mutex<std::collections::VecDeque<crate::error::Result<()>>>,
  /// `MULTIZONE_STATUS` members per group endpoint key; a key without an entry answers an error.
  members: Mutex<HashMap<String, Vec<crate::cast::MultizoneMember>>>,
  /// Every `set_volume` as `(key, level)`, in call order.
  volumes: Mutex<Vec<(String, f32)>>,
}

impl FakeCast {
  pub(super) fn answer(&self, answer: crate::error::Result<crate::cast::PlaybackSessionUpdate>) {
    self.answers.lock().push_back(answer);
  }

  /// Answer the next `move_session` with `result` (an adopt that fails on the new host).
  pub(super) fn answer_move(&self, result: crate::error::Result<()>) {
    self.moves.lock().push_back(result);
  }

  /// Run `hook` during the next LOAD, before it returns.
  pub(super) fn on_next_load(&self, hook: impl FnOnce() + Send + 'static) {
    *self.load_hook.lock() = Some(LoadHook(Box::new(hook)));
  }

  /// Members the group endpoint `key` lists in `MULTIZONE_STATUS`.
  pub(super) fn set_members(&self, key: &str, members: Vec<crate::cast::MultizoneMember>) {
    drop(self.members.lock().insert(key.to_owned(), members));
  }

  /// Levels `set_volume` was called with, in call order.
  pub(super) fn volumes(&self) -> Vec<(String, f32)> {
    self.volumes.lock().clone()
  }

  pub(super) fn calls(&self) -> Vec<CastCall> {
    self.calls.lock().clone()
  }

  fn record(&self, call: CastCall) {
    self.calls.lock().push(call);
  }
}

impl CastControl for FakeCast {
  fn load(
    &self,
    key: &str,
    _request: crate::cast::MediaLoadRequest,
  ) -> crate::error::Result<crate::cast::MediaSessionRef> {
    self.record(CastCall::Load(key.to_owned()));
    let pending = self.load_hook.lock().take();
    if let Some(LoadHook(hook)) = pending {
      hook();
    }
    Ok(crate::cast::MediaSessionRef {
      transport_id: format!("transport-{key}"),
      session_id: format!("session-{key}"),
      media_session_id: 1,
      queue_item_id: Some(1),
    })
  }

  fn set_volume(&self, key: &str, level: f32) -> crate::error::Result<()> {
    self.record(CastCall::SetVolume(key.to_owned()));
    self.volumes.lock().push((key.to_owned(), level));
    Ok(())
  }

  fn get_volume(&self, key: &str) -> crate::error::Result<f32> {
    self.record(CastCall::GetVolume(key.to_owned()));
    Ok(0.5)
  }

  fn pause(&self, key: &str) -> crate::error::Result<crate::cast::MediaPlayback> {
    self.record(CastCall::Pause(key.to_owned()));
    Ok(crate::cast::MediaPlayback {
      player_state: rust_cast::channels::media::PlayerState::Paused,
      current_time: Some(1.0),
    })
  }

  fn play(&self, key: &str) -> crate::error::Result<()> {
    self.record(CastCall::Play(key.to_owned()));
    Ok(())
  }

  fn media_playback(&self, key: &str) -> crate::error::Result<crate::cast::MediaPlayback> {
    self.record(CastCall::MediaStatus(key.to_owned()));
    Ok(crate::cast::MediaPlayback {
      player_state: rust_cast::channels::media::PlayerState::Paused,
      current_time: Some(1.0),
    })
  }

  fn stop_best_effort(&self, key: &str, _timeout: std::time::Duration) -> bool {
    self.record(CastCall::Stop(key.to_owned()));
    true
  }

  fn session_ended(&self, key: &str) {
    self.record(CastCall::SessionEnded(key.to_owned()));
  }

  fn update_metadata(&self, key: &str, _track: crate::cast::CastTrack) {
    self.record(CastCall::Metadata(key.to_owned()));
  }

  fn set_playback_devices(
    &self,
    key: &str,
    request: crate::cast::SetPlaybackDevices,
  ) -> crate::error::Result<crate::cast::PlaybackSessionUpdate> {
    self.record(CastCall::SetPlaybackDevices {
      key: key.to_owned(),
      members: request.devices.into_iter().map(|device| device.device_id).collect(),
    });
    self
      .answers
      .lock()
      .pop_front()
      .unwrap_or_else(|| Ok(crate::cast::PlaybackSessionUpdate::default()))
  }

  fn multizone_members(&self, key: &str) -> crate::error::Result<Vec<crate::cast::MultizoneMember>> {
    self.record(CastCall::MultizoneStatus(key.to_owned()));
    self
      .members
      .lock()
      .get(key)
      .cloned()
      .ok_or_else(|| crate::error::Error::Cast(format!("no multizone status for {key}")))
  }

  fn open_session_host(&self, key: &str, host: &str, port: u16) {
    self.record(CastCall::OpenHost {
      key: key.to_owned(),
      host: host.to_owned(),
      port,
    });
  }

  fn move_session(&self, from: &str, to: &str, session_id: Option<&str>) -> crate::error::Result<()> {
    self.record(CastCall::Move {
      from: from.to_owned(),
      to: to.to_owned(),
      session_id: session_id.map(str::to_owned),
    });
    self.moves.lock().pop_front().unwrap_or(Ok(()))
  }

  fn close_session_host(&self, key: &str) {
    self.record(CastCall::CloseHost(key.to_owned()));
  }
}

/// Ring lookup whose entries tests replace (a receiver's new stream, or its end).
#[derive(Default)]
pub(super) struct SwapRingLookup {
  pub(super) rings: Mutex<HashMap<String, Arc<PcmRing>>>,
}

impl SwapRingLookup {
  pub(super) fn set(&self, device_id: &str, current: Option<Arc<PcmRing>>) {
    let mut rings = self.rings.lock();
    match current {
      Some(ring) => drop(rings.insert(device_id.to_owned(), ring)),
      None => drop(rings.remove(device_id)),
    }
  }
}

impl RingLookup for SwapRingLookup {
  fn ring_for(&self, device_id: &str) -> Option<Arc<PcmRing>> {
    self.rings.lock().get(device_id).map(Arc::clone)
  }
}

/// Registry speaker on loopback.
pub(super) fn test_device(id: &str, name: &str) -> crate::registry::Device {
  crate::registry::Device {
    id: id.to_owned(),
    name: name.to_owned(),
    model: String::new(),
    host: "127.0.0.1".to_owned(),
    hostname: format!("{id}.local"),
    port: 8009,
    last_seen: Instant::now(),
    instance: id.to_owned(),
    pending_leave_deadline: None,
    pending_leave_since: None,
  }
}
