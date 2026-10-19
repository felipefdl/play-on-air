//! Session start: prebuffer, initial Cast LOAD (FLAC → WAV fallback), and `Playing` install.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::time::sleep;

use crate::airplay::cast_linear_to_airplay_db;
use crate::audio::sync::{SyncTimeline, resolve_sync};
use crate::audio::{FLAC_BLOCK_SIZE, PcmRing};
use crate::cast::{CastStreamKind, MediaLoadRequest};
use crate::error::{Error, Result};
use crate::media::{MediaContent, MediaServer, MediaServerHandle};
use crate::net::advertise_host_for_peer;

use super::fallback::is_transport_shaped_cast_error;
use super::group::GroupEntry;
use super::load::{LoadVolumePolicy, cast_load_media, spawn_rollover_reload_loop};
use super::stall::spawn_stall_watchdog;
use super::state::SessionTasks;
use super::state::{CastRoute, DeviceSlot, DeviceState, EgressKind, PlayingSession, select_egress};
use super::sync::{SyncAttach, SyncSession, run_sync};
use super::teardown::TeardownCastPolicy;
use super::{Bridge, RingLookup};

/// Frames of real PCM required before Cast LOAD: one FLAC block (~43 ms at 48 kHz).
///
/// shairplay hands PCM over on the sender's anchor timeline, up to `CAST_READ_AHEAD` before
/// presentation (a burst when an anchor arrives with audio queued, then real time), so the
/// prebuffer only confirms that audio is flowing. Whatever the ring holds when Cast GETs the
/// stream is burst to it, and the media server's pacing keeps Cast that far ahead.
pub(super) const PREBUFFER_FRAMES: usize = FLAC_BLOCK_SIZE;

/// Max prebuffer poll iterations (160 × 50 ms = 8 s). Real-time PCM reaches one block within
/// tens of milliseconds, so running out means the sender paused or stopped before audio flowed.
const PREBUFFER_POLLS: u32 = 160;

const PREBUFFER_POLL: Duration = Duration::from_millis(50);

/// PCM layout advertised to the media server and Cast load.
#[derive(Debug, Clone, Copy)]
struct StreamFormat {
  channels: u16,
  sample_rate: u32,
}

/// Clears `Starting{generation}` if still armed when dropped (panic safety for session start).
struct StartingGuard<'a> {
  bridge: &'a Bridge,
  device_id: String,
  generation: u64,
  armed: bool,
}

impl<'a> StartingGuard<'a> {
  fn arm(bridge: &'a Bridge, device_id: &str, generation: u64) -> Self {
    Self {
      bridge,
      device_id: device_id.to_owned(),
      generation,
      armed: true,
    }
  }

  const fn disarm(&mut self) {
    self.armed = false;
  }
}

impl Drop for StartingGuard<'_> {
  fn drop(&mut self) {
    if self.armed {
      self.bridge.clear_starting_if(&self.device_id, self.generation);
    }
  }
}

/// Bump `slot`'s generation and install `Starting`; return the prior `Playing` session, if any.
pub(super) fn accept_start_in(slot: &mut DeviceSlot, device_id: &str) -> (u64, Option<Box<PlayingSession>>) {
  slot.generation = slot.generation.saturating_add(1);
  let generation = slot.generation;
  let previous = std::mem::replace(&mut slot.state, DeviceState::Starting { generation });
  let prior = match previous {
    DeviceState::Playing { generation: old_gen, session } => {
      // Prior session tears down on a detached task; slot is already Starting{new}.
      // Skip Cast STOP on that path so the new LOAD is not HOL-blocked.
      tracing::debug!(%device_id, old_gen, new_gen = generation, "prior Playing moved to detached drain");
      Some(session)
    },
    DeviceState::Starting { generation: old_gen } => {
      tracing::debug!(%device_id, old_gen, new_gen = generation, "superseding in-flight Starting");
      None
    },
    DeviceState::Following { generation: old_gen, follower } => {
      tracing::debug!(%device_id, old_gen, new_gen = generation, "group follower starts its own session");
      follower.pump.stop();
      None
    },
    DeviceState::Idle => None,
  };
  (generation, prior)
}

/// Poll until [`PREBUFFER_FRAMES`] are available, or the ring is superseded.
///
/// Returns `Ok(true)` when the prebuffer is ready, `Ok(false)` if the session restarted (stale
/// ring), and an error when the 8 s budget runs out first.
async fn wait_for_prebuffer(device_id: &str, ring: &Arc<PcmRing>, rings: &dyn RingLookup) -> Result<bool> {
  for _ in 0..PREBUFFER_POLLS {
    let still_current = rings.ring_for(device_id).is_some_and(|current| Arc::ptr_eq(&current, ring));
    if !still_current {
      tracing::info!(%device_id, "session restarted during prebuffer; skipping stale start");
      return Ok(false);
    }
    if ring.available_frames() >= PREBUFFER_FRAMES {
      return Ok(true);
    }
    sleep(PREBUFFER_POLL).await;
  }
  let available = ring.available_frames();
  if available >= PREBUFFER_FRAMES {
    return Ok(true);
  }
  Err(Error::Bridge(format!(
    "prebuffer timeout after 8s: {available} frames available, need {PREBUFFER_FRAMES}"
  )))
}

impl Bridge {
  pub(super) async fn handle_session_start(
    self: &Arc<Self>,
    device_id: &str,
    sample_rate: u32,
    ring: Arc<PcmRing>,
    airplay_group: Option<String>,
    rings: Arc<dyn RingLookup>,
  ) -> Result<()> {
    let is_current = |expected: &Arc<PcmRing>| -> bool {
      rings.ring_for(device_id).is_some_and(|current| Arc::ptr_eq(&current, expected))
    };

    // A stale Started means the client already restarted the stream (a fresh
    // event with the rebuilt ring is queued behind this one) or the receiver
    // was withdrawn. Skip instead of bridging a ring that no longer feeds.
    if !is_current(&ring) {
      tracing::info!(%device_id, "skipping stale AirPlay session start (ring rebuilt or receiver gone)");
      return Ok(());
    }

    // Test seam: both concurrent starts must reach this wait for the barrier to open.
    let start_gate = self.start_barrier.lock().clone();
    if let Some(gate) = start_gate {
      let _party = gate.wait().await;
    }

    // Accept start: bump generation, install Starting, detach prior Playing teardown.
    // Do NOT await prior Cast STOP — new LOAD replaces the app session (SkipStopForReplace).
    // Every non-install exit after this point must clear Starting for this generation.
    // StartingGuard also clears on panic between accept and Playing install.
    // AirPlay multi-select: the first member to start owns the Cast session; the others
    // join it through Cast multizone instead of loading their own stream.
    let (generation, prior, group, media_key) = if let Some(uuid) = airplay_group {
      match self.enter_group(device_id, &uuid, sample_rate, &ring) {
        GroupEntry::Following { prior } => {
          if let Some(old) = prior {
            // The member's own Cast session ends; multizone adds the speaker to the group.
            self.detach_teardown(*old, TeardownCastPolicy::StopBestEffort);
          }
          self.schedule_group_reconcile(&uuid);
          return Ok(());
        },
        GroupEntry::Owner { generation, prior, media_key } => (generation, prior, Some(uuid), media_key),
        GroupEntry::Standalone => {
          let (generation, prior) = self.accept_start(device_id);
          (generation, prior, None, None)
        },
      }
    } else {
      let (generation, prior) = self.accept_start(device_id);
      (generation, prior, None, None)
    };
    let route = Arc::new(CastRoute::new(media_key.as_deref().unwrap_or(device_id), device_id));
    let mut starting_guard = StartingGuard::arm(self, device_id, generation);

    if let Some(old) = prior {
      tracing::info!(
        %device_id,
        new_gen = generation,
        "detaching prior session teardown (skip Cast STOP; new LOAD replaces)"
      );
      self.detach_teardown(*old, TeardownCastPolicy::SkipStopForReplace);
    }

    let Some(device) = self.registry.get(device_id) else {
      self.clear_starting_if(device_id, generation);
      return Err(Error::Bridge(format!("unknown device {device_id}")));
    };

    let fmt = StreamFormat {
      channels: ring.channels().max(1),
      sample_rate: sample_rate.max(1),
    };

    match wait_for_prebuffer(device_id, &ring, rings.as_ref()).await {
      Ok(true) => {},
      Ok(false) => {
        self.clear_starting_if(device_id, generation);
        return Ok(());
      },
      Err(err) => {
        self.clear_starting_if(device_id, generation);
        return Err(err);
      },
    }

    if !is_current(&ring) || !self.starting_still(device_id, generation) {
      tracing::info!(%device_id, generation, "session restarted before Cast load; skipping stale start");
      self.clear_starting_if(device_id, generation);
      return Ok(());
    }

    let result = self
      .start_cast_session(device_id, generation, &device, ring, fmt, route, group)
      .await;
    // Disarm only when this generation is live Playing (install kept the session).
    // clear_starting_if is a no-op on Playing; keep explicit error clears + guard for panics.
    if result.is_ok()
      && self
        .devices
        .lock()
        .get(device_id)
        .is_some_and(|slot| matches!(slot.state, DeviceState::Playing { generation: g, .. } if g == generation))
    {
      starting_guard.disarm();
    }
    result
  }

  /// Bump generation, install `Starting`, return prior `Playing` session if any.
  pub(super) fn accept_start(&self, device_id: &str) -> (u64, Option<Box<PlayingSession>>) {
    let mut devices = self.devices.lock();
    let slot = devices.entry(device_id.to_owned()).or_default();
    let accepted = accept_start_in(slot, device_id);
    drop(devices);
    accepted
  }

  pub(super) fn clear_starting_if(&self, device_id: &str, generation: u64) {
    let mut devices = self.devices.lock();
    let Some(slot) = devices.get_mut(device_id) else {
      return;
    };
    if matches!(slot.state, DeviceState::Starting { generation: g } if g == generation) {
      slot.state = DeviceState::Idle;
    }
    drop(devices);
  }

  fn starting_still(&self, device_id: &str, generation: u64) -> bool {
    let devices = self.devices.lock();
    devices
      .get(device_id)
      .is_some_and(|slot| matches!(slot.state, DeviceState::Starting { generation: g } if g == generation))
  }

  #[expect(
    clippy::too_many_arguments,
    reason = "session start carries device identity, ring, format, Cast route, and group"
  )]
  async fn start_cast_session(
    self: &Arc<Self>,
    device_id: &str,
    generation: u64,
    device: &crate::registry::Device,
    ring: Arc<PcmRing>,
    fmt: StreamFormat,
    route: Arc<CastRoute>,
    group: Option<String>,
  ) -> Result<()> {
    let host = advertise_host_for_peer(&device.host);
    let media = match MediaServer::start(&host).await {
      Ok(media) => media,
      Err(err) => {
        self.clear_starting_if(device_id, generation);
        return Err(err);
      },
    };
    let stream_url = media.stream_url();

    let remembered_wav = self.flac_fallback.lock().contains(device_id);
    let mut egress = select_egress(remembered_wav);

    tracing::info!(
      %device_id,
      generation,
      cast = %device.host,
      cast_target = %route.media(),
      group = ?group,
      %stream_url,
      ?egress,
      frames = ring.available_frames(),
      "starting Cast bridge session"
    );

    let session_ring = Arc::clone(&ring);
    let mut sync_attach = None;
    let load_result = self
      .initial_cast_load(
        device_id,
        generation,
        device,
        &route,
        &media,
        &ring,
        fmt,
        &stream_url,
        &mut egress,
        &mut sync_attach,
      )
      .await;

    match load_result {
      Ok(session) => {
        // Post-LOAD liveness: do not install if Ended / superseding start cancelled Starting.
        if !self.starting_still(device_id, generation) {
          tracing::info!(
            %device_id,
            generation,
            "Cast LOAD completed but start was cancelled; discarding session"
          );
          media.shutdown();
          let pool = Arc::clone(&self.cast_pool);
          let id = route.media();
          let _stop = tokio::task::spawn_blocking(move || {
            let _ok = pool.stop_best_effort(&id, Duration::from_secs(2));
          })
          .await;
          self.clear_starting_if(device_id, generation);
          return Ok(());
        }
        tracing::info!(
          %device_id,
          generation,
          cast = %device.host,
          transport_id = %session.transport_id,
          media_session_id = session.media_session_id,
          %stream_url,
          ?egress,
          "bridge session Cast load ok"
        );
        self
          .install_playing_after_load(
            device_id,
            generation,
            device,
            media,
            session_ring,
            stream_url,
            fmt,
            egress,
            route,
            group,
            sync_attach,
          )
          .await;
        Ok(())
      },
      Err(err) => {
        media.shutdown();
        self.clear_starting_if(device_id, generation);
        tracing::warn!(%device_id, generation, error = %err, "Cast load failed (device may be offline)");
        Err(err)
      },
    }
  }

  /// Perform the initial Cast LOAD, with FLAC → WAV fallback on LOAD error.
  #[expect(
    clippy::too_many_arguments,
    reason = "LOAD needs device identity, media content inputs, and egress out-param"
  )]
  async fn initial_cast_load(
    &self,
    device_id: &str,
    generation: u64,
    device: &crate::registry::Device,
    route: &Arc<CastRoute>,
    media: &MediaServerHandle,
    ring: &Arc<PcmRing>,
    fmt: StreamFormat,
    stream_url: &str,
    egress: &mut EgressKind,
    sync_out: &mut Option<SyncAttach>,
  ) -> Result<crate::cast::MediaSessionRef> {
    let track = self.load_track(device_id, media, &device.name);
    match *egress {
      EgressKind::FlacLive => {
        media.set_content(MediaContent::LiveFlac {
          ring: Arc::clone(ring),
          channels: fmt.channels,
          sample_rate: fmt.sample_rate,
        });
        // Cast speaker groups (multizone) keep the normal path; an AirPlay multi-select with one
        // Cast speaker in it is exactly the mixed-vendor case sync targets.
        let syncing = resolve_sync(&device.model);
        let attached = syncing.as_ref().map(|params| {
          let created = Arc::new(SyncTimeline::new(fmt.sample_rate, device_id, &params.model));
          ring.attach_timeline(Arc::clone(&created));
          created.prepare_for_load();
          created
        });
        let mut request = MediaLoadRequest::flac(stream_url.to_owned(), CastStreamKind::Live).with_track(track.clone());
        if attached.is_some() {
          request = request.with_autoplay(false);
        }
        let flac_result = self.blocking_load(route, request).await?;
        match flac_result {
          Ok(session) => {
            if let (Some(created), Some(params)) = (attached, syncing) {
              *sync_out = Some(SyncAttach { timeline: created, params });
            }
            Ok(session)
          },
          Err(flac_err) => {
            ring.detach_timeline();
            // Always WAV for this session. Process-lifetime memory only on decisive rejection
            // (not timeout / disconnect / no worker) so transport blips do not poison FLAC forever.
            let remember = !is_transport_shaped_cast_error(&flac_err);
            tracing::warn!(
              %device_id,
              generation,
              error = %flac_err,
              remember_fallback = remember,
              "Cast FLAC LIVE load failed; falling back to WAV BUFFERED"
            );
            if remember {
              self.remember_flac_fallback(device_id);
            }
            *egress = EgressKind::WavBuffered;
            media.set_content(MediaContent::LiveWav {
              ring: Arc::clone(ring),
              channels: fmt.channels,
              sample_rate: fmt.sample_rate,
            });
            self
              .blocking_load(
                route,
                MediaLoadRequest::wav(stream_url.to_owned(), CastStreamKind::Buffered).with_track(track),
              )
              .await?
          },
        }
      },
      EgressKind::WavBuffered => {
        media.set_content(MediaContent::LiveWav {
          ring: Arc::clone(ring),
          channels: fmt.channels,
          sample_rate: fmt.sample_rate,
        });
        self
          .blocking_load(
            route,
            MediaLoadRequest::wav(stream_url.to_owned(), CastStreamKind::Buffered).with_track(track),
          )
          .await?
      },
    }
  }

  async fn blocking_load(
    &self,
    route: &Arc<CastRoute>,
    request: MediaLoadRequest,
  ) -> Result<std::result::Result<crate::cast::MediaSessionRef, Error>> {
    let pool = Arc::clone(&self.cast_pool);
    let load_route = Arc::clone(route);
    tokio::task::spawn_blocking(move || {
      cast_load_media(pool.as_ref(), &load_route, request, LoadVolumePolicy::PreserveDevice)
    })
    .await
    .map_err(|join_err| Error::Bridge(format!("Cast load task join: {join_err}")))
  }

  #[expect(
    clippy::too_many_arguments,
    reason = "playing install needs media handle, ring, and session identity"
  )]
  async fn install_playing_after_load(
    self: &Arc<Self>,
    device_id: &str,
    generation: u64,
    device: &crate::registry::Device,
    media: MediaServerHandle,
    session_ring: Arc<PcmRing>,
    stream_url: String,
    fmt: StreamFormat,
    egress: EgressKind,
    route: Arc<CastRoute>,
    group: Option<String>,
    sync_attach: Option<SyncAttach>,
  ) {
    let cast_linear = self.sync_reported_volume_after_load(device_id).await;
    let session_alive = Arc::new(AtomicBool::new(true));
    let late_stop_allowed = Arc::new(AtomicBool::new(true));
    let last_volume_linear = Arc::new(Mutex::new(cast_linear));
    let track = Arc::new(Mutex::new(self.load_track(device_id, &media, &device.name)));
    let inflight_load = Arc::new(Mutex::new(None));
    let reload_flight = Arc::new(tokio::sync::Mutex::new(()));
    let playback_paused = Arc::new(AtomicBool::new(false));
    let transport_flight = Arc::new(tokio::sync::Mutex::new(()));
    let sync_session = sync_attach.map(|attach| {
      Arc::new(SyncSession::from_attach(
        attach,
        device_id,
        fmt.sample_rate,
        Arc::clone(&self.cast_pool),
        Arc::clone(&route),
        Arc::clone(&session_alive),
        Arc::clone(&playback_paused),
        Arc::clone(&transport_flight),
      ))
    });

    let (rollover_cancel, rollover_task) = if egress == EgressKind::WavBuffered {
      let (cancel, task) = spawn_rollover_reload_loop(
        Arc::clone(&route),
        stream_url.clone(),
        Arc::clone(&track),
        Arc::clone(&self.cast_pool),
        media.rollover_signal(),
        Arc::clone(&session_alive),
        Arc::clone(&late_stop_allowed),
        Arc::clone(&last_volume_linear),
        Arc::clone(&inflight_load),
        Arc::clone(&reload_flight),
      );
      (Some(cancel), Some(task))
    } else {
      (None, None)
    };

    let (watchdog_cancel, watchdog_task) = spawn_stall_watchdog(Arc::clone(self), device_id.to_owned(), generation);

    let mut playing = PlayingSession {
      media,
      device_id: device_id.to_owned(),
      pool: Arc::clone(&self.cast_pool),
      route,
      group: group.clone(),
      ring: session_ring,
      source: None,
      stream_url,
      cast_name: device.name.clone(),
      channels: fmt.channels,
      sample_rate: fmt.sample_rate,
      egress,
      paused: false,
      playback_paused,
      paused_at: None,
      flush_reload_requested: 0,
      flush_reload_done: 0,
      last_stall_reload_at: None,
      stall_reload_owed: false,
      started_at: Instant::now(),
      flac_strike: None,
      transport_flight,
      tasks: SessionTasks::default(),
      rollover_cancel,
      rollover_task,
      watchdog_cancel: Some(watchdog_cancel),
      watchdog_task: Some(watchdog_task),
      session_alive,
      late_stop_allowed,
      last_volume_linear,
      track,
      inflight_load,
      reload_flight,
      sync: sync_session,
    };

    if let Some(session) = playing.sync.as_ref().map(Arc::clone) {
      let _task = playing.tasks.spawn(device_id, run_sync(session));
    }

    let orphan = {
      let mut devices = self.devices.lock();
      let result = match devices.get_mut(device_id) {
        Some(slot) if matches!(slot.state, DeviceState::Starting { generation: g } if g == generation) => {
          slot.state = DeviceState::Playing { generation, session: Box::new(playing) };
          None
        },
        Some(_) | None => Some(playing),
      };
      drop(devices);
      result
    };

    if let Some(orphaned) = orphan {
      tracing::info!(%device_id, generation, "start superseded before Playing install; tearing down");
      self.detach_teardown(orphaned, TeardownCastPolicy::StopBestEffort);
    } else if let Some(uuid) = group {
      self.group_owner_playing(&uuid, device_id);
    }
  }

  /// Read Cast volume after LOAD and refresh AirPlay `GET_PARAMETER` if a manager is attached.
  async fn sync_reported_volume_after_load(&self, device_id: &str) -> Option<f32> {
    let pool = Arc::clone(&self.cast_pool);
    let id = device_id.to_owned();
    let level = match tokio::task::spawn_blocking(move || pool.get_volume(&id)).await {
      Ok(Ok(level)) => level,
      Ok(Err(err)) => {
        tracing::debug!(%device_id, error = %err, "Cast get_volume after LOAD failed");
        return None;
      },
      Err(err) => {
        tracing::debug!(%device_id, error = %err, "Cast get_volume task join failed");
        return None;
      },
    };
    let db = cast_linear_to_airplay_db(level);
    if let Some(airplay) = &self.airplay {
      airplay.set_reported_volume_db(device_id, db);
    }
    tracing::info!(
      %device_id,
      cast_linear = level,
      airplay_db = db,
      "synced AirPlay reported volume from Cast"
    );
    Some(level)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::bridge::state::DeviceSlot;
  use crate::bridge::teardown::teardown_playing;
  use crate::bridge::test_support::{FixedRingLookup, insert_playing, test_playing_session};
  use crate::cast::CastControl;
  use crate::cast::CastPool;
  use crate::registry::{Device, DeviceRegistry};

  #[test]
  fn accept_start_bumps_generation_and_returns_prior_playing() {
    let bridge = Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None)));
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
    let media = runtime.block_on(async { MediaServer::start("127.0.0.1").await.expect("media") });
    let pool: Arc<dyn CastControl> = Arc::new(CastPool::new(None));
    let ring1 = Arc::new(PcmRing::new(2, 64));
    insert_playing(&bridge, "dev", 1, test_playing_session(media, "dev", pool, Arc::clone(&ring1)));

    let (generation, prior) = bridge.accept_start("dev");
    assert_eq!(generation, 2);
    assert!(prior.is_some());
    assert!(bridge.has_session("dev"));
    assert!(matches!(
      bridge.devices.lock().get("dev").map(|s| &s.state),
      Some(DeviceState::Starting { generation: 2 })
    ));
    // Detached teardown of prior would normally run; drop it here to shut media.
    if let Some(old) = prior {
      runtime.block_on(teardown_playing(*old, TeardownCastPolicy::SkipStopForReplace));
    }
  }

  #[tokio::test]
  async fn stale_session_start_skips_without_prebuffer_or_session() {
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None))));
    let event_ring = Arc::new(PcmRing::new(2, 64));
    let rebuilt_ring = Arc::new(PcmRing::new(2, 64));
    let rings: Arc<dyn RingLookup> = Arc::new(FixedRingLookup { current: Some(rebuilt_ring) });

    let start = Instant::now();
    bridge
      .handle_session_start("dev-1", 48_000, event_ring, None, rings)
      .await
      .expect("stale start skips cleanly");
    assert!(
      start.elapsed() < Duration::from_secs(1),
      "stale start must not prebuffer or Cast-load"
    );
    assert!(!bridge.has_session("dev-1"));
  }

  #[tokio::test]
  async fn session_start_with_receiver_gone_skips() {
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None))));
    let event_ring = Arc::new(PcmRing::new(2, 64));
    let rings: Arc<dyn RingLookup> = Arc::new(FixedRingLookup { current: None });

    bridge
      .handle_session_start("dev-1", 48_000, event_ring, None, rings)
      .await
      .expect("withdrawn receiver skips cleanly");
    assert!(!bridge.has_session("dev-1"));
  }

  #[tokio::test]
  async fn session_start_unknown_device_clears_starting() {
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None))));
    let ring = Arc::new(PcmRing::new(2, 64));
    let rings: Arc<dyn RingLookup> = Arc::new(FixedRingLookup { current: Some(Arc::clone(&ring)) });
    let err = bridge
      .handle_session_start("missing-dev", 48_000, ring, None, rings)
      .await
      .expect_err("unknown device must fail");
    assert!(err.to_string().contains("unknown device"), "unexpected error: {err}");
    assert!(!bridge.has_session("missing-dev"), "failed start path must clear Starting");
  }

  #[tokio::test(start_paused = true)]
  async fn session_start_prebuffer_timeout_clears_starting() {
    let registry = Arc::new(DeviceRegistry::new());
    let _appeared = registry.appear(Device {
      id: "dev-prebuf".to_owned(),
      name: "Prebuf".to_owned(),
      model: String::new(),
      host: "127.0.0.1".to_owned(),
      hostname: "prebuf.local".to_owned(),
      port: 9,
      last_seen: Instant::now(),
      instance: "dev-prebuf".to_owned(),
      pending_leave_deadline: None,
      pending_leave_since: None,
    });
    let bridge = Arc::new(Bridge::new(registry, Arc::new(CastPool::new(None))));
    // Current ring but empty (below one block) → prebuffer polls until timeout (virtual time advances).
    let ring = Arc::new(PcmRing::new(2, PREBUFFER_FRAMES * 2));
    let rings: Arc<dyn RingLookup> = Arc::new(FixedRingLookup { current: Some(Arc::clone(&ring)) });
    let err = bridge
      .handle_session_start("dev-prebuf", 48_000, ring, None, rings)
      .await
      .expect_err("empty ring must prebuffer-timeout");
    let msg = err.to_string();
    assert!(msg.contains("prebuffer timeout"), "unexpected error: {msg}");
    assert!(
      msg.contains(&PREBUFFER_FRAMES.to_string()),
      "timeout error should mention the prebuffer target: {msg}"
    );
    assert!(!bridge.has_session("dev-prebuf"), "prebuffer timeout must clear Starting");
  }

  #[test]
  fn starting_guard_clears_starting_on_drop() {
    let bridge = Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None)));
    {
      let mut devices = bridge.devices.lock();
      drop(devices.insert(
        "dev-guard".to_owned(),
        DeviceSlot {
          generation: 3,
          state: DeviceState::Starting { generation: 3 },
        },
      ));
    }
    assert!(bridge.has_session("dev-guard"));
    {
      let _guard = StartingGuard::arm(&bridge, "dev-guard", 3);
      // drop armed → clear Starting
    }
    assert!(!bridge.has_session("dev-guard"), "armed StartingGuard drop must clear Starting");
    assert!(matches!(
      bridge.devices.lock().get("dev-guard").map(|s| &s.state),
      Some(DeviceState::Idle)
    ));
  }

  #[test]
  fn starting_guard_disarm_preserves_starting() {
    let bridge = Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None)));
    {
      let mut devices = bridge.devices.lock();
      drop(devices.insert(
        "dev-guard-disarm".to_owned(),
        DeviceSlot {
          generation: 1,
          state: DeviceState::Starting { generation: 1 },
        },
      ));
    }
    {
      let mut guard = StartingGuard::arm(&bridge, "dev-guard-disarm", 1);
      guard.disarm();
    }
    assert!(
      bridge.has_session("dev-guard-disarm"),
      "disarmed StartingGuard must not clear Starting"
    );
    assert!(matches!(
      bridge.devices.lock().get("dev-guard-disarm").map(|s| &s.state),
      Some(DeviceState::Starting { generation: 1 })
    ));
  }
}
