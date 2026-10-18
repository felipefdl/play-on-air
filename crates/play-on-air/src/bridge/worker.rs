//! Event routing: one sequential worker per device so Cast I/O never blocks another device.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use crate::airplay::AirPlaySessionEvent;

use super::{Bridge, RingLookup, SHUTDOWN_TEARDOWN, SHUTDOWN_WORKER_JOIN};

/// How long the main `run` loop waits for per-device workers after the event channel closes.
const WORKER_JOIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Tracks per-device session workers; aborts all on drop so `Bridge::run` abort cannot leave zombies.
struct DeviceWorkerSet {
  senders: HashMap<String, mpsc::UnboundedSender<AirPlaySessionEvent>>,
  set: JoinSet<()>,
}

impl DeviceWorkerSet {
  fn new() -> Self {
    Self {
      senders: HashMap::new(),
      set: JoinSet::new(),
    }
  }

  fn spawn_worker(
    &mut self,
    bridge: Arc<Bridge>,
    rings: Arc<dyn RingLookup>,
    device_id: String,
  ) -> mpsc::UnboundedSender<AirPlaySessionEvent> {
    let (tx, rx) = mpsc::unbounded_channel();
    let worker_id = device_id.clone();
    let _abort = self.set.spawn(async move {
      device_worker_loop(bridge, rings, worker_id, rx).await;
    });
    let sender = tx.clone();
    drop(self.senders.insert(device_id, tx));
    sender
  }

  /// Drop inboxes, wait up to `budget` for workers to finish, then abort any still running.
  async fn shutdown_graceful(&mut self, budget: Duration) {
    self.senders.clear();
    let deadline = tokio::time::Instant::now() + budget;
    while !self.set.is_empty() {
      let now = tokio::time::Instant::now();
      if now >= deadline {
        tracing::warn!("device session workers join timed out; aborting remaining");
        self.set.abort_all();
        break;
      }
      let remaining = deadline - now;
      match tokio::time::timeout(remaining, self.set.join_next()).await {
        Ok(Some(Ok(()))) => {},
        Ok(Some(Err(err))) => {
          if !err.is_cancelled() {
            tracing::warn!(error = %err, "device session worker panicked");
          }
        },
        Ok(None) => break,
        Err(_) => {
          tracing::warn!("device session workers join timed out; aborting remaining");
          self.set.abort_all();
          break;
        },
      }
    }
    // Drain aborted / finished tasks so JoinSet drops cleanly.
    while self.set.join_next().await.is_some() {}
  }
}

impl Drop for DeviceWorkerSet {
  fn drop(&mut self) {
    self.senders.clear();
    self.set.abort_all();
  }
}

/// Resolves once [`Bridge::request_shutdown`] ran (never if the bridge's sender is gone).
async fn shutdown_requested(stop: &mut watch::Receiver<bool>) {
  if stop.wait_for(|requested| *requested).await.is_err() {
    std::future::pending::<()>().await;
  }
}

const fn event_device_id(event: &AirPlaySessionEvent) -> &str {
  match event {
    AirPlaySessionEvent::Started { device_id, .. }
    | AirPlaySessionEvent::Ended { device_id }
    | AirPlaySessionEvent::Paused { device_id, .. }
    | AirPlaySessionEvent::Resumed { device_id }
    | AirPlaySessionEvent::Flushed { device_id }
    | AirPlaySessionEvent::Volume { device_id, .. }
    | AirPlaySessionEvent::NowPlaying { device_id, .. } => device_id.as_str(),
  }
}

async fn device_worker_loop(
  bridge: Arc<Bridge>,
  rings: Arc<dyn RingLookup>,
  device_id: String,
  mut events: mpsc::UnboundedReceiver<AirPlaySessionEvent>,
) {
  let mut stop = bridge.shutdown.subscribe();
  loop {
    // Shutdown wins over queued events: nothing new starts once the bridge is stopping.
    let event = tokio::select! {
      biased;
      () = shutdown_requested(&mut stop) => break,
      next = events.recv() => match next {
        Some(event) => event,
        None => break,
      },
    };
    match event {
      AirPlaySessionEvent::Started {
        device_id: event_device,
        sample_rate,
        ring,
        group,
      } => {
        // Prebuffer + LOAD can take seconds; shutdown abandons the start (StartingGuard
        // clears `Starting`, the unpublished media server drops) instead of waiting on it.
        tokio::select! {
          biased;
          () = shutdown_requested(&mut stop) => break,
          result = bridge.handle_session_start(&event_device, sample_rate, ring, group, Arc::clone(&rings)) => {
            if let Err(err) = result {
              tracing::error!(device_id = %event_device, error = %err, "failed to start Cast bridge session");
            }
          },
        }
        // A group owner whose start failed hands the group to a member.
        bridge.group_check_owner(&event_device);
      },
      AirPlaySessionEvent::Ended { device_id: event_device } => {
        bridge.handle_ended(&event_device, rings.as_ref()).await;
      },
      // Transport and metadata of a group follower belong to the group owner's session: every
      // member gets the same sender command, and only the owner turns it into a Cast command.
      AirPlaySessionEvent::Paused { device_id: event_device, .. }
      | AirPlaySessionEvent::Resumed { device_id: event_device }
      | AirPlaySessionEvent::Flushed { device_id: event_device }
        if bridge.is_following(&event_device) =>
      {
        tracing::debug!(device_id = %event_device, "group follower transport event; the group owner acts on Cast");
      },
      AirPlaySessionEvent::Paused { device_id: event_device, at } => {
        bridge.handle_pause(&event_device, at);
      },
      AirPlaySessionEvent::Resumed { device_id: event_device } => {
        bridge.handle_resume(&event_device);
      },
      AirPlaySessionEvent::Flushed { device_id: event_device } => {
        bridge.handle_flush(&event_device);
      },
      AirPlaySessionEvent::Volume { device_id: event_device, volume_db } => {
        bridge.handle_volume(&event_device, volume_db).await;
      },
      AirPlaySessionEvent::NowPlaying { device_id: event_device, now_playing } => {
        bridge.handle_now_playing(&event_device, &now_playing);
      },
    }
  }
  tracing::debug!(%device_id, "device session worker exited");
}

impl Bridge {
  /// Run until the session event channel closes or [`Bridge::request_shutdown`] is called.
  ///
  /// Events are routed to a small per-`device_id` worker so one device's slow
  /// Cast LOAD cannot head-of-line-block another device. Ordering within a
  /// device (Started / Ended / Volume) remains sequential.
  ///
  /// On exit, intake closes, workers are joined (bounded, then aborted), every live session
  /// is torn down (media, then best-effort Cast STOP), and detached teardowns are joined.
  /// Workers are tracked in a [`JoinSet`] and aborted when `run` is dropped/aborted, so
  /// nested tasks cannot outlive the bridge task as zombies.
  pub async fn run(
    self: Arc<Self>,
    mut events: mpsc::UnboundedReceiver<AirPlaySessionEvent>,
    rings: Arc<dyn RingLookup>,
  ) {
    let mut workers = DeviceWorkerSet::new();
    let mut stop = self.shutdown.subscribe();
    let taken = self.redispatch_rx.lock().take();
    let Some(mut redispatch) = taken else {
      tracing::error!("bridge run called twice; refusing second run");
      return;
    };

    loop {
      let event = tokio::select! {
        biased;
        () = shutdown_requested(&mut stop) => break,
        Some(event) = redispatch.recv() => event,
        next = events.recv() => match next {
          Some(event) => event,
          None => break,
        },
      };
      let device_id = event_device_id(&event).to_owned();
      if !workers.senders.contains_key(&device_id) {
        let _tx = workers.spawn_worker(Arc::clone(&self), Arc::clone(&rings), device_id.clone());
      }
      let Some(sender) = workers.senders.get(&device_id).cloned() else {
        tracing::error!(%device_id, "device session worker missing after spawn");
        continue;
      };
      match sender.send(event) {
        Ok(()) => {},
        Err(mpsc::error::SendError(failed_event)) => {
          // Dead worker: recreate and retry the same event (do not discard).
          tracing::warn!(%device_id, "device session worker dropped; recreating and retrying event");
          drop(workers.senders.remove(&device_id));
          let tx = workers.spawn_worker(Arc::clone(&self), Arc::clone(&rings), device_id.clone());
          if tx.send(failed_event).is_err() {
            tracing::error!(%device_id, "device session worker failed to accept retried event");
          }
        },
      }
    }

    // Refuse further AirPlay events; queued ones are dropped with the receiver.
    events.close();
    drop(events);
    let shutting_down = *stop.borrow();
    let worker_budget = if shutting_down {
      SHUTDOWN_WORKER_JOIN
    } else {
      WORKER_JOIN_TIMEOUT
    };
    tracing::debug!(shutting_down, "bridge intake closed; draining device workers");
    workers.shutdown_graceful(worker_budget).await;
    self.end_all_sessions(SHUTDOWN_TEARDOWN).await;
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::time::Instant;

  use tokio::time::sleep;

  use crate::audio::PcmRing;
  use crate::bridge::SHUTDOWN_TIMEOUT;
  use crate::bridge::start::PREBUFFER_FRAMES;
  use crate::bridge::test_support::{
    FixedRingLookup, MapRingLookup, http_get_status_ok, insert_playing, test_playing_session,
  };
  use crate::cast::CastPool;
  use crate::media::MediaServer;
  use crate::registry::{Device, DeviceRegistry};

  #[tokio::test]
  async fn multi_device_session_starts_do_not_serialize() {
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let registry = Arc::new(DeviceRegistry::new());
    for id in ["dev-a", "dev-b"] {
      let _appeared = registry.appear(Device {
        id: id.to_owned(),
        name: id.to_owned(),
        model: String::new(),
        host: "127.0.0.1".to_owned(),
        hostname: format!("{id}.local"),
        port: 9,
        last_seen: Instant::now(),
        instance: id.to_owned(),
        pending_leave_deadline: None,
        pending_leave_since: None,
      });
    }

    let bridge = Arc::new(Bridge::new(registry, Arc::new(CastPool::new(None))));
    bridge.set_start_barrier(Arc::clone(&barrier));

    let mut ring_map = HashMap::new();
    let mut samples = Vec::with_capacity(PREBUFFER_FRAMES * 2);
    for _ in 0..PREBUFFER_FRAMES {
      samples.push(0.01);
      samples.push(0.01);
    }
    for id in ["dev-a", "dev-b"] {
      let ring = Arc::new(PcmRing::new(2, PREBUFFER_FRAMES * 2));
      ring.push_f32(&samples);
      drop(ring_map.insert(id.to_owned(), ring));
    }
    let rings: Arc<dyn RingLookup> = Arc::new(MapRingLookup { rings: ring_map.clone() });

    let (tx, rx) = mpsc::unbounded_channel();
    let run = tokio::spawn({
      let bridge_for_run = Arc::clone(&bridge);
      async move {
        bridge_for_run.run(rx, rings).await;
      }
    });

    let ring_a = Arc::clone(ring_map.get("dev-a").expect("dev-a"));
    let ring_b = Arc::clone(ring_map.get("dev-b").expect("dev-b"));
    tx.send(AirPlaySessionEvent::Started {
      device_id: "dev-a".to_owned(),
      sample_rate: 48_000,
      ring: ring_a,
      group: None,
    })
    .expect("send a");
    tx.send(AirPlaySessionEvent::Started {
      device_id: "dev-b".to_owned(),
      sample_rate: 48_000,
      ring: ring_b,
      group: None,
    })
    .expect("send b");

    let concurrent = tokio::time::timeout(Duration::from_secs(3), barrier.wait()).await;
    assert!(
      concurrent.is_ok(),
      "both device session starts must enter handle_session_start concurrently (no cross-device HOL)"
    );

    drop(tx);
    let joined = tokio::time::timeout(Duration::from_secs(10), run).await;
    assert!(joined.is_ok(), "bridge run must exit after event channel close");
  }

  #[tokio::test]
  async fn aborting_bridge_run_aborts_device_workers() {
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None))));
    let rings: Arc<dyn RingLookup> = Arc::new(FixedRingLookup { current: None });
    let (tx, rx) = mpsc::unbounded_channel();
    let run = tokio::spawn({
      let bridge_for_run = Arc::clone(&bridge);
      async move {
        bridge_for_run.run(rx, rings).await;
      }
    });

    tx.send(AirPlaySessionEvent::Ended { device_id: "dev-zombie".to_owned() })
      .expect("send");
    sleep(Duration::from_millis(50)).await;

    run.abort();
    let result = tokio::time::timeout(Duration::from_secs(2), run).await;
    assert!(result.is_ok(), "aborted bridge run must finish promptly");
    drop(tx);
  }

  /// Shutdown must not wait for every event sender to drop (AirPlay keeps one for the process
  /// lifetime): it closes intake, abandons an in-flight start, and ends live sessions.
  #[tokio::test]
  async fn request_shutdown_ends_live_sessions_while_senders_alive() {
    let registry = Arc::new(DeviceRegistry::new());
    let _appeared = registry.appear(Device {
      id: "dev-starting".to_owned(),
      name: "Starting".to_owned(),
      model: String::new(),
      host: "127.0.0.1".to_owned(),
      hostname: "starting.local".to_owned(),
      port: 9,
      last_seen: Instant::now(),
      instance: "dev-starting".to_owned(),
      pending_leave_deadline: None,
      pending_leave_since: None,
    });
    let bridge = Arc::new(Bridge::new(registry, Arc::new(CastPool::new(None))));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let health_url = format!("{}/health", media.base_url);
    insert_playing(
      &bridge,
      "dev-playing",
      1,
      test_playing_session(
        media,
        "dev-playing",
        Arc::clone(&bridge.cast_pool),
        Arc::new(PcmRing::new(2, 64)),
      ),
    );

    // Empty ring: the start sits in the prebuffer poll when shutdown arrives.
    let starting_ring = Arc::new(PcmRing::new(2, PREBUFFER_FRAMES * 2));
    let rings: Arc<dyn RingLookup> = Arc::new(FixedRingLookup {
      current: Some(Arc::clone(&starting_ring)),
    });
    let (tx, rx) = mpsc::unbounded_channel();
    let run = tokio::spawn({
      let bridge_for_run = Arc::clone(&bridge);
      async move {
        bridge_for_run.run(rx, rings).await;
      }
    });
    tx.send(AirPlaySessionEvent::Started {
      device_id: "dev-starting".to_owned(),
      sample_rate: 48_000,
      ring: starting_ring,
      group: None,
    })
    .expect("send start");
    let wait_start = Instant::now();
    while !bridge.has_session("dev-starting") && wait_start.elapsed() < Duration::from_secs(2) {
      sleep(Duration::from_millis(5)).await;
    }
    assert!(bridge.has_session("dev-starting"), "start must be in flight");

    bridge.request_shutdown();
    let joined = tokio::time::timeout(SHUTDOWN_TIMEOUT, run).await;
    assert!(joined.is_ok(), "run must return after request_shutdown while senders are alive");
    assert!(!bridge.has_session("dev-playing"), "live session ended on shutdown");
    assert!(!bridge.has_session("dev-starting"), "in-flight start abandoned on shutdown");
    assert!(!http_get_status_ok(&health_url).await, "shutdown must stop live media");
    assert!(tx.is_closed(), "event intake closed after shutdown");
  }
}
