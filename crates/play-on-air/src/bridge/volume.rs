//! AirPlay volume to Cast.
//!
//! A speaker's level goes to its own receiver (`SET_VOLUME`), also while it plays in a Cast
//! multizone group: each member keeps its own volume.
//!
//! A Google Home speaker group's receiver (`CAST_GROUP_MODEL`) has a master volume that scales
//! its member speakers only while the group endpoint hosts the media session. Once the session
//! plays elsewhere (the group follows an AirPlay multi-select, or Cast moved the session to a
//! multizone host), setting it changes nothing audible. The level then goes to the group's
//! member speakers: each member's level is scaled so their mean, which Cast reports as the group
//! level, becomes the target, keeping their balance. Members come from `MULTIZONE_STATUS` on the
//! group's own endpoint (read again after [`MEMBER_CACHE_TTL`]); each is set through its own
//! warm worker, which attributes the resulting volume status to us, so nothing of it is
//! reported back to the sender as a user change. A slider drag sends about ten levels a second:
//! one task per group applies the latest target, then the next one, never a backlog.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinSet;

use crate::airplay::airplay_db_to_cast_linear;
use crate::cast::{normalize_device_id, volume_level_clamped};
use crate::registry::{CAST_GROUP_MODEL, Device};

use super::Bridge;
use super::group::{CastGroup, session_host_key_at};
use super::state::DeviceState;

/// How long a speaker group's members (and the levels their balance is kept from) are reused.
const MEMBER_CACHE_TTL: Duration = Duration::from_secs(5);

/// A member mean at or below this counts as silent: every member gets the target.
const SILENT_MEAN: f32 = 0.001;

/// Where an AirPlay receiver's volume goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VolumeRoute {
  /// No live session for the receiver.
  Skip,
  /// The receiver's own Cast endpoint (`SET_VOLUME`).
  Own,
  /// A speaker group whose session plays elsewhere: its member speakers.
  Members,
}

/// A speaker group's member, as its registry id (pool worker) and its level when read.
#[derive(Debug, Clone)]
struct GroupMember {
  device_id: String,
  level: f32,
}

/// Coalesced member volume of one speaker-group receiver.
#[derive(Debug, Default)]
pub(super) struct GroupVolume {
  /// Latest target not applied yet.
  target: Option<f32>,
  /// An apply task runs for this group.
  running: bool,
  /// Members and the levels their balance is kept from, with when they were read.
  members: Option<(Instant, Vec<GroupMember>)>,
}

/// Member levels whose mean is `target`, keeping their balance.
///
/// Each level is scaled by `target / mean` and clamped to `0.0..=1.0`; members that are all
/// (about) silent get `target` each.
fn scaled_member_levels(levels: &[f32], target: f32) -> Vec<f32> {
  if levels.is_empty() {
    return Vec::new();
  }
  let clamped = volume_level_clamped(target);
  let (sum, count) = levels
    .iter()
    .fold((0.0_f32, 0.0_f32), |(sum, count), level| (sum + level, count + 1.0));
  let mean = sum / count;
  if mean <= SILENT_MEAN {
    return vec![clamped; levels.len()];
  }
  let scale = clamped / mean;
  levels.iter().map(|level| volume_level_clamped(level * scale)).collect()
}

/// Whether a session controlled through pool worker `media_key` plays on `group`'s own endpoint.
fn hosted_on(media_key: &str, group: &Device) -> bool {
  media_key == group.id || media_key == session_host_key_at(&group.host, group.port)
}

impl Bridge {
  /// AirPlay volume for `device_id` while it plays (alone, as group owner, or as follower).
  pub(super) async fn handle_volume(self: &Arc<Self>, device_id: &str, volume_db: f32) {
    let level = airplay_db_to_cast_linear(volume_db);
    match self.volume_route(device_id, level) {
      VolumeRoute::Skip => {},
      VolumeRoute::Own => self.set_own_volume(device_id, level).await,
      VolumeRoute::Members => self.queue_group_volume(device_id, level),
    }
  }

  /// Where `device_id`'s volume goes; records `level` on its Playing session (re-LOADs reapply it).
  fn volume_route(&self, device_id: &str, level: f32) -> VolumeRoute {
    let speaker_group = self.registry.get(device_id).filter(|device| device.model == CAST_GROUP_MODEL);
    let devices = self.devices.lock();
    let media_key = match devices.get(device_id).map(|slot| &slot.state) {
      Some(DeviceState::Playing { session, .. }) => {
        *session.last_volume_linear.lock() = Some(level);
        Some(session.route.media())
      },
      Some(DeviceState::Following { follower, .. }) => {
        self.groups.lock().get(&follower.group).and_then(CastGroup::media_key)
      },
      Some(DeviceState::Idle | DeviceState::Starting { .. }) | None => return VolumeRoute::Skip,
    };
    drop(devices);
    match speaker_group {
      Some(group) if !media_key.is_some_and(|key| hosted_on(&key, &group)) => VolumeRoute::Members,
      Some(_) | None => VolumeRoute::Own,
    }
  }

  /// `SET_VOLUME` on `device_id`'s own endpoint.
  ///
  /// Awaited, so volume stays ordered after prior events on this device worker without
  /// blocking other devices; the Cast worker coalesces queued levels.
  async fn set_own_volume(&self, device_id: &str, level: f32) {
    let pool = Arc::clone(&self.cast_pool);
    let id = device_id.to_owned();
    match tokio::task::spawn_blocking(move || pool.set_volume(&id, level)).await {
      Ok(Ok(())) => {},
      Ok(Err(err)) => {
        tracing::debug!(device_id = %device_id, error = %err, "Cast volume sync failed");
      },
      Err(err) => {
        tracing::debug!(device_id = %device_id, error = %err, "Cast volume task join failed");
      },
    }
  }

  /// Make `level` the next target of speaker group `group_id`; start its apply task if idle.
  fn queue_group_volume(self: &Arc<Self>, group_id: &str, level: f32) {
    let start = {
      let mut volumes = self.group_volumes.lock();
      let entry = volumes.entry(group_id.to_owned()).or_default();
      entry.target = Some(level);
      let idle = !std::mem::replace(&mut entry.running, true);
      drop(volumes);
      idle
    };
    if start {
      let bridge = Arc::clone(self);
      let id = group_id.to_owned();
      self.spawn_detached(async move { bridge.apply_group_volume(&id).await });
    }
  }

  /// Apply speaker group `group_id`'s latest target to its members until none is pending.
  async fn apply_group_volume(&self, group_id: &str) {
    loop {
      let next = {
        let mut volumes = self.group_volumes.lock();
        let next = volumes.get_mut(group_id).and_then(|entry| {
          let Some(target) = entry.target.take() else {
            entry.running = false;
            return None;
          };
          let cached = entry
            .members
            .as_ref()
            .filter(|(read_at, _)| read_at.elapsed() < MEMBER_CACHE_TTL)
            .map(|(_, members)| members.clone());
          Some((target, cached))
        });
        drop(volumes);
        next
      };
      let Some((target, cached)) = next else {
        return;
      };
      let members = if let Some(members) = cached {
        members
      } else {
        let Some(members) = self.read_group_members(group_id).await else {
          continue;
        };
        if let Some(entry) = self.group_volumes.lock().get_mut(group_id) {
          entry.members = Some((Instant::now(), members.clone()));
        }
        members
      };
      self.set_member_levels(group_id, &members, target).await;
    }
  }

  /// Members of speaker group `group_id` from its endpoint's `MULTIZONE_STATUS`, as registry
  /// speakers (others are skipped).
  async fn read_group_members(&self, group_id: &str) -> Option<Vec<GroupMember>> {
    let pool = Arc::clone(&self.cast_pool);
    let key = group_id.to_owned();
    let status = match tokio::task::spawn_blocking(move || pool.multizone_members(&key)).await {
      Ok(Ok(members)) => members,
      Ok(Err(err)) => {
        tracing::debug!(group_id, error = %err, "speaker group members not read; volume not applied");
        return None;
      },
      Err(err) => {
        tracing::debug!(group_id, error = %err, "speaker group members task join failed");
        return None;
      },
    };
    let speakers = self.registry.list();
    let members = status
      .into_iter()
      .filter_map(|member| {
        let found = speakers
          .iter()
          .find(|speaker| normalize_device_id(&speaker.id) == member.device_id);
        if found.is_none() {
          tracing::debug!(group_id, member = %member.device_id, "speaker group member not discovered; skipped");
        }
        found.map(|speaker| GroupMember {
          device_id: speaker.id.clone(),
          level: member.level,
        })
      })
      .collect();
    Some(members)
  }

  /// Set each member of `group_id` so their mean becomes `target`, all members at once.
  async fn set_member_levels(&self, group_id: &str, members: &[GroupMember], target: f32) {
    let levels: Vec<f32> = members.iter().map(|member| member.level).collect();
    let scaled = scaled_member_levels(&levels, target);
    tracing::debug!(group_id, target, ?members, ?scaled, "speaker group volume set on its members");
    let mut sets = JoinSet::new();
    for (member, level) in members.iter().zip(scaled) {
      let pool = Arc::clone(&self.cast_pool);
      let key = member.device_id.clone();
      let _abort = sets.spawn_blocking(move || {
        let result = pool.set_volume(&key, level);
        (key, result)
      });
    }
    while let Some(joined) = sets.join_next().await {
      match joined {
        Ok((_, Ok(()))) => {},
        Ok((member, Err(err))) => {
          tracing::debug!(group_id, %member, error = %err, "speaker group member volume failed");
        },
        Err(err) => tracing::debug!(group_id, error = %err, "speaker group member volume task join failed"),
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  use parking_lot::Mutex;

  use crate::audio::PcmRing;
  use crate::bridge::load::{LoadVolumePolicy, volume_after_load};
  use crate::bridge::test_support::{CastCall, FakeCast, insert_playing, test_device, test_playing_session};
  use crate::cast::{CastControl, MultizoneMember};
  use crate::media::MediaServer;
  use crate::registry::DeviceRegistry;

  const GROUP: &str = "13777DD6-C2C9-449C-8D93-5F8FA75EAEF0";
  const GYM: &str = "56cb7fe7e13f325625325c4a304c57fa";
  const POOL: &str = "d417a9b9bf1ebe211639512dad18be1d";
  const SPEAKER: &str = "aaaa0000aaaa0000aaaa0000aaaa0000";
  const HOST_KEY: &str = "cast-session:127.0.0.1:32230";

  fn assert_levels(actual: &[f32], expected: &[f32]) {
    assert_eq!(actual.len(), expected.len(), "{actual:?} vs {expected:?}");
    for (a, e) in actual.iter().zip(expected) {
      assert!((a - e).abs() < 1e-5, "{actual:?} vs {expected:?}");
    }
  }

  fn mean(levels: &[f32]) -> f32 {
    levels.iter().sum::<f32>() / 2.0
  }

  #[test]
  fn scaling_reaches_the_target_mean_and_keeps_balance() {
    let levels = [0.616, 0.766];
    let scaled = scaled_member_levels(&levels, 0.398);
    assert!((mean(&scaled) - 0.398).abs() < 1e-5, "{scaled:?}");
    let (before, after) = (levels[1] / levels[0], scaled[1] / scaled[0]);
    assert!((before - after).abs() < 1e-5, "balance kept: {before} vs {after}");
    assert_levels(&scaled_member_levels(&[0.2, 0.6], 0.8), &[0.4, 1.0]);
  }

  #[test]
  fn scaling_clamps_to_the_valid_range() {
    assert_levels(&scaled_member_levels(&[0.4, 0.8], 0.9), &[0.6, 1.0]);
    assert_levels(&scaled_member_levels(&[0.4, 0.8], 1.5), &[0.666_666_7, 1.0]);
    assert_levels(&scaled_member_levels(&[0.4, 0.8], -0.2), &[0.0, 0.0]);
  }

  #[test]
  fn silent_members_all_get_the_target() {
    assert_levels(&scaled_member_levels(&[0.0, 0.0], 0.5), &[0.5, 0.5]);
    assert_levels(&scaled_member_levels(&[0.0, 0.0005], 0.3), &[0.3, 0.3]);
    assert_eq!(scaled_member_levels(&[], 0.5), Vec::<f32>::new());
  }

  /// Bridge on a recording [`FakeCast`]; the registry knows the "Gym and Pool" group, its two
  /// members, and a lone speaker. The group endpoint lists Gym at 0.2 and Pool at 0.6.
  fn group_bridge() -> (Arc<Bridge>, Arc<FakeCast>) {
    let registry = Arc::new(DeviceRegistry::new());
    let mut group = test_device(GROUP, "Gym and Pool").with_model(CAST_GROUP_MODEL);
    group.port = 32_128;
    for device in [
      group,
      test_device(GYM, "Gym speaker"),
      test_device(POOL, "Pool Bathroom speaker"),
      test_device(SPEAKER, "Kitchen speaker"),
    ] {
      let _new = registry.appear(device);
    }
    let fake = Arc::new(FakeCast::default());
    fake.set_members(
      GROUP,
      vec![
        MultizoneMember {
          device_id: "56cb7fe7e13f325625325c4a304c57fa".to_owned(),
          level: 0.2,
        },
        MultizoneMember {
          device_id: "d417a9b9bf1ebe211639512dad18be1d".to_owned(),
          level: 0.6,
        },
        MultizoneMember {
          device_id: "eeee0000eeee0000eeee0000eeee0000".to_owned(),
          level: 0.9,
        },
      ],
    );
    let pool: Arc<dyn CastControl> = Arc::<FakeCast>::clone(&fake);
    (Arc::new(Bridge::new(registry, pool)), fake)
  }

  /// Playing session for `device_id` whose Cast media is controlled through `media_key`.
  async fn play(bridge: &Arc<Bridge>, device_id: &str, media_key: &str) {
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let session = test_playing_session(media, device_id, Arc::clone(&bridge.cast_pool), Arc::new(PcmRing::new(2, 64)));
    session.route.set_media(media_key);
    insert_playing(bridge, device_id, 1, session);
  }

  async fn wait_for_volumes(fake: &FakeCast, count: usize) -> Vec<(String, f32)> {
    let started = Instant::now();
    loop {
      let volumes = fake.volumes();
      if volumes.len() >= count {
        return volumes;
      }
      assert!(started.elapsed() < Duration::from_secs(5), "volumes: {volumes:?}");
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  }

  #[tokio::test]
  async fn group_hosting_its_own_session_sets_its_endpoint() {
    let (bridge, fake) = group_bridge();
    play(&bridge, GROUP, GROUP).await;
    bridge.handle_volume(GROUP, 0.0).await;
    assert_eq!(fake.calls(), vec![CastCall::SetVolume(GROUP.to_owned())]);
    bridge.handle_session_end(GROUP).await;

    // A session joined on the group's own endpoint by its multizone host key is still its own.
    play(&bridge, GROUP, "cast-session:127.0.0.1:32128").await;
    bridge.handle_volume(GROUP, 0.0).await;
    assert_eq!(fake.volumes(), vec![(GROUP.to_owned(), 1.0), (GROUP.to_owned(), 1.0)]);
    bridge.handle_session_end(GROUP).await;
  }

  #[tokio::test]
  async fn group_on_a_moved_session_scales_its_members() {
    let (bridge, fake) = group_bridge();
    play(&bridge, GROUP, HOST_KEY).await;

    // Target mean from Gym at 0.2 and Pool at 0.6; the third member is not discovered.
    bridge.handle_volume(GROUP, -6.0).await;
    let mut volumes = wait_for_volumes(&fake, 2).await;
    volumes.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(volumes.iter().map(|(key, _)| key.as_str()).collect::<Vec<_>>(), vec![GYM, POOL]);
    let levels: Vec<f32> = volumes.iter().map(|(_, level)| *level).collect();
    let target = airplay_db_to_cast_linear(-6.0);
    assert_levels(&levels, &scaled_member_levels(&[0.2, 0.6], target));
    assert_eq!(
      fake
        .calls()
        .iter()
        .filter(|call| **call == CastCall::SetVolume(GROUP.to_owned()))
        .count(),
      0,
      "the group's master volume is not touched"
    );
    bridge.handle_session_end(GROUP).await;
  }

  #[tokio::test]
  async fn group_volume_bursts_apply_the_latest_target_with_one_member_read() {
    let (bridge, fake) = group_bridge();
    play(&bridge, GROUP, HOST_KEY).await;

    for step in 0_u8..20 {
      bridge.handle_volume(GROUP, -30.0 + f32::from(step)).await;
    }
    let last = airplay_db_to_cast_linear(-11.0);
    let expected = scaled_member_levels(&[0.2, 0.6], last);
    let started = Instant::now();
    loop {
      let volumes = fake.volumes();
      let latest = |key: &str| volumes.iter().rev().find(|(k, _)| k == key).map(|(_, level)| *level);
      if let (Some(gym), Some(pool)) = (latest(GYM), latest(POOL))
        && (gym - expected[0]).abs() < 1e-5
        && (pool - expected[1]).abs() < 1e-5
      {
        break;
      }
      assert!(started.elapsed() < Duration::from_secs(5), "volumes: {volumes:?}");
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let calls = fake.calls();
    let reads = calls.iter().filter(|call| matches!(call, CastCall::MultizoneStatus(_))).count();
    assert_eq!(reads, 1, "members cached across the burst: {calls:?}");
    assert!(fake.volumes().len() < 40, "burst coalesced: {:?}", fake.volumes());
    assert!(!bridge.group_volumes.lock().get(GROUP).expect("entry").running);
    bridge.handle_session_end(GROUP).await;
  }

  #[tokio::test]
  async fn speaker_on_a_moved_session_sets_its_own_endpoint() {
    let (bridge, fake) = group_bridge();
    play(&bridge, SPEAKER, HOST_KEY).await;
    bridge.handle_volume(SPEAKER, 0.0).await;
    assert_eq!(fake.calls(), vec![CastCall::SetVolume(SPEAKER.to_owned())]);
    bridge.handle_session_end(SPEAKER).await;
  }

  #[tokio::test]
  async fn volume_without_a_session_is_dropped() {
    let (bridge, fake) = group_bridge();
    bridge.handle_volume(GROUP, 0.0).await;
    bridge.handle_volume(SPEAKER, 0.0).await;
    assert_eq!(fake.calls(), Vec::new());
  }

  #[tokio::test]
  async fn handle_volume_updates_last_volume_on_active_session() {
    let (bridge, _fake) = group_bridge();
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let last_volume_linear = Arc::new(Mutex::new(None));
    let mut session =
      test_playing_session(media, SPEAKER, Arc::clone(&bridge.cast_pool), Arc::new(PcmRing::new(2, 64)));
    session.last_volume_linear = Arc::clone(&last_volume_linear);
    insert_playing(&bridge, SPEAKER, 1, session);

    bridge.handle_volume(SPEAKER, 0.0).await;
    let stored = *last_volume_linear.lock();
    assert_eq!(stored, Some(1.0));
    assert_eq!(volume_after_load(LoadVolumePolicy::PreserveDevice), None);
    assert_eq!(volume_after_load(LoadVolumePolicy::Rollover { last_volume: stored }), Some(1.0));

    bridge.handle_session_end(SPEAKER).await;
  }
}
