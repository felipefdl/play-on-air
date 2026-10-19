//! AirPlay multi-select played as one Google Cast multizone session.
//!
//! When the sender selects several PlayOnAir speakers together, every receiver gets its own
//! AirPlay session carrying the same `groupUUID`. Feeding each Chromecast its own stream lets
//! them drift; Google multizone keeps speakers in sync when one playing Cast session is
//! expanded with `SET_PLAYBACK_DEVICES` (`docs/cast-multizone-spec.md`). So:
//!
//! - The first member to start is the group **owner**: a normal LOAD on its speaker.
//! - Every other member **follows**: its AirPlay session stays healthy (audio accepted and
//!   drained, RTSP answered) and nothing of it reaches Cast; the owner's session is expanded to
//!   its speaker instead. Transport commands (pause, resume, flush) arrive on every member and
//!   only the owner turns them into Cast commands. Volume stays per receiver (`volume`): a
//!   speaker's own `SET_VOLUME`, a Google Home speaker group's member speakers.
//! - Membership changes are reconciled: the full desired speaker list goes to the session host
//!   (the owner's speaker first, the multizone group endpoint once the session moved there), one
//!   request at a time, debounced so a burst of joins or leaves becomes one request.
//! - A member Cast cannot add (`failedDevices`, or the request refused) plays standalone.
//! - The owner leaving while joined members remain hands the Cast session over instead of
//!   ending it: the session (media server, Cast session, re-LOAD state) moves to a member, whose
//!   AirPlay ring now feeds the media ring. Members of one multi-select receive the same audio on
//!   the sender's PTP timeline, so the stream continues without a re-LOAD; the old owner's
//!   speaker is then dropped from the list. Ending and re-LOADing on a member instead would gap
//!   every speaker and re-form the group.
//! - The last member leaving ends the Cast session normally (STOP on the session host).
//! - When Cast moves the session (`sessionEndpoint`), media control joins it on the new host by
//!   session id. If that fails, control never stays on the old endpoint: the owner re-LOADs a
//!   fresh session on its own speaker and the remaining members are added back.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::airplay::AirPlaySessionEvent;
use crate::audio::PcmRing;
use crate::cast::{PlaybackDevice, SessionEndpoint, SetPlaybackDevices, normalize_device_id};
use crate::error::Error;

use super::Bridge;
use super::load::ReloadOutcome;
use super::stall::spawn_stall_watchdog;
use super::start::accept_start_in;
use super::state::{CastRoute, DeviceState, FollowingSession, PlayingSession, RingPump};

/// Membership changes within this window go to Cast as one `SET_PLAYBACK_DEVICES`.
const GROUP_RECONCILE_DEBOUNCE: Duration = Duration::from_millis(300);

/// One AirPlay multi-select group (keyed by the sender's `groupUUID`).
#[derive(Debug, Default)]
pub(super) struct CastGroup {
  /// Member whose `Starting` / `Playing` session feeds Cast.
  owner: Option<String>,
  /// Members that follow the owner's Cast session, in join order.
  followers: Vec<String>,
  /// Speakers Cast confirmed in the session after the last answer (session order).
  cast_members: Vec<String>,
  /// Pool worker of the multizone session host, once the Cast session moved there.
  host_key: Option<String>,
  /// Members Cast could not add: they play standalone until their AirPlay session ends.
  excluded: HashSet<String>,
  /// Follower sent a `Started` to take over an ownerless group (not yet started).
  promoting: Option<String>,
  /// A reconcile task runs for this group.
  reconcile_running: bool,
  /// Membership changed while the reconcile task was busy.
  reconcile_dirty: bool,
}

impl CastGroup {
  fn is_empty(&self) -> bool {
    self.owner.is_none() && self.followers.is_empty() && self.excluded.is_empty()
  }

  /// Pool worker hosting the group's Cast media session: the multizone session host once the
  /// session moved there, else the owner's speaker.
  pub(super) fn media_key(&self) -> Option<String> {
    self.host_key.clone().or_else(|| self.owner.clone())
  }
}

/// How a member's AirPlay session joins its group (decided and installed atomically).
pub(super) enum GroupEntry {
  /// This member owns the group's Cast session: `Starting` installed, LOAD on `media_key`
  /// (the session host when the owner restarts an expanded session) or its own speaker.
  Owner {
    generation: u64,
    prior: Option<Box<PlayingSession>>,
    media_key: Option<String>,
  },
  /// Another member owns the Cast session; `Following` installed. `prior` is this device's
  /// own previous session, which must end.
  Following { prior: Option<Box<PlayingSession>> },
  /// Cast could not add this member earlier; it plays on its own.
  Standalone,
}

/// One `SET_PLAYBACK_DEVICES` to send.
struct GroupPlan {
  route: Arc<CastRoute>,
  reload_flight: Arc<tokio::sync::Mutex<()>>,
  /// Owner (never failed over by an answer).
  owner: String,
  /// Full member list sent.
  desired: Vec<String>,
  /// Members Cast is asked to add.
  added: Vec<String>,
  request: SetPlaybackDevices,
}

/// Desired Cast member order: confirmed members first (minus leavers), then new members.
fn desired_members(cast_members: &[String], owner: &str, followers: &[String]) -> Vec<String> {
  let wanted = |id: &String| id == owner || followers.contains(id);
  let mut desired: Vec<String> = cast_members.iter().filter(|id| wanted(id)).cloned().collect();
  for id in std::iter::once(owner.to_owned()).chain(followers.iter().cloned()) {
    if !desired.contains(&id) {
      desired.push(id);
    }
  }
  desired
}

/// Session name in the Google Home style: "<first> + <n - 1>", or the single speaker's name.
fn session_name(names: &[String]) -> Option<String> {
  let first = names.first()?;
  Some(match names.len() {
    1 => first.clone(),
    n => format!("{first} + {}", n - 1),
  })
}

/// Pool worker key for a multizone session host at `ip:port`.
fn session_host_key(endpoint: &SessionEndpoint) -> String {
  session_host_key_at(&endpoint.ip_address, endpoint.port)
}

/// Pool worker key a multizone session hosted at `ip:port` is controlled through.
pub(super) fn session_host_key_at(ip: &str, port: u16) -> String {
  format!("cast-session:{ip}:{port}")
}

impl Bridge {
  /// Decide this member's role in group `uuid` and install its slot state under one lock, so
  /// two members starting together cannot both become owner.
  pub(super) fn enter_group(&self, device_id: &str, uuid: &str, sample_rate: u32, ring: &Arc<PcmRing>) -> GroupEntry {
    let mut devices = self.devices.lock();
    let mut groups = self.groups.lock();
    let group = groups.entry(uuid.to_owned()).or_default();
    if group.excluded.contains(device_id) {
      drop(groups);
      drop(devices);
      return GroupEntry::Standalone;
    }
    let owner_live = group.owner.as_ref().filter(|owner| *owner != device_id).is_some_and(|owner| {
      devices
        .get(owner)
        .is_some_and(|slot| matches!(&slot.state, DeviceState::Starting { .. } | DeviceState::Playing { .. }))
    });

    if owner_live {
      if !group.followers.iter().any(|id| id == device_id) {
        group.followers.push(device_id.to_owned());
      }
      let owner = group.owner.clone();
      drop(groups);
      let slot = devices.entry(device_id.to_owned()).or_default();
      slot.generation = slot.generation.saturating_add(1);
      let generation = slot.generation;
      let following = DeviceState::Following {
        generation,
        follower: Box::new(FollowingSession {
          group: uuid.to_owned(),
          sample_rate,
          pump: RingPump::spawn(Arc::clone(ring)),
        }),
      };
      let prior = match std::mem::replace(&mut slot.state, following) {
        DeviceState::Playing { session, .. } => Some(session),
        DeviceState::Following { follower, .. } => {
          follower.pump.stop();
          None
        },
        DeviceState::Starting { .. } | DeviceState::Idle => None,
      };
      drop(devices);
      tracing::info!(
        %device_id,
        group = %uuid,
        owner = ?owner,
        generation,
        "AirPlay group member follows the group owner's Cast session"
      );
      return GroupEntry::Following { prior };
    }

    let mut stale_host = None;
    let media_key = if group.owner.as_deref() == Some(device_id) {
      // Owner restart (stream format change): reload where the session plays now.
      group.host_key.clone()
    } else {
      // New owner: a fresh Cast session on its own speaker.
      group.cast_members.clear();
      stale_host = group.host_key.take();
      None
    };
    group.owner = Some(device_id.to_owned());
    group.followers.retain(|id| id != device_id);
    if group.promoting.as_deref() == Some(device_id) {
      group.promoting = None;
    }
    drop(groups);
    let slot = devices.entry(device_id.to_owned()).or_default();
    let (generation, prior) = accept_start_in(slot, device_id);
    drop(devices);
    if let Some(host) = stale_host {
      self.close_session_host(host);
    }
    tracing::info!(%device_id, group = %uuid, generation, media_key = ?media_key, "AirPlay group owner starts the Cast session");
    GroupEntry::Owner { generation, prior, media_key }
  }

  /// Whether `device_id` currently follows a group owner.
  pub(super) fn is_following(&self, device_id: &str) -> bool {
    matches!(
      self.devices.lock().get(device_id).map(|slot| &slot.state),
      Some(DeviceState::Following { .. })
    )
  }

  /// Group of the Cast session `device_id` owns, if any.
  pub(super) fn session_group(&self, device_id: &str) -> Option<String> {
    match self.devices.lock().get(device_id).map(|slot| &slot.state) {
      Some(DeviceState::Playing { session, .. }) => session.group.clone(),
      Some(DeviceState::Idle | DeviceState::Starting { .. } | DeviceState::Following { .. }) | None => None,
    }
  }

  /// AirPlay device whose session plays on Cast worker `cast_key` (a speaker or a session host).
  pub(super) fn owner_for_cast_key(&self, cast_key: &str) -> Option<String> {
    let devices = self.devices.lock();
    let owner = devices.iter().find_map(|(device_id, slot)| match &slot.state {
      DeviceState::Playing { session, .. } if session.route.media() == cast_key => Some(device_id.clone()),
      DeviceState::Idle
      | DeviceState::Starting { .. }
      | DeviceState::Playing { .. }
      | DeviceState::Following { .. } => None,
    });
    drop(devices);
    owner
  }

  /// The owner's session is `Playing`: confirm it as the only Cast member of a fresh session and
  /// add the members waiting for it.
  pub(super) fn group_owner_playing(self: &Arc<Self>, uuid: &str, device_id: &str) {
    let mut groups = self.groups.lock();
    let Some(group) = groups.get_mut(uuid) else {
      return;
    };
    if group.owner.as_deref() != Some(device_id) {
      return;
    }
    if group.cast_members.is_empty() {
      group.cast_members.push(device_id.to_owned());
    }
    drop(groups);
    self.schedule_group_reconcile(uuid);
  }

  /// An accepted re-LOAD of `device_id`: if it owns a group whose Cast membership is unconfirmed
  /// (a fresh session after a failed move), confirm it and re-add the members.
  pub(super) fn confirm_group_session(self: &Arc<Self>, device_id: &str) {
    let Some(uuid) = self.session_group(device_id) else {
      return;
    };
    let unconfirmed = self
      .groups
      .lock()
      .get(&uuid)
      .is_some_and(|group| group.owner.as_deref() == Some(device_id) && group.cast_members.is_empty());
    if unconfirmed {
      tracing::info!(%device_id, group = %uuid, "group owner's Cast session re-LOADed; re-adding the members");
      self.group_owner_playing(&uuid, device_id);
    }
  }

  /// A follower's AirPlay session ended: drop it from the Cast group.
  pub(super) fn end_following(self: &Arc<Self>, device_id: &str, generation: u64) {
    let follower = {
      let mut devices = self.devices.lock();
      let Some(slot) = devices.get_mut(device_id) else {
        return;
      };
      if !matches!(&slot.state, DeviceState::Following { generation: g, .. } if *g == generation) {
        return;
      }
      let DeviceState::Following { follower, .. } = std::mem::take(&mut slot.state) else {
        return;
      };
      drop(devices);
      follower
    };
    let FollowingSession { group: uuid, pump, .. } = *follower;
    pump.stop();
    {
      let mut groups = self.groups.lock();
      if let Some(group) = groups.get_mut(&uuid) {
        group.followers.retain(|id| id != device_id);
      }
    }
    tracing::info!(%device_id, group = %uuid, "AirPlay group member left; removing its speaker from the Cast group");
    self.promote_if_ownerless(&uuid);
    self.schedule_group_reconcile(&uuid);
  }

  /// The group owner's AirPlay session ended. If members Cast already plays to remain, hand the
  /// Cast session to one of them and return `true`; otherwise the caller ends it normally.
  pub(super) fn group_owner_left(self: &Arc<Self>, device_id: &str, generation: u64) -> bool {
    let mut devices = self.devices.lock();
    let mut groups = self.groups.lock();
    let (uuid, channels, sample_rate) = match devices.get(device_id).map(|slot| &slot.state) {
      Some(DeviceState::Playing { generation: g, session }) if *g == generation => match &session.group {
        Some(uuid) => (uuid.clone(), session.channels, session.sample_rate),
        None => return false,
      },
      _ => return false,
    };
    let Some(group) = groups.get_mut(&uuid) else {
      return false;
    };
    if group.owner.as_deref() != Some(device_id) {
      return false;
    }
    // A member already in the Cast session, with the owner's stream format (the media server
    // encodes one format).
    let found = group
      .followers
      .iter()
      .find(|id| {
        group.cast_members.contains(id)
          && matches!(
            devices.get(*id).map(|slot| &slot.state),
            Some(DeviceState::Following { follower, .. })
              if follower.sample_rate == sample_rate && follower.pump.source().channels() == channels
          )
      })
      .cloned();
    let Some(survivor) = found else {
      return false;
    };

    // Both slot states were checked under these locks; take them together.
    let Some(survivor_slot) = devices.get_mut(&survivor) else {
      return false;
    };
    let DeviceState::Following { generation: survivor_gen, follower } = std::mem::take(&mut survivor_slot.state) else {
      return false;
    };
    let Some(owner_slot) = devices.get_mut(device_id) else {
      return false;
    };
    let DeviceState::Playing { mut session, .. } = std::mem::take(&mut owner_slot.state) else {
      return false;
    };
    session.device_id.clone_from(&survivor);
    session.route.set_volume(&survivor);
    if let Some(cancel) = session.watchdog_cancel.take() {
      let _cancelled = cancel.send(());
    }
    if let Some(task) = session.watchdog_task.take() {
      task.abort();
    }
    let (watchdog_cancel, watchdog_task) = spawn_stall_watchdog(Arc::clone(self), survivor.clone(), survivor_gen);
    session.watchdog_cancel = Some(watchdog_cancel);
    session.watchdog_task = Some(watchdog_task);
    let FollowingSession { pump, .. } = *follower;
    pump.forward_into(Arc::clone(&session.ring));
    if let Some(previous) = session.source.replace(pump) {
      previous.stop();
    }
    if let Some(slot) = devices.get_mut(&survivor) {
      slot.state = DeviceState::Playing { generation: survivor_gen, session };
    }
    group.owner = Some(survivor.clone());
    group.followers.retain(|id| *id != survivor);
    drop(groups);
    drop(devices);
    tracing::info!(
      old_owner = %device_id,
      new_owner = %survivor,
      group = %uuid,
      "AirPlay group owner left; its Cast session continues fed by a remaining member"
    );
    self.schedule_group_reconcile(&uuid);
    true
  }

  /// After a start attempt or a session end of `device_id`: if it owned a group but no longer
  /// has a live session, the group's Cast session is gone; a member takes over.
  pub(super) fn group_check_owner(&self, device_id: &str) {
    let live = self
      .devices
      .lock()
      .get(device_id)
      .is_some_and(super::state::DeviceSlot::is_live);
    if live {
      return;
    }
    let mut hosts = Vec::new();
    let mut ownerless = Vec::new();
    {
      let mut groups = self.groups.lock();
      for (uuid, group) in groups.iter_mut() {
        let _was_excluded = group.excluded.remove(device_id);
        if group.owner.as_deref() == Some(device_id) {
          tracing::info!(%device_id, group = %uuid, "AirPlay group owner session gone");
          group.owner = None;
          group.cast_members.clear();
          hosts.extend(group.host_key.take());
          ownerless.push(uuid.clone());
        }
      }
      groups.retain(|_, group| {
        if group.is_empty() {
          hosts.extend(group.host_key.take());
          false
        } else {
          true
        }
      });
    }
    for host in hosts {
      self.close_session_host(host);
    }
    for uuid in ownerless {
      self.promote_if_ownerless(&uuid);
    }
  }

  /// Ownerless group with members: the first member starts its own Cast session (as owner).
  fn promote_if_ownerless(&self, uuid: &str) {
    let candidate = {
      let mut groups = self.groups.lock();
      let Some(group) = groups.get_mut(uuid) else {
        return;
      };
      if group.owner.is_some() {
        return;
      }
      if group
        .promoting
        .as_ref()
        .is_some_and(|promoting| group.followers.contains(promoting))
      {
        return;
      }
      let Some(first) = group.followers.first().cloned() else {
        let empty = group.is_empty();
        let stale_host = group.host_key.take();
        if empty {
          drop(groups.remove(uuid));
        }
        drop(groups);
        if let Some(host) = stale_host {
          self.close_session_host(host);
        }
        return;
      };
      group.promoting = Some(first.clone());
      first
    };
    tracing::info!(device_id = %candidate, group = %uuid, "AirPlay group has no Cast session; member starts one");
    self.redispatch_start(&candidate);
  }

  /// Queue a `Started` for a follower so its device worker starts a session for it.
  fn redispatch_start(&self, device_id: &str) {
    let event = match self.devices.lock().get(device_id).map(|slot| &slot.state) {
      Some(DeviceState::Following { follower, .. }) => AirPlaySessionEvent::Started {
        device_id: device_id.to_owned(),
        sample_rate: follower.sample_rate,
        ring: Arc::clone(follower.pump.source()),
        group: Some(follower.group.clone()),
      },
      _ => return,
    };
    if self.redispatch.send(event).is_err() {
      tracing::warn!(%device_id, "bridge event intake closed; cannot start group member");
    }
  }

  /// End every follower of group `uuid` with its Cast session (ownership lost, terminal stall).
  ///
  /// Returns the followers whose AirPlay senders should be disconnected, and the session host
  /// worker the caller closes once the owner's teardown (its Cast STOP) went through it.
  pub(super) fn end_group(&self, uuid: &str) -> (Vec<String>, Option<String>) {
    let (followers, stale_host) = {
      let mut devices = self.devices.lock();
      let mut groups = self.groups.lock();
      let Some(group) = groups.remove(uuid) else {
        return (Vec::new(), None);
      };
      drop(groups);
      let mut ended = Vec::new();
      for id in group.followers {
        if let Some(slot) = devices.get_mut(&id)
          && matches!(&slot.state, DeviceState::Following { follower, .. } if follower.group == uuid)
          && let DeviceState::Following { follower, .. } = std::mem::take(&mut slot.state)
        {
          follower.pump.stop();
          ended.push(id);
        }
      }
      drop(devices);
      (ended, group.host_key)
    };
    if !followers.is_empty() {
      tracing::info!(group = %uuid, followers = ?followers, "Cast group session ended for every member");
    }
    (followers, stale_host)
  }

  /// Drop every group (bridge exit, after every session's teardown); follower slots are
  /// cleared by the caller.
  pub(super) fn forget_all_groups(&self) {
    let groups = std::mem::take(&mut *self.groups.lock());
    for group in groups.into_values() {
      if let Some(host) = group.host_key {
        self.close_session_host(host);
      }
    }
  }

  /// Close a session host worker off the async runtime (worker join can block briefly).
  pub(super) fn close_session_host(&self, key: String) {
    let pool = Arc::clone(&self.cast_pool);
    drop(tokio::task::spawn_blocking(move || pool.close_session_host(&key)));
  }

  /// Run (or re-run) membership reconciliation for group `uuid`.
  pub(super) fn schedule_group_reconcile(self: &Arc<Self>, uuid: &str) {
    let mut groups = self.groups.lock();
    let Some(group) = groups.get_mut(uuid) else {
      return;
    };
    if group.reconcile_running {
      group.reconcile_dirty = true;
      return;
    }
    group.reconcile_running = true;
    group.reconcile_dirty = false;
    drop(groups);
    let bridge = Arc::clone(self);
    let key = uuid.to_owned();
    self.spawn_detached(async move {
      bridge.run_group_reconcile(&key).await;
    });
  }

  async fn run_group_reconcile(self: &Arc<Self>, uuid: &str) {
    loop {
      tokio::time::sleep(GROUP_RECONCILE_DEBOUNCE).await;
      if !self.set_reconcile_dirty(uuid, false) {
        return;
      }
      if let Some(plan) = self.group_plan(uuid) {
        self.apply_group_plan(uuid, plan).await;
      }
      if self.finish_reconcile_pass(uuid) {
        return;
      }
    }
  }

  /// Set the group's dirty flag; `false` when the group is gone.
  fn set_reconcile_dirty(&self, uuid: &str, dirty: bool) -> bool {
    self.groups.lock().get_mut(uuid).is_some_and(|group| {
      group.reconcile_dirty = dirty;
      true
    })
  }

  /// End the reconcile task unless membership changed meanwhile; `true` when it ends.
  fn finish_reconcile_pass(&self, uuid: &str) -> bool {
    self.groups.lock().get_mut(uuid).is_none_or(|group| {
      if group.reconcile_dirty {
        false
      } else {
        group.reconcile_running = false;
        true
      }
    })
  }

  /// The `SET_PLAYBACK_DEVICES` that brings Cast to the current membership, if one is due.
  fn group_plan(&self, uuid: &str) -> Option<GroupPlan> {
    let devices = self.devices.lock();
    let groups = self.groups.lock();
    let group = groups.get(uuid)?;
    let owner = group.owner.clone()?;
    let Some(DeviceState::Playing { session, .. }) = devices.get(&owner).map(|slot| &slot.state) else {
      return None;
    };
    if session.group.as_deref() != Some(uuid) {
      return None;
    }
    let followers: Vec<String> = group
      .followers
      .iter()
      .filter(|id| {
        matches!(
          devices.get(*id).map(|slot| &slot.state),
          Some(DeviceState::Following { follower, .. }) if follower.group == uuid
        )
      })
      .cloned()
      .collect();
    let desired = desired_members(&group.cast_members, &owner, &followers);
    // No confirmed Cast session to expand yet (the owner's LOAD confirms it).
    if group.cast_members.is_empty() || desired == group.cast_members {
      return None;
    }
    let added: Vec<String> = desired.iter().filter(|id| !group.cast_members.contains(id)).cloned().collect();
    let route = Arc::clone(&session.route);
    let reload_flight = Arc::clone(&session.reload_flight);
    drop(groups);
    drop(devices);

    let mut devices_wire = Vec::with_capacity(desired.len());
    let mut names = Vec::with_capacity(desired.len());
    for id in &desired {
      let Some(device) = self.registry.get(id) else {
        tracing::warn!(device_id = %id, group = %uuid, "group member not in the Cast registry; left out");
        continue;
      };
      devices_wire.push(PlaybackDevice {
        device_id: device.id.clone(),
        ip_address: device.host.clone(),
        port: device.port,
      });
      names.push(device.name);
    }
    Some(GroupPlan {
      route,
      reload_flight,
      owner,
      desired,
      added,
      request: SetPlaybackDevices {
        devices: devices_wire,
        session_name: session_name(&names),
      },
    })
  }

  async fn apply_group_plan(self: &Arc<Self>, uuid: &str, plan: GroupPlan) {
    // No re-LOAD may interleave with a membership change (it would LOAD on a moving host).
    let _flight = plan.reload_flight.lock().await;
    let key = plan.route.media();
    tracing::info!(group = %uuid, cast_target = %key, members = ?plan.desired, "updating Cast multizone group");
    let pool = Arc::clone(&self.cast_pool);
    let request = plan.request.clone();
    let send_key = key.clone();
    let result = tokio::task::spawn_blocking(move || pool.set_playback_devices(&send_key, request))
      .await
      .unwrap_or_else(|join_err| Err(Error::Bridge(format!("multizone task join: {join_err}"))));

    let candidates: Vec<String> = plan.added.iter().filter(|id| **id != plan.owner).cloned().collect();
    let (failed, moved_to) = match result {
      Ok(update) => {
        let failed: Vec<String> = candidates
          .iter()
          .filter(|id| update.failed_devices.contains(&normalize_device_id(id)))
          .cloned()
          .collect();
        let mut groups = self.groups.lock();
        if let Some(group) = groups.get_mut(uuid) {
          group.cast_members = plan.desired.iter().filter(|id| !failed.contains(id)).cloned().collect();
        }
        drop(groups);
        (failed, update.endpoint)
      },
      Err(err) => {
        tracing::warn!(group = %uuid, cast_target = %key, error = %err, "Cast multizone request failed");
        (candidates, None)
      },
    };

    if !failed.is_empty() {
      {
        let mut groups = self.groups.lock();
        if let Some(group) = groups.get_mut(uuid) {
          group.followers.retain(|id| !failed.contains(id));
          group.excluded.extend(failed.iter().cloned());
        }
      }
      for device_id in &failed {
        tracing::warn!(%device_id, group = %uuid, "Cast could not add the speaker to the group; it plays standalone");
        self.redispatch_start(device_id);
      }
    }

    if let Some(endpoint) = moved_to {
      self.follow_session_endpoint(uuid, &plan.owner, &plan.route, &endpoint).await;
    }
  }

  /// The Cast session moved to `endpoint`: media control (LOAD, PAUSE, PLAY, STOP, ownership
  /// probes) follows it there, since that host runs the Default Media Receiver now.
  ///
  /// If the session cannot be taken over there, it no longer plays where control was either, so
  /// the owner re-LOADs a fresh session on its own speaker and the members are added back.
  async fn follow_session_endpoint(
    self: &Arc<Self>,
    uuid: &str,
    owner: &str,
    route: &Arc<CastRoute>,
    endpoint: &SessionEndpoint,
  ) {
    let new_key = session_host_key(endpoint);
    let old_key = route.media();
    if new_key == old_key {
      return;
    }
    let pool = Arc::clone(&self.cast_pool);
    let (host, port) = (endpoint.ip_address.clone(), endpoint.port);
    let (from, to) = (old_key.clone(), new_key.clone());
    let session_id = endpoint.session_id.clone();
    let moved = tokio::task::spawn_blocking(move || {
      pool.open_session_host(&to, &host, port);
      pool.move_session(&from, &to, session_id.as_deref())
    })
    .await
    .unwrap_or_else(|join_err| Err(Error::Bridge(format!("session move task join: {join_err}"))));
    let previous_host = match moved {
      Ok(()) => {
        route.set_media(&new_key);
        let previous = {
          let mut groups = self.groups.lock();
          groups.get_mut(uuid).and_then(|group| group.host_key.replace(new_key.clone()))
        };
        tracing::info!(
          group = %uuid,
          from = %old_key,
          to = %new_key,
          "Cast group session moved; media control follows the session host"
        );
        previous
      },
      Err(err) => {
        tracing::warn!(
          group = %uuid,
          from = %old_key,
          host = %new_key,
          %owner,
          error = %err,
          "cannot take over the moved Cast session; re-LOADing on the owner's speaker and re-adding members"
        );
        route.set_media(owner);
        let previous = {
          let mut groups = self.groups.lock();
          groups.get_mut(uuid).and_then(|group| {
            group.cast_members.clear();
            group.host_key.take()
          })
        };
        self.close_session_host(new_key.clone());
        self.reload_group_owner(uuid, owner);
        previous
      },
    };
    if let Some(previous) = previous_host.filter(|previous| *previous != new_key) {
      self.close_session_host(previous);
    }
  }

  /// Fresh LOAD of the group owner's session on its own speaker (after a failed session move).
  ///
  /// Runs as a session task behind the membership change that holds the re-LOAD flight; the
  /// accepted LOAD re-adds the members ([`Self::confirm_group_session`]). While paused, the
  /// resume's PLAY finds no media on the speaker and re-LOADs instead.
  fn reload_group_owner(self: &Arc<Self>, uuid: &str, owner: &str) {
    let Some(generation) = self.session_generation(owner) else {
      return;
    };
    let bridge = Arc::clone(self);
    let (group, device_id) = (uuid.to_owned(), owner.to_owned());
    self.spawn_session_task(owner, generation, async move {
      match bridge
        .reload_playing_media(&device_id, generation, "group_move_failed", None)
        .await
      {
        ReloadOutcome::Loaded => {},
        ReloadOutcome::Paused => {
          tracing::info!(%device_id, %group, generation, "group session re-LOAD waits for resume");
        },
        ReloadOutcome::Failed => {
          tracing::warn!(%device_id, %group, generation, "group session re-LOAD on the owner's speaker failed");
        },
      }
    });
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::time::Instant;

  use tokio::sync::mpsc;
  use tokio::task::JoinHandle;

  use crate::bridge::RingLookup;
  use crate::bridge::test_support::{CastCall, FakeCast, SwapRingLookup, started_ring, test_device};
  use crate::cast::{CastControl, MultizoneMember, PlaybackSessionUpdate};
  use crate::registry::DeviceRegistry;

  const GROUP: &str = "group-uuid";
  const A: &str = "aaaa0000aaaa0000aaaa0000aaaa0000";
  const B: &str = "bbbb0000bbbb0000bbbb0000bbbb0000";
  const C: &str = "cccc0000cccc0000cccc0000cccc0000";
  const HOST_KEY: &str = "cast-session:127.0.0.1:32227";
  const WAIT: Duration = Duration::from_secs(5);

  struct Harness {
    bridge: Arc<Bridge>,
    cast: Arc<FakeCast>,
    rings: Arc<SwapRingLookup>,
    events: mpsc::UnboundedSender<AirPlaySessionEvent>,
    run: JoinHandle<()>,
  }

  impl Harness {
    fn start() -> Self {
      let registry = Arc::new(DeviceRegistry::new());
      for (id, name) in [(A, "Pool Bathroom speaker"), (B, "Gym speaker"), (C, "Kitchen speaker")] {
        let _appeared = registry.appear(test_device(id, name));
      }
      let cast = Arc::new(FakeCast::default());
      let control: Arc<dyn CastControl> = Arc::<FakeCast>::clone(&cast);
      let bridge = Arc::new(Bridge::new(registry, control));
      let rings = Arc::new(SwapRingLookup::default());
      let (events, rx) = mpsc::unbounded_channel();
      let lookup: Arc<dyn RingLookup> = Arc::<SwapRingLookup>::clone(&rings);
      let run = tokio::spawn(Arc::clone(&bridge).run(rx, lookup));
      Self { bridge, cast, rings, events, run }
    }

    /// A receiver's AirPlay stream of the multi-select starts (ring holds the start lead).
    fn airplay_start(&self, device_id: &str) -> Arc<PcmRing> {
      let ring = started_ring();
      self.rings.set(device_id, Some(Arc::clone(&ring)));
      self.send(AirPlaySessionEvent::Started {
        device_id: device_id.to_owned(),
        sample_rate: 48_000,
        ring: Arc::clone(&ring),
        group: Some(GROUP.to_owned()),
      });
      ring
    }

    fn airplay_end(&self, device_id: &str) {
      self.rings.set(device_id, None);
      self.send(AirPlaySessionEvent::Ended { device_id: device_id.to_owned() });
    }

    fn send(&self, event: AirPlaySessionEvent) {
      self.events.send(event).expect("bridge intake");
    }

    async fn wait_for(&self, what: &str, check: impl Fn(&Self) -> bool) {
      let started = Instant::now();
      while !check(self) {
        assert!(
          started.elapsed() < WAIT,
          "timed out waiting for {what}; calls: {:?}",
          self.cast.calls()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
      }
    }

    fn set_playback_calls(&self) -> Vec<(String, Vec<String>)> {
      self
        .cast
        .calls()
        .into_iter()
        .filter_map(|call| {
          if let CastCall::SetPlaybackDevices { key, members } = call {
            Some((key, members))
          } else {
            None
          }
        })
        .collect()
    }

    fn playing_owner(&self, device_id: &str) -> bool {
      self.bridge.session_group(device_id).as_deref() == Some(GROUP)
    }

    fn cast_members(&self) -> Vec<String> {
      self
        .bridge
        .groups
        .lock()
        .get(GROUP)
        .map(|group| group.cast_members.clone())
        .unwrap_or_default()
    }

    async fn stop(self) {
      self.bridge.request_shutdown();
      drop(tokio::time::timeout(Duration::from_secs(10), self.run).await);
    }
  }

  fn moved_to_group_endpoint() -> PlaybackSessionUpdate {
    PlaybackSessionUpdate {
      name: Some("Pool Bathroom speaker + 1".to_owned()),
      failed_devices: Vec::new(),
      endpoint: Some(SessionEndpoint {
        device_id: "5c23adbc-893b-446c-bb38-a935d16e1be4".to_owned(),
        ip_address: "127.0.0.1".to_owned(),
        port: 32227,
        session_id: Some("session-a".to_owned()),
      }),
    }
  }

  /// A owns the Cast session; B joins it; Cast moves the session to the group endpoint.
  async fn form_group(h: &Harness) -> (Arc<PcmRing>, Arc<PcmRing>) {
    h.cast.answer(Ok(moved_to_group_endpoint()));
    let ring_a = h.airplay_start(A);
    h.wait_for("owner Playing", |it| it.playing_owner(A)).await;
    let ring_b = h.airplay_start(B);
    h.wait_for("follower joined", |it| it.cast_members() == ids(&[A, B])).await;
    h.wait_for("session moved to the group endpoint", |it| {
      it.bridge.owner_for_cast_key(HOST_KEY).as_deref() == Some(A)
    })
    .await;
    (ring_a, ring_b)
  }

  #[tokio::test]
  async fn first_member_loads_and_the_next_joins_by_multizone() {
    let h = Harness::start();
    let _rings = form_group(&h).await;

    assert!(h.bridge.is_following(B), "second member follows instead of loading");
    let loads: Vec<CastCall> = h.cast.calls().into_iter().filter(|c| matches!(c, CastCall::Load(_))).collect();
    assert_eq!(loads, vec![CastCall::Load(A.to_owned())], "only the owner LOADs");
    assert_eq!(
      h.set_playback_calls(),
      vec![(A.to_owned(), ids(&[A, B]))],
      "expand on the owner's speaker"
    );
    let calls = h.cast.calls();
    assert!(calls.contains(&CastCall::OpenHost {
      key: HOST_KEY.to_owned(),
      host: "127.0.0.1".to_owned(),
      port: 32227,
    }));
    assert!(calls.contains(&CastCall::Move {
      from: A.to_owned(),
      to: HOST_KEY.to_owned(),
      session_id: Some("session-a".to_owned()),
    }));
    h.stop().await;
  }

  /// Live failure: the owner leaves, the shrink moves the session to a new endpoint, and the
  /// session cannot be taken over there. Control must not stay on the old (gone) endpoint: the
  /// surviving owner re-LOADs on its own speaker, the remaining member is added back, and
  /// transport commands go to the new session.
  #[tokio::test]
  async fn failed_takeover_after_move_reloads_on_the_survivor_and_re_expands() {
    let h = Harness::start();
    let _rings = form_group(&h).await;
    let _ring_c = h.airplay_start(C);
    h.wait_for("third member joined", |it| it.cast_members() == ids(&[A, B, C]))
      .await;
    let moved_host = "cast-session:127.0.0.1:32300";
    h.cast.answer(Ok(PlaybackSessionUpdate {
      name: Some("Gym speaker + 1".to_owned()),
      failed_devices: Vec::new(),
      endpoint: Some(SessionEndpoint {
        device_id: "7d0c1f2e-0000-4000-8000-000000000001".to_owned(),
        ip_address: "127.0.0.1".to_owned(),
        port: 32300,
        session_id: Some("session-a".to_owned()),
      }),
    }));
    h.cast
      .answer_move(Err(Error::Cast("adopt media status: failed to fill whole buffer".to_owned())));

    h.airplay_end(A);
    h.wait_for("fresh LOAD on the survivor's speaker", |it| {
      it.cast.calls().contains(&CastCall::Load(B.to_owned()))
    })
    .await;
    h.wait_for("members re-added on the survivor's speaker", |it| {
      it.cast_members() == ids(&[B, C])
    })
    .await;

    let calls = h.cast.calls();
    assert!(calls.contains(&CastCall::Move {
      from: HOST_KEY.to_owned(),
      to: moved_host.to_owned(),
      session_id: Some("session-a".to_owned()),
    }));
    assert!(calls.contains(&CastCall::CloseHost(moved_host.to_owned())));
    assert!(
      calls.contains(&CastCall::CloseHost(HOST_KEY.to_owned())),
      "the old endpoint is dropped"
    );
    assert_eq!(
      h.set_playback_calls().last(),
      Some(&(B.to_owned(), ids(&[B, C]))),
      "re-expansion from the survivor's speaker"
    );
    assert_eq!(h.bridge.owner_for_cast_key(B).as_deref(), Some(B));
    assert_eq!(h.bridge.owner_for_cast_key(HOST_KEY), None, "no control on the gone endpoint");
    assert!(h.playing_owner(B));
    assert!(h.bridge.is_following(C));

    for device_id in [B, C] {
      h.send(AirPlaySessionEvent::Paused {
        device_id: device_id.to_owned(),
        at: Instant::now(),
      });
    }
    h.wait_for("pause on the survivor's speaker", |it| {
      it.cast.calls().contains(&CastCall::Pause(B.to_owned()))
    })
    .await;
    assert!(!h.cast.calls().contains(&CastCall::Pause(HOST_KEY.to_owned())));
    h.stop().await;
  }

  #[tokio::test]
  async fn follower_leaving_shrinks_the_list_on_the_session_host() {
    let h = Harness::start();
    let _rings = form_group(&h).await;

    h.airplay_end(B);
    h.wait_for("follower removed", |it| it.cast_members() == ids(&[A])).await;
    assert_eq!(h.set_playback_calls().last(), Some(&(HOST_KEY.to_owned(), ids(&[A]))));
    assert!(!h.bridge.has_session(B));
    assert!(h.playing_owner(A), "owner keeps playing");
    assert!(
      !h.cast.calls().iter().any(|c| matches!(c, CastCall::Stop(_))),
      "a follower leaving stops nothing"
    );
    h.stop().await;
  }

  #[tokio::test]
  async fn owner_leaving_hands_the_session_to_a_member() {
    let h = Harness::start();
    let (_ring_a, ring_b) = form_group(&h).await;
    let _ring_c = h.airplay_start(C);
    h.wait_for("third member joined", |it| it.cast_members() == ids(&[A, B, C]))
      .await;

    h.airplay_end(A);
    h.wait_for("owner dropped from the Cast list", |it| it.cast_members() == ids(&[B, C]))
      .await;
    assert_eq!(h.set_playback_calls().last(), Some(&(HOST_KEY.to_owned(), ids(&[B, C]))));
    assert!(h.playing_owner(B), "B owns the Cast session now");
    assert!(h.bridge.is_following(C));
    assert!(!h.bridge.has_session(A));
    assert_eq!(
      h.bridge.owner_for_cast_key(HOST_KEY).as_deref(),
      Some(B),
      "same Cast session, same host"
    );
    assert!(
      !h.cast
        .calls()
        .iter()
        .any(|c| { matches!(c, CastCall::Stop(_)) || matches!(c, CastCall::Load(key) if key != A) }),
      "hand-over neither stops nor re-LOADs"
    );

    // B's AirPlay audio now feeds the media ring of the Cast session.
    let media_ring = {
      let devices = h.bridge.devices.lock();
      match devices.get(B).map(|slot| &slot.state) {
        Some(DeviceState::Playing { session, .. }) => Arc::clone(&session.ring),
        _ => panic!("B must be Playing"),
      }
    };
    media_ring.clear();
    ring_b.push_f32(&[0.25; 512]);
    h.wait_for("member audio forwarded", |_| media_ring.available_frames() >= 256)
      .await;

    // Last members leave: the session ends with a STOP on the session host.
    h.airplay_end(C);
    h.wait_for("C removed", |it| it.cast_members() == ids(&[B])).await;
    h.airplay_end(B);
    h.wait_for("group session stopped", |it| {
      it.cast.calls().contains(&CastCall::Stop(HOST_KEY.to_owned()))
    })
    .await;
    h.wait_for("group forgotten", |it| it.bridge.groups.lock().is_empty()).await;
    h.stop().await;
  }

  #[tokio::test]
  async fn transport_events_reach_cast_once_and_volume_stays_per_speaker() {
    let h = Harness::start();
    let _rings = form_group(&h).await;

    for device_id in [A, B] {
      h.send(AirPlaySessionEvent::Paused {
        device_id: device_id.to_owned(),
        at: Instant::now(),
      });
      h.send(AirPlaySessionEvent::Volume {
        device_id: device_id.to_owned(),
        volume_db: -10.0,
      });
    }
    h.wait_for("pause", |it| it.cast.calls().contains(&CastCall::Pause(HOST_KEY.to_owned())))
      .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let calls = h.cast.calls();
    let pauses = calls.iter().filter(|c| matches!(c, CastCall::Pause(_))).count();
    assert_eq!(pauses, 1, "one Cast PAUSE for the group: {calls:?}");
    assert!(calls.contains(&CastCall::SetVolume(A.to_owned())));
    assert!(
      calls.contains(&CastCall::SetVolume(B.to_owned())),
      "follower volume goes to its own speaker"
    );
    assert!(!calls.contains(&CastCall::SetVolume(HOST_KEY.to_owned())));

    for device_id in [A, B] {
      h.send(AirPlaySessionEvent::Resumed { device_id: device_id.to_owned() });
    }
    h.wait_for("resume", |it| it.cast.calls().contains(&CastCall::Play(HOST_KEY.to_owned())))
      .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let plays = h.cast.calls().iter().filter(|c| matches!(c, CastCall::Play(_))).count();
    assert_eq!(plays, 1, "one Cast PLAY for the group");
    h.stop().await;
  }

  /// Live case: a Google Home speaker group joins a speaker's session, then owns it after the
  /// speaker leaves. Its session never plays on its own endpoint, so its volume goes to its
  /// member speakers, never to its master volume.
  #[tokio::test]
  async fn speaker_group_volume_goes_to_its_members_while_its_session_plays_elsewhere() {
    let h = Harness::start();
    let mut group = test_device(B, "Gym and Pool").with_model(crate::registry::CAST_GROUP_MODEL);
    group.port = 32_128;
    let _known = h.bridge.registry.appear(group);
    h.cast.set_members(
      B,
      vec![
        MultizoneMember { device_id: A.to_owned(), level: 0.2 },
        MultizoneMember { device_id: C.to_owned(), level: 0.6 },
      ],
    );
    let _rings = form_group(&h).await;
    assert!(h.bridge.is_following(B));

    let volume = |volume_db: f32| AirPlaySessionEvent::Volume { device_id: B.to_owned(), volume_db };
    let member_keys = |it: &Harness| {
      let mut keys: Vec<String> = it.cast.volumes().into_iter().map(|(key, _)| key).collect();
      keys.sort();
      keys
    };
    h.send(volume(-6.0));
    h.wait_for("following group's members set", |it| it.cast.volumes().len() == 2)
      .await;
    assert_eq!(member_keys(&h), ids(&[A, C]));

    h.airplay_end(A);
    h.wait_for("group receiver owns the session", |it| it.playing_owner(B)).await;
    h.send(volume(-12.0));
    h.wait_for("owning group's members set", |it| it.cast.volumes().len() == 4)
      .await;
    assert_eq!(member_keys(&h), ids(&[A, A, C, C]));
    assert!(!h.cast.calls().contains(&CastCall::SetVolume(B.to_owned())));
    h.stop().await;
  }

  #[tokio::test]
  async fn member_cast_cannot_add_plays_standalone() {
    let h = Harness::start();
    h.cast.answer(Ok(PlaybackSessionUpdate {
      failed_devices: vec![B.to_owned()],
      ..PlaybackSessionUpdate::default()
    }));
    let _ring_a = h.airplay_start(A);
    h.wait_for("owner Playing", |it| it.playing_owner(A)).await;
    let ring_b = h.airplay_start(B);
    // The sender keeps streaming (about 5x real time, as an iPhone does after a skip): the
    // drained ring refills to the start lead once B stops following.
    h.wait_for("standalone LOAD", |it| {
      ring_b.push_f32(&[0.01; 4_800]);
      it.cast.calls().contains(&CastCall::Load(B.to_owned()))
    })
    .await;
    h.wait_for("standalone Playing", |it| {
      it.bridge.has_session(B) && !it.bridge.is_following(B)
    })
    .await;
    assert_eq!(h.cast_members(), ids(&[A]));
    assert_eq!(h.bridge.session_group(B), None, "B's session is not the group's");
    h.stop().await;
  }

  #[tokio::test]
  async fn refused_request_falls_back_to_standalone() {
    let h = Harness::start();
    h.cast
      .answer(Err(Error::Cast("multizone INVALID_REQUEST: INVALID_COMMAND".to_owned())));
    let _ring_a = h.airplay_start(A);
    h.wait_for("owner Playing", |it| it.playing_owner(A)).await;
    let ring_b = h.airplay_start(B);
    // The sender keeps streaming (about 5x real time, as an iPhone does after a skip): the
    // drained ring refills to the start lead once B stops following.
    h.wait_for("standalone LOAD", |it| {
      ring_b.push_f32(&[0.01; 4_800]);
      it.cast.calls().contains(&CastCall::Load(B.to_owned()))
    })
    .await;
    assert_eq!(h.cast_members(), ids(&[A]));
    h.stop().await;
  }

  #[tokio::test]
  async fn single_receiver_streams_never_touch_multizone() {
    let h = Harness::start();
    let ring = started_ring();
    h.rings.set(A, Some(Arc::clone(&ring)));
    h.send(AirPlaySessionEvent::Started {
      device_id: A.to_owned(),
      sample_rate: 48_000,
      ring,
      group: None,
    });
    h.wait_for("Playing", |it| it.bridge.has_session(A) && !it.bridge.is_following(A))
      .await;
    tokio::time::sleep(GROUP_RECONCILE_DEBOUNCE * 2).await;
    assert_eq!(h.set_playback_calls(), Vec::new());
    assert!(h.bridge.groups.lock().is_empty());
    h.stop().await;
  }

  fn ids(list: &[&str]) -> Vec<String> {
    list.iter().map(|id| (*id).to_owned()).collect()
  }

  #[test]
  fn desired_members_keep_cast_order_and_append_joiners() {
    assert_eq!(desired_members(&ids(&["a"]), "a", &ids(&["b", "c"])), ids(&["a", "b", "c"]));
    assert_eq!(
      desired_members(&ids(&["a", "b", "c"]), "b", &ids(&["c"])),
      ids(&["b", "c"]),
      "old owner leaves the list after a hand-over"
    );
    assert_eq!(desired_members(&ids(&["a", "b"]), "a", &[]), ids(&["a"]));
    assert_eq!(desired_members(&[], "a", &[]), ids(&["a"]));
  }

  #[test]
  fn session_name_follows_google_home_style() {
    assert_eq!(session_name(&[]), None);
    assert_eq!(session_name(&ids(&["Gym speaker"])).as_deref(), Some("Gym speaker"));
    assert_eq!(
      session_name(&ids(&["Pool Bathroom speaker", "Gym speaker"])).as_deref(),
      Some("Pool Bathroom speaker + 1")
    );
  }
}
