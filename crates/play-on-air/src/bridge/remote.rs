//! Now-playing metadata toward Cast, and Cast-side user actions back to the AirPlay sender.
//!
//! Metadata: the sender's title / artist / album and artwork become Cast
//! `MusicTrackMediaMetadata`, sent with every (re-)LOAD and, while playing, as a `QUEUE_UPDATE`
//! of the current item (no re-LOAD, so audio is not interrupted). Artwork is served by the
//! session's media server at `/artwork/<id>`.
//!
//! Reverse control: a play / pause / volume change on the Cast device that the pool did not
//! command (touch controls, voice, Google Home app) is forwarded to the sender over the AirPlay
//! event channel (`docs/ap2-receiver-spec.md` section 5). For a pause the bridge pauses locally
//! first, as Apple's receiver does, so the sender's resulting rate-0 is not echoed back to Cast.

use std::sync::atomic::Ordering;
use std::time::Instant;

use shairplay::MediaRemoteCommand;

use crate::airplay::NowPlaying;
use crate::cast::{CastRemoteEvent, CastRemoteKind, CastTrack};
use crate::media::MediaServerHandle;

use super::Bridge;
use super::state::DeviceState;

/// Cast metadata for `now_playing`, served from `media`. Without a sender title, the Cast
/// device name stands in so the sink UI never shows a bare URL.
fn cast_track(now_playing: &NowPlaying, media: &MediaServerHandle, fallback_title: &str) -> CastTrack {
  CastTrack {
    title: Some(now_playing.title.clone().unwrap_or_else(|| fallback_title.to_owned())),
    artist: now_playing.artist.clone(),
    album: now_playing.album.clone(),
    image_url: now_playing.artwork.as_deref().map(|art| media.artwork_url(art)),
  }
}

impl Bridge {
  /// Metadata for a LOAD on `media`: serves the current artwork there and builds the track.
  pub(super) fn load_track(&self, device_id: &str, media: &MediaServerHandle, fallback_title: &str) -> CastTrack {
    let now_playing = self
      .airplay
      .as_ref()
      .and_then(|airplay| airplay.now_playing(device_id))
      .unwrap_or_default();
    media.set_artwork(now_playing.artwork.clone());
    cast_track(&now_playing, media, fallback_title)
  }

  /// The sender's now-playing changed: refresh the live session's Cast metadata in place.
  ///
  /// Not playing yet: the next LOAD reads the receiver's now-playing state instead.
  pub(super) fn handle_now_playing(&self, device_id: &str, now_playing: &NowPlaying) {
    let track = {
      let devices = self.devices.lock();
      let Some(DeviceState::Playing { session, .. }) = devices.get(device_id).map(|slot| &slot.state) else {
        return;
      };
      session.media.set_artwork(now_playing.artwork.clone());
      let track = cast_track(now_playing, &session.media, &session.cast_name);
      session.track.lock().clone_from(&track);
      drop(devices);
      track
    };
    self.cast_pool.update_metadata(device_id, track);
  }

  /// A Cast-side change we did not command: forward it to the AirPlay sender.
  pub fn on_cast_remote(&self, event: &CastRemoteEvent) {
    let device_id = event.device_id.as_str();
    match event.kind {
      CastRemoteKind::Paused => {
        if self.pause_locally_for_cast(device_id) {
          self.send_to_sender(device_id, MediaRemoteCommand::Pause);
        }
      },
      CastRemoteKind::Played => {
        if self.is_locally_paused(device_id) {
          // Local resume follows the sender's own rate change (Resumed event).
          self.send_to_sender(device_id, MediaRemoteCommand::Play);
        }
      },
      CastRemoteKind::Volume { level, muted } => {
        if !muted {
          let mut devices = self.devices.lock();
          if let Some(DeviceState::Playing { session, .. }) = devices.get_mut(device_id).map(|slot| &mut slot.state) {
            *session.last_volume_linear.lock() = Some(level);
          }
          drop(devices);
        }
        let Some(airplay) = &self.airplay else {
          return;
        };
        match airplay.report_volume(device_id, level, muted) {
          Ok(()) => tracing::debug!(%device_id, level, muted, "Cast volume change reported to AirPlay sender"),
          Err(err) => tracing::debug!(%device_id, error = %err, "Cast volume change not reported to AirPlay sender"),
        }
      },
    }
  }

  /// Mark the Playing session paused because Cast already paused itself (no Cast PAUSE).
  ///
  /// Returns `false` when there is no live session or it is already paused (nothing to
  /// forward). The sender's rate-0 that follows then hits an already-paused session and
  /// issues no Cast command.
  fn pause_locally_for_cast(&self, device_id: &str) -> bool {
    let mut devices = self.devices.lock();
    let Some(DeviceState::Playing { generation, session }) = devices.get_mut(device_id).map(|slot| &mut slot.state)
    else {
      return false;
    };
    if session.paused {
      return false;
    }
    session.paused = true;
    session.playback_paused.store(true, Ordering::Release);
    session.paused_at = Some(Instant::now());
    let live_generation = *generation;
    drop(devices);
    tracing::info!(%device_id, generation = live_generation, "Cast paused on the device; pausing AirPlay sender");
    true
  }

  fn is_locally_paused(&self, device_id: &str) -> bool {
    matches!(
      self.devices.lock().get(device_id).map(|slot| &slot.state),
      Some(DeviceState::Playing { session, .. }) if session.paused
    )
  }

  fn send_to_sender(&self, device_id: &str, command: MediaRemoteCommand) {
    let Some(airplay) = &self.airplay else {
      return;
    };
    match airplay.send_media_remote_command(device_id, command) {
      Ok(()) => tracing::info!(%device_id, code = command.code(), "forwarded Cast control to AirPlay sender"),
      Err(err) => tracing::warn!(%device_id, code = command.code(), error = %err, "could not forward Cast control"),
    }
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use bytes::Bytes;

  use super::*;
  use crate::audio::PcmRing;
  use crate::bridge::test_support::{insert_playing, test_playing_session};
  use crate::cast::CastPool;
  use crate::media::{Artwork, MediaServer};
  use crate::registry::DeviceRegistry;

  fn remote(device_id: &str, kind: CastRemoteKind) -> CastRemoteEvent {
    CastRemoteEvent { device_id: device_id.to_owned(), kind }
  }

  /// Device pause: local pause first, and the sender's rate-0 echo then arms no Cast PAUSE.
  #[tokio::test]
  async fn cast_pause_pauses_locally_and_absorbs_sender_echo() {
    let pool = Arc::new(CastPool::new(None));
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    insert_playing(
      &bridge,
      "dev-rc",
      1,
      test_playing_session(media, "dev-rc", pool, Arc::new(PcmRing::new(2, 64))),
    );

    bridge.on_cast_remote(&remote("dev-rc", CastRemoteKind::Paused));
    assert!(bridge.is_locally_paused("dev-rc"));
    // Second report (or our own PAUSE status) changes nothing.
    assert!(!bridge.pause_locally_for_cast("dev-rc"));

    // Sender honors `paus` → AirPlay rate 0 → bridge must not send Cast PAUSE back.
    bridge.handle_pause("dev-rc");
    {
      let devices = bridge.devices.lock();
      let Some(DeviceState::Playing { session, .. }) = devices.get("dev-rc").map(|slot| &slot.state) else {
        panic!("expected Playing");
      };
      assert!(session.playback_paused.load(Ordering::Acquire));
      assert!(session.pause_debounce_cancel.is_none(), "no Cast PAUSE echo armed");
      drop(devices);
    }

    // Device play while paused leaves local state for the sender's own resume.
    bridge.on_cast_remote(&remote("dev-rc", CastRemoteKind::Played));
    assert!(bridge.is_locally_paused("dev-rc"));
    bridge.handle_session_end("dev-rc").await;
  }

  #[tokio::test]
  async fn cast_volume_updates_rollover_volume_unless_muted() {
    let pool = Arc::new(CastPool::new(None));
    let bridge = Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let session = test_playing_session(media, "dev-vol-rc", pool, Arc::new(PcmRing::new(2, 64)));
    let last_volume = Arc::clone(&session.last_volume_linear);
    insert_playing(&bridge, "dev-vol-rc", 1, session);

    bridge.on_cast_remote(&remote("dev-vol-rc", CastRemoteKind::Volume { level: 0.35, muted: false }));
    assert_eq!(*last_volume.lock(), Some(0.35));
    bridge.on_cast_remote(&remote("dev-vol-rc", CastRemoteKind::Volume { level: 0.9, muted: true }));
    assert_eq!(*last_volume.lock(), Some(0.35), "mute keeps the level to restore");
    bridge.handle_session_end("dev-vol-rc").await;
  }

  /// Now-playing updates the session track (used by re-LOADs) and serves the artwork.
  #[tokio::test]
  async fn now_playing_updates_session_track_and_artwork() {
    let pool = Arc::new(CastPool::new(None));
    let bridge = Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let base_url = media.base_url.clone();
    let session = test_playing_session(media, "dev-np", pool, Arc::new(PcmRing::new(2, 64)));
    let track = Arc::clone(&session.track);
    insert_playing(&bridge, "dev-np", 1, session);

    let now_playing = NowPlaying {
      title: None,
      artist: Some("Band".to_owned()),
      album: Some("Record".to_owned()),
      artwork: Some(Arc::new(Artwork {
        id: 9_001,
        content_type: "image/jpeg".to_owned(),
        body: Bytes::from_static(b"jpeg"),
      })),
    };
    bridge.handle_now_playing("dev-np", &now_playing);
    let shown = track.lock().clone();
    assert_eq!(shown.title.as_deref(), Some("test"), "Cast name stands in for a missing title");
    assert_eq!(shown.artist.as_deref(), Some("Band"));
    assert_eq!(shown.album.as_deref(), Some("Record"));
    let image_url = format!("{base_url}/artwork/9001");
    assert_eq!(shown.image_url.as_deref(), Some(image_url.as_str()));
    assert!(crate::bridge::test_support::http_get_status_ok(&image_url).await);
    bridge.handle_session_end("dev-np").await;
  }
}
