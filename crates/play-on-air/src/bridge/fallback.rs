//! FLAC LIVE → WAV BUFFERED fallback: rejection memory, failure strikes, and live switch.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::error::Error;
use crate::media::MediaContent;

use super::Bridge;
use super::load::{ReloadOutcome, spawn_rollover_reload_loop};
use super::state::{DeviceState, EgressKind, PlayingSession};

/// Early-session FLAC → WAV fallback window after a successful FLAC LOAD.
///
/// Decisive rejections inside this window (IDLE recovery, stall) fall back immediately.
pub(super) const FLAC_FALLBACK_WINDOW: Duration = Duration::from_secs(10);

/// Two FLAC failure strikes within this window (any session age) → switch LIVE to WAV.
const FLAC_STRIKE_PAIR_WINDOW: Duration = Duration::from_secs(90);

/// True when the Cast error is transport-shaped (timeout, disconnect, no session/worker).
///
/// Those must not poison process-lifetime FLAC→WAV memory; the caller may still fall back to
/// WAV for *this* session only.
pub(super) fn is_transport_shaped_cast_error(err: &Error) -> bool {
  // Join / bridge wrapper around a failed pool call is not a decisive media rejection.
  let msg = match err {
    Error::Cast(msg) | Error::Bridge(msg) => msg.as_str(),
    Error::Io(_) => return true,
    Error::Config { .. } | Error::Discovery(_) | Error::Media(_) | Error::AirPlay(_) | Error::Audio(_) => return false,
  };
  let lower = msg.to_ascii_lowercase();
  lower.contains("timeout")
    || lower.contains("timed out")
    || lower.contains("disconnected")
    || lower.contains("reply dropped")
    || lower.contains("no warm cast worker")
    || lower.contains("no active session")
    || lower.contains("not connected")
    || lower.contains("reconnect backoff")
    || lower.contains("broken pipe")
    || lower.contains("connection")
    || lower.contains("connect ")
}

/// Why a FLAC LIVE session counted a failure strike (unit-tested with [`flac_strike_decision`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FlacStrikeReason {
  /// Pool re-LOAD recovery (IDLE ERROR path → internal re-LOAD).
  MediaRecovered,
  /// Bridge stall watchdog issued a generation-scoped re-LOAD.
  StallWatchdogReload,
}

/// Outcome of recording a FLAC failure strike against optional previous strike state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FlacStrikeDecision {
  /// First strike, or second strike outside the pair window (keep latest as previous).
  RecordOnly,
  /// Second strike within the pair window → switch session to WAV and remember fallback.
  FallbackToWav,
}

/// Pure FLAC strike accounting: timestamps + reasons in, decision + updated previous out.
///
/// Two strikes within `pair_window` → [`FlacStrikeDecision::FallbackToWav`]; otherwise
/// [`FlacStrikeDecision::RecordOnly`] and the new strike becomes the previous for the next call.
fn flac_strike_decision(
  previous: Option<(Instant, FlacStrikeReason)>,
  now: Instant,
  reason: FlacStrikeReason,
  pair_window: Duration,
) -> (FlacStrikeDecision, Option<(Instant, FlacStrikeReason)>) {
  let decision = match previous {
    Some((prev_at, _)) if now.saturating_duration_since(prev_at) < pair_window => FlacStrikeDecision::FallbackToWav,
    Some(_) | None => FlacStrikeDecision::RecordOnly,
  };
  (decision, Some((now, reason)))
}

/// Apply a FLAC strike on a Playing session; returns whether pair-window fallback is due.
pub(super) fn apply_flac_strike(
  session: &mut PlayingSession,
  now: Instant,
  reason: FlacStrikeReason,
) -> FlacStrikeDecision {
  let previous = session.flac_strike;
  let (decision, updated) = flac_strike_decision(previous, now, reason, FLAC_STRIKE_PAIR_WINDOW);
  session.flac_strike = updated;
  decision
}

/// Plan from [`Bridge::on_media_recovered`] locked section when FLAC LIVE needs WAV fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MediaRecoveredPlan {
  /// Inside the early decisive window (one recovery is enough).
  EarlyImmediate { generation: u64 },
  /// Second strike within [`FLAC_STRIKE_PAIR_WINDOW`].
  StrikeFallback {
    generation: u64,
    first_strike: Option<FlacStrikeReason>,
  },
}

impl Bridge {
  pub(super) fn remember_flac_fallback(&self, device_id: &str) {
    let mut set = self.flac_fallback.lock();
    if set.insert(device_id.to_owned()) {
      tracing::info!(
        %device_id,
        "remembering WAV BUFFERED fallback for device (FLAC rejected or early stall)"
      );
    }
    drop(set);
  }

  /// Pool re-LOAD recovered media (IDLE ERROR recovery path).
  ///
  /// Early FLAC window → immediate WAV fallback. Outside the window, each recovery is a FLAC
  /// strike; two strikes within [`FLAC_STRIKE_PAIR_WINDOW`] switch the LIVE session to WAV and
  /// remember process-lifetime fallback for the device.
  pub async fn on_media_recovered(self: &Arc<Self>, cast_key: &str) {
    let owner = self.owner_for_cast_key(cast_key).unwrap_or_else(|| cast_key.to_owned());
    let Some(plan) = self.plan_media_recovered_flac(&owner) else {
      return;
    };
    self.apply_media_recovered_plan(&owner, plan).await;
  }

  /// Decide whether a pool recovery should fall back FLAC→WAV (updates strike state under lock).
  fn plan_media_recovered_flac(&self, device_id: &str) -> Option<MediaRecoveredPlan> {
    let mut devices = self.devices.lock();
    let Some(slot) = devices.get_mut(device_id) else {
      drop(devices);
      tracing::info!(%device_id, "Cast pool re-LOAD recovered media (no bridge session)");
      return None;
    };
    let plan = match &mut slot.state {
      DeviceState::Playing { generation: live_gen, session } if session.egress == EgressKind::FlacLive => {
        let session_gen = *live_gen;
        if session.started_at.elapsed() < FLAC_FALLBACK_WINDOW {
          Some(MediaRecoveredPlan::EarlyImmediate { generation: session_gen })
        } else {
          let previous = session.flac_strike;
          let decision = apply_flac_strike(session, Instant::now(), FlacStrikeReason::MediaRecovered);
          match decision {
            FlacStrikeDecision::RecordOnly => {
              tracing::info!(
                %device_id,
                generation = session_gen,
                strike = ?FlacStrikeReason::MediaRecovered,
                "Cast pool re-LOAD recovered media; FLAC strike recorded"
              );
              None
            },
            FlacStrikeDecision::FallbackToWav => Some(MediaRecoveredPlan::StrikeFallback {
              generation: session_gen,
              first_strike: previous.map(|(_, r)| r),
            }),
          }
        }
      },
      DeviceState::Idle
      | DeviceState::Starting { .. }
      | DeviceState::Playing { .. }
      | DeviceState::Following { .. } => {
        let live_generation = slot.live_gen();
        tracing::info!(
          %device_id,
          session_generation = ?live_generation,
          "Cast pool re-LOAD recovered media session"
        );
        None
      },
    };
    drop(devices);
    plan
  }

  async fn apply_media_recovered_plan(self: &Arc<Self>, device_id: &str, plan: MediaRecoveredPlan) {
    let (generation, early) = match plan {
      MediaRecoveredPlan::EarlyImmediate { generation } => {
        tracing::warn!(
          %device_id,
          generation,
          "early FLAC media_recovered (IDLE recovery); falling back to WAV BUFFERED"
        );
        (generation, true)
      },
      MediaRecoveredPlan::StrikeFallback { generation, first_strike } => {
        tracing::info!(
          %device_id,
          generation,
          first_strike = ?first_strike,
          second_strike = ?FlacStrikeReason::MediaRecovered,
          "FLAC session hit two failure strikes within pair window; falling back to WAV BUFFERED"
        );
        (generation, false)
      },
    };
    self.remember_flac_fallback(device_id);
    let switched = self.switch_playing_to_wav(device_id, generation).await;
    if !switched {
      let kind = if early { "early" } else { "strike" };
      tracing::debug!(
        %device_id,
        generation,
        kind,
        "FLAC media_recovered fallback did not switch (session gone or already WAV)"
      );
    }
  }

  /// Switch a live FLAC session to WAV BUFFERED (early stall fallback). Returns success.
  pub(super) async fn switch_playing_to_wav(self: &Arc<Self>, device_id: &str, generation: u64) -> bool {
    let switched = {
      let mut devices = self.devices.lock();
      let Some(slot) = devices.get_mut(device_id) else {
        return false;
      };
      let DeviceState::Playing { generation: live_gen, session } = &mut slot.state else {
        return false;
      };
      if *live_gen != generation || session.egress != EgressKind::FlacLive {
        return false;
      }
      session.media.set_content(MediaContent::LiveWav {
        ring: Arc::clone(&session.ring),
        channels: session.channels,
        sample_rate: session.sample_rate,
      });
      session.egress = EgressKind::WavBuffered;
      // Start rollover for WAV if not already running.
      if session.rollover_task.is_none() {
        let (cancel, task) = spawn_rollover_reload_loop(
          Arc::clone(&session.route),
          session.stream_url.clone(),
          Arc::clone(&session.track),
          Arc::clone(&session.pool),
          session.media.rollover_signal(),
          Arc::clone(&session.session_alive),
          Arc::clone(&session.late_stop_allowed),
          Arc::clone(&session.last_volume_linear),
          Arc::clone(&session.inflight_load),
          Arc::clone(&session.reload_flight),
        );
        session.rollover_cancel = Some(cancel);
        session.rollover_task = Some(task);
      }
      session.last_stall_reload_at = Some(Instant::now());
      drop(devices);
      true
    };
    if !switched {
      return false;
    }
    self
      .reload_playing_media(device_id, generation, "flac_to_wav_fallback", None)
      .await
      == ReloadOutcome::Loaded
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::audio::PcmRing;
  use crate::bridge::state::select_egress;
  use crate::bridge::test_support::{insert_playing, test_playing_session};
  use crate::cast::CastControl;
  use crate::cast::CastPool;
  use crate::media::MediaServer;
  use crate::registry::DeviceRegistry;

  #[test]
  fn flac_fallback_memory_skips_flac_next_select() {
    let bridge = Bridge::new(Arc::new(DeviceRegistry::new()), Arc::new(CastPool::new(None)));
    assert_eq!(
      select_egress(bridge.flac_fallback.lock().contains("nest")),
      EgressKind::FlacLive
    );
    bridge.remember_flac_fallback("nest");
    assert_eq!(
      select_egress(bridge.flac_fallback.lock().contains("nest")),
      EgressKind::WavBuffered
    );
    // Idempotent remember.
    bridge.remember_flac_fallback("nest");
    assert_eq!(bridge.flac_fallback.lock().len(), 1);
  }

  #[tokio::test]
  async fn on_media_recovered_early_flac_switches_to_wav() {
    let pool: Arc<dyn CastControl> = Arc::new(CastPool::new(None));
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let mut playing = test_playing_session(media, "dev-recov", Arc::clone(&pool), Arc::new(PcmRing::new(2, 64)));
    playing.egress = EgressKind::FlacLive;
    playing.started_at = Instant::now();
    insert_playing(&bridge, "dev-recov", 1, playing);

    bridge.on_media_recovered("dev-recov").await;

    assert!(
      bridge.flac_fallback.lock().contains("dev-recov"),
      "early media_recovered must remember WAV fallback"
    );
    {
      let devices = bridge.devices.lock();
      let DeviceState::Playing { session: live, .. } = &devices.get("dev-recov").expect("slot").state else {
        panic!("expected Playing after media_recovered");
      };
      assert_eq!(live.egress, EgressKind::WavBuffered);
      drop(devices);
    }
    bridge.handle_session_end("dev-recov").await;
  }

  #[tokio::test]
  async fn on_media_recovered_one_late_strike_keeps_flac() {
    let pool: Arc<dyn CastControl> = Arc::new(CastPool::new(None));
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let mut playing = test_playing_session(media, "dev-late", Arc::clone(&pool), Arc::new(PcmRing::new(2, 64)));
    playing.egress = EgressKind::FlacLive;
    // Started well before the early-fallback window.
    playing.started_at = Instant::now()
      .checked_sub(FLAC_FALLBACK_WINDOW + Duration::from_secs(1))
      .unwrap_or_else(Instant::now);
    insert_playing(&bridge, "dev-late", 1, playing);

    bridge.on_media_recovered("dev-late").await;

    assert!(!bridge.flac_fallback.lock().contains("dev-late"));
    {
      let devices = bridge.devices.lock();
      let DeviceState::Playing { session: live, .. } = &devices.get("dev-late").expect("slot").state else {
        panic!("expected Playing");
      };
      assert_eq!(live.egress, EgressKind::FlacLive);
      assert!(
        live.flac_strike.is_some_and(|(_, r)| r == FlacStrikeReason::MediaRecovered),
        "single late media_recovered must record a MediaRecovered strike"
      );
      drop(devices);
    }
    bridge.handle_session_end("dev-late").await;
  }

  #[tokio::test]
  async fn on_media_recovered_two_late_strikes_switch_to_wav() {
    let pool: Arc<dyn CastControl> = Arc::new(CastPool::new(None));
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let mut playing = test_playing_session(media, "dev-late2", Arc::clone(&pool), Arc::new(PcmRing::new(2, 64)));
    playing.egress = EgressKind::FlacLive;
    playing.started_at = Instant::now()
      .checked_sub(FLAC_FALLBACK_WINDOW + Duration::from_secs(5))
      .unwrap_or_else(Instant::now);
    insert_playing(&bridge, "dev-late2", 1, playing);

    bridge.on_media_recovered("dev-late2").await;
    bridge.on_media_recovered("dev-late2").await;

    assert!(
      bridge.flac_fallback.lock().contains("dev-late2"),
      "two late media_recovered strikes must remember WAV fallback"
    );
    {
      let devices = bridge.devices.lock();
      let DeviceState::Playing { session: live, .. } = &devices.get("dev-late2").expect("slot").state else {
        panic!("expected Playing after two late recoveries");
      };
      assert_eq!(live.egress, EgressKind::WavBuffered);
      drop(devices);
    }
    bridge.handle_session_end("dev-late2").await;
  }

  #[tokio::test]
  async fn late_media_recovered_then_stall_strike_switches_to_wav() {
    let pool: Arc<dyn CastControl> = Arc::new(CastPool::new(None));
    let bridge = Arc::new(Bridge::new(Arc::new(DeviceRegistry::new()), Arc::clone(&pool)));
    let media = MediaServer::start("127.0.0.1").await.expect("media");
    let mut playing = test_playing_session(media, "dev-mixed", Arc::clone(&pool), Arc::new(PcmRing::new(2, 64)));
    playing.egress = EgressKind::FlacLive;
    playing.started_at = Instant::now()
      .checked_sub(FLAC_FALLBACK_WINDOW + Duration::from_secs(5))
      .unwrap_or_else(Instant::now);
    // Seed a MediaRecovered strike as if the pool already recovered once.
    playing.flac_strike = Some((Instant::now(), FlacStrikeReason::MediaRecovered));
    insert_playing(&bridge, "dev-mixed", 1, playing);

    // Second strike via stall-watchdog reason (same pure accounting as handle_stall_reload).
    {
      let mut devices = bridge.devices.lock();
      let DeviceState::Playing { session, .. } = &mut devices.get_mut("dev-mixed").expect("slot").state else {
        panic!("expected Playing");
      };
      let previous = session.flac_strike;
      let decision = apply_flac_strike(session, Instant::now(), FlacStrikeReason::StallWatchdogReload);
      assert_eq!(decision, FlacStrikeDecision::FallbackToWav);
      assert_eq!(
        previous.map(|(_, r)| r),
        Some(FlacStrikeReason::MediaRecovered),
        "pair fallback must retain first strike reason for logging"
      );
      drop(devices);
    }
    bridge.remember_flac_fallback("dev-mixed");
    // switch_playing_to_wav returns re-LOAD success; without a Cast worker that is false, but
    // egress still flips under the lock (same as early media_recovered tests).
    let _reload_ok = bridge.switch_playing_to_wav("dev-mixed", 1).await;
    assert!(bridge.flac_fallback.lock().contains("dev-mixed"));
    {
      let devices = bridge.devices.lock();
      let DeviceState::Playing { session: live, .. } = &devices.get("dev-mixed").expect("slot").state else {
        panic!("expected Playing");
      };
      assert_eq!(live.egress, EgressKind::WavBuffered);
      drop(devices);
    }
    bridge.handle_session_end("dev-mixed").await;
  }

  #[test]
  fn transport_shaped_cast_errors_do_not_remember_fallback() {
    let transport = [
      "warm Cast load timed out for nest",
      "warm Cast worker for nest disconnected",
      "no active session",
      "no warm Cast worker for nest",
      "warm Cast device not connected",
      "reconnect backoff active for nest",
      "warm connection channel: broken pipe",
      "connect 192.0.2.1:8009: connection refused",
      "warm Cast load reply dropped for nest",
    ];
    for msg in transport {
      assert!(
        is_transport_shaped_cast_error(&Error::Cast(msg.to_owned())),
        "expected transport: {msg}"
      );
    }
  }

  #[test]
  fn decisive_cast_errors_are_not_transport_shaped() {
    let decisive = [
      "warm media load: unsupported content type",
      "LOAD status had no media session entries",
      "warm media load: invalid request",
    ];
    for msg in decisive {
      assert!(
        !is_transport_shaped_cast_error(&Error::Cast(msg.to_owned())),
        "expected decisive (not transport): {msg}"
      );
    }
  }

  #[test]
  fn flac_strike_first_is_record_only() {
    let t0 = Instant::now();
    let (decision, previous) =
      flac_strike_decision(None, t0, FlacStrikeReason::MediaRecovered, FLAC_STRIKE_PAIR_WINDOW);
    assert_eq!(decision, FlacStrikeDecision::RecordOnly);
    assert_eq!(previous, Some((t0, FlacStrikeReason::MediaRecovered)));
  }

  #[test]
  fn flac_strike_second_within_pair_window_is_fallback() {
    let t0 = Instant::now();
    let t1 = t0 + Duration::from_secs(30);
    let previous = Some((t0, FlacStrikeReason::MediaRecovered));
    let (decision, updated) =
      flac_strike_decision(previous, t1, FlacStrikeReason::StallWatchdogReload, FLAC_STRIKE_PAIR_WINDOW);
    assert_eq!(decision, FlacStrikeDecision::FallbackToWav);
    assert_eq!(updated, Some((t1, FlacStrikeReason::StallWatchdogReload)));
  }

  #[test]
  fn flac_strike_second_after_pair_window_is_record_only() {
    let t0 = Instant::now();
    let t1 = t0 + FLAC_STRIKE_PAIR_WINDOW;
    let previous = Some((t0, FlacStrikeReason::StallWatchdogReload));
    let (decision, updated) =
      flac_strike_decision(previous, t1, FlacStrikeReason::MediaRecovered, FLAC_STRIKE_PAIR_WINDOW);
    assert_eq!(decision, FlacStrikeDecision::RecordOnly);
    assert_eq!(updated, Some((t1, FlacStrikeReason::MediaRecovered)));
  }

  #[test]
  fn flac_strike_exactly_at_pair_window_boundary_resets() {
    // Pair window is exclusive upper bound: elapsed == window → RecordOnly.
    let t0 = Instant::now();
    let t1 = t0 + FLAC_STRIKE_PAIR_WINDOW;
    let (decision, _) = flac_strike_decision(
      Some((t0, FlacStrikeReason::MediaRecovered)),
      t1,
      FlacStrikeReason::MediaRecovered,
      FLAC_STRIKE_PAIR_WINDOW,
    );
    assert_eq!(decision, FlacStrikeDecision::RecordOnly);
  }
}
