//! Sender volume in a multi-select session (`docs/ap2-receiver-spec.md` section 6,
//! "Volume in multi-select groups").
//!
//! Apple's receiver applies the last `SET_PARAMETER volume` and ignores one whose slider
//! position is within 0.01 of the current volume. In a multi-select session the sender keeps a
//! per-room volume that only the receiver's `dvlc` report updates; without it the iPhone keeps
//! re-asserting its stored room volume between the live drag values. So for multi-select
//! sessions every applied sender volume is acknowledged with `dvlc`, throttled to one report per
//! [`VOLUME_ACK_INTERVAL`], and the last value of a burst is always reported.

use std::time::{Duration, Instant};

use super::connection::AIRPLAY_VOLUME_DB_MIN;
use super::remote::db_to_slider;

/// Slider distance under which a new volume is not noticeably different (Apple's
/// `APSVolumeSliderValuesAreNoticeablyDifferent`).
const SLIDER_DEAD_BAND: f32 = 0.01;

/// At most one `dvlc` acknowledgement per this interval.
pub(crate) const VOLUME_ACK_INTERVAL: Duration = Duration::from_millis(250);

/// Whether `next_db` moves the slider at least 0.01 away from `current_db`.
pub(crate) fn noticeably_different(current_db: f32, next_db: f32) -> bool {
    (db_to_slider(next_db) - db_to_slider(current_db)).abs() >= SLIDER_DEAD_BAND
}

/// `dvlc` fields for AirPlay `volume_db`: slider position, and mute for the `-144` sentinel.
pub(crate) fn db_to_slider_and_mute(volume_db: f32) -> (f32, bool) {
    (db_to_slider(volume_db), volume_db <= AIRPLAY_VOLUME_DB_MIN)
}

/// What to do with the `dvlc` acknowledgement after an applied volume or a timer.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum AckStep {
    /// Report this volume (dB) now.
    Send(f32),
    /// Run [`VolumeAck::fire`] at this instant.
    Arm(Instant),
    /// Nothing to do (a timer is already armed, or nothing is pending).
    Idle,
}

/// Throttle for `dvlc` acknowledgements of sender volume: the first goes out at once, later
/// ones at most every [`VOLUME_ACK_INTERVAL`], and the newest pending value always goes out.
#[derive(Debug, Default)]
pub(crate) struct VolumeAck {
    last_sent: Option<Instant>,
    pending: Option<f32>,
    armed: bool,
}

impl VolumeAck {
    /// The receiver applied sender volume `volume_db` at `now`.
    pub(crate) fn applied(&mut self, volume_db: f32, now: Instant) -> AckStep {
        match self.next_allowed(now) {
            None => {
                self.pending = None;
                self.last_sent = Some(now);
                AckStep::Send(volume_db)
            }
            Some(at) => {
                self.pending = Some(volume_db);
                if self.armed {
                    AckStep::Idle
                } else {
                    self.armed = true;
                    AckStep::Arm(at)
                }
            }
        }
    }

    /// The timer armed by an earlier [`AckStep::Arm`] fired at `now`.
    pub(crate) fn fire(&mut self, now: Instant) -> AckStep {
        let Some(volume_db) = self.pending else {
            self.armed = false;
            return AckStep::Idle;
        };
        if let Some(at) = self.next_allowed(now) {
            return AckStep::Arm(at);
        }
        self.pending = None;
        self.armed = false;
        self.last_sent = Some(now);
        AckStep::Send(volume_db)
    }

    /// When the next report may go out, if not yet at `now`.
    fn next_allowed(&self, now: Instant) -> Option<Instant> {
        let at = self.last_sent? + VOLUME_ACK_INTERVAL;
        (now < at).then_some(at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn slider_maps_minus_30_to_0_db_linearly() {
        assert!((db_to_slider(0.0) - 1.0).abs() < f32::EPSILON);
        assert!((db_to_slider(-15.0) - 0.5).abs() < f32::EPSILON);
        assert!(db_to_slider(-30.0).abs() < f32::EPSILON);
        assert!(db_to_slider(AIRPLAY_VOLUME_DB_MIN).abs() < f32::EPSILON);
        assert!((db_to_slider(3.0) - 1.0).abs() < f32::EPSILON);
        assert!(db_to_slider(f32::NAN).abs() < f32::EPSILON);
    }

    #[test]
    fn dead_band_is_one_hundredth_of_the_slider() {
        // 0.01 slider is 0.3 dB.
        assert!(!noticeably_different(-21.0, -21.0));
        assert!(!noticeably_different(-21.0, -20.8));
        assert!(!noticeably_different(-21.0, -21.2));
        assert!(noticeably_different(-21.0, -20.6));
        assert!(noticeably_different(-21.0, -10.0));
        // Below -30 dB everything is slider 0, mute included.
        assert!(!noticeably_different(-30.0, AIRPLAY_VOLUME_DB_MIN));
        assert!(noticeably_different(-29.0, AIRPLAY_VOLUME_DB_MIN));
    }

    #[test]
    fn first_ack_goes_out_at_once() {
        let t0 = Instant::now();
        let mut ack = VolumeAck::default();
        assert_eq!(ack.applied(-10.0, t0), AckStep::Send(-10.0));
        assert_eq!(ack.applied(-11.0, t0 + VOLUME_ACK_INTERVAL), AckStep::Send(-11.0));
    }

    #[test]
    fn burst_is_throttled_and_its_last_value_is_sent() {
        let t0 = Instant::now();
        let mut ack = VolumeAck::default();
        assert_eq!(ack.applied(-10.0, t0), AckStep::Send(-10.0));
        let deadline = t0 + VOLUME_ACK_INTERVAL;
        assert_eq!(ack.applied(-11.0, t0 + 10 * MS), AckStep::Arm(deadline));
        assert_eq!(ack.applied(-12.0, t0 + 20 * MS), AckStep::Idle);
        assert_eq!(ack.applied(-13.0, t0 + 200 * MS), AckStep::Idle);
        // The trailing report carries the newest value.
        assert_eq!(ack.fire(deadline), AckStep::Send(-13.0));
        // The next value inside the new window arms again from the trailing send.
        assert_eq!(
            ack.applied(-14.0, deadline + 10 * MS),
            AckStep::Arm(deadline + VOLUME_ACK_INTERVAL)
        );
        assert_eq!(ack.fire(deadline + VOLUME_ACK_INTERVAL), AckStep::Send(-14.0));
        assert_eq!(ack.fire(deadline + 2 * VOLUME_ACK_INTERVAL), AckStep::Idle);
    }

    #[test]
    fn early_timer_rearms_and_late_values_bypass_it() {
        let t0 = Instant::now();
        let mut ack = VolumeAck::default();
        assert_eq!(ack.applied(-10.0, t0), AckStep::Send(-10.0));
        assert_eq!(ack.applied(-11.0, t0 + 10 * MS), AckStep::Arm(t0 + VOLUME_ACK_INTERVAL));
        // A value after the window goes out directly; the stale timer then finds nothing.
        assert_eq!(ack.applied(-12.0, t0 + 300 * MS), AckStep::Send(-12.0));
        assert_eq!(ack.applied(-13.0, t0 + 310 * MS), AckStep::Idle);
        // The old timer fires before the new window ends: it re-arms instead of sending.
        assert_eq!(
            ack.fire(t0 + 320 * MS),
            AckStep::Arm(t0 + 300 * MS + VOLUME_ACK_INTERVAL)
        );
        assert_eq!(ack.fire(t0 + 550 * MS), AckStep::Send(-13.0));
        assert_eq!(ack.fire(t0 + 600 * MS), AckStep::Idle);
    }
}
