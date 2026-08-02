# Changelog

## 0.1.11

- After Cast load, ignore stale AirPlay pause events (grace window + ring still has PCM) so re-cast after Hey Google steal is not immediately Cast-PAUSEd into silence.

## 0.1.10

- Hard kick after Cast steal: vendored shairplay aborts live RTSP + AP2 session tasks on `stop()` (iPhone Now Playing actually drops).
- Kick lock: maintain loop cannot re-advertise a speaker mid-kick.
- Honest `ss -K` logging (no false success from column headers).

## 0.1.9

- Kick fix: after Cast steal, call `RaopServer::stop` and force-close live RTSP TCP sockets on the RAOP port (shairplay stop alone left iPhone Now Playing connected). Brief pause before re-advertise.

## 0.1.8

- Cast ownership: probe **media** status (with transport CONNECT), not only receiver apps — Google/YouTube steal is detected when the session goes IDLE interrupted or disappears.
- Ownership kick fires when warm TCP dies mid-bridge as well.
- HA Configuration: rename list to **Optional rename / hide** (`device_overrides`); empty = auto-discover, do not register devices.
- Deploy: docker workflow creates/updates GitHub Release `vX.Y.Z` in lockstep with GHCR; document ship flow in AGENTS.md.

## 0.1.7

- Cast ownership watch: when another Cast app takes the speaker (Assistant, YouTube, native Cast), end the LiveWav bridge and kick AirPlay clients so the phone leaves Now Playing.
- AirPlay supersede logging: new exclusive audio session logs at info when the stack aborts a prior stream (no `max_clients=1`; multi-connection iOS stays supported).

## 0.1.6

- Home Assistant **Configuration** tab: `log_level` and optional `devices` (rename / hide).
- Container entrypoint reads `/data/options.json`, sets `RUST_LOG`, and regenerates `play-on-air.toml` on each start (HA options are the source of truth).
- User docs polish (install, Configuration fields, troubleshooting). Asset guidance for `icon.png` / `logo.png`.

## 0.1.5

- Volume: do not force Cast receiver to 100% on stream start; leave device volume alone.
- LiveWav: stop injecting silence on underrun (was constant Nest Mini cuts); wait for real PCM.
- LiveWav: pace progressive pull so Nest cannot drain the ring faster than AirPlay fills it.
- LiveWav: drop 0.5s silence preroll (Nest was playing it as the start of the track).
- Pause idle window increased to 750ms to avoid PAUSE/PLAY thrash on short gaps.

## 0.1.4

- Pause/resume: detect AirPlay playout idle and issue Cast PAUSE/PLAY (snappier stop).
- Flush: clear ring and pause Cast so Nest does not keep playing stale audio.
- LiveWav underrun: feed silence instead of stalling the HTTP body (Nest Mini hickups).

## 0.1.3

- End Cast bridge only when the **audio** session drops, not on every RTSP
  disconnect (Remote Control teardown was killing multi-speaker after seconds).
- Start one process-global AP2 PTP sink (UDP 319/320) shared by all receivers.

## 0.1.2

- Ignore noisy `mdns-sd` ServiceRemoved events that were withdrawing AirPlay ads
  every few seconds (iOS stayed on Remote Control only and never started audio).
- Device departures still use stale TTL when re-resolve stops.

## 0.1.1

- Linux Chromecast discovery uses in-process mDNS (`mdns-sd`) instead of `avahi-browse`.
  Containers no longer need Avahi D-Bus; host network + multicast is enough.
- Slimmer runtime image (dropped `avahi-utils`).

## 0.1.0

- First Home Assistant OS app packaging for PlayOnAir.
- Multi-arch container image on GHCR (`amd64`, `aarch64`).
- Zero-setup default; optional `play-on-air.toml` on the addon config share for rename/hide.
