# Changelog

## 0.2.3

- Fix: Cast LOAD no longer leaves an active media session after the caller times out (late success is abandoned and stopped).
- Fix: AirPlay pause cancels in-flight Cast re-LOAD recovery; resume no longer fires PLAY while a re-LOAD is in flight.
- Fix: buffered AirPlay map flush/re-anchor no longer drops thousands of packets on RTP timeline jumps.
- Fix: devices with permanent Cast TLS failures (for example some TVs reporting UnsupportedCertVersion) are not advertised as AirPlay sinks and are not reconnect-hammered.
- Logging: quieter false PTP bind warnings; clearer first-time Cast reachability wording.

## 0.2.2

- AirPlay devices advertise a neutral `PlayOnAir1,1` model, so the iOS picker shows a speaker icon instead of an Apple TV.
- macOS: when the Local Network permission is denied (discovery works but every Cast connection fails with "No route to host"), one clear error names the fix; the README documents the permission.

## 0.2.1

- Fix: tearing down a stale AirPlay control connection no longer kills the active audio session (music died seconds after starting on 0.2.0).
- Fix: Nest devices no longer vanish from the AirPlay picker for ~44 s on spurious mDNS removals; on Linux a device leaves only when stale **and** its Cast control plane is unreachable.
- Fix: warm Cast connections no longer churn on a 30 s watchdog while idle (drain reads are bounded to their short timeout).

## 0.2.0

- **FLAC to Cast by default**: lossless FLAC over a live chunked stream (`streamType` LIVE). Devices that reject FLAC fall back to WAV automatically.
- **~2 s playback cushion**: silence preroll fills the Cast buffer at start and a maintained lead keeps it there, so brief Wi-Fi or sender hiccups no longer pause playback; the cushion self-heals after stalls.
- iPhone pause, resume, and track-skip flush arrive as real AirPlay 2 events: Cast pauses promptly, resume after a long pause rejoins live instead of replaying stale audio, and skips re-load for a fast track change.
- Mixed 44.1/48 kHz queues no longer kill the session on track boundaries (format changes rebuild the stream in place).
- Cast control recovers instead of disconnecting: reconnects with backoff, a single failed heartbeat no longer kicks the iPhone, media errors and stuck buffering re-load the stream, and a stall watchdog restarts a dead pull.
- Discovery blips no longer tear down live playback (debounced removal on macOS and Linux, live sessions are guarded); devices that really leave withdraw within minutes instead of 24 h.
- Mac system audio (realtime ALAC): packet reorder and loss concealment keep the clock steady on busy Wi-Fi.
- Playout runs on a monotonic clock, immune to NTP steps on the host.
- One termination signal stops the process cleanly; a second forces exit.
- License notices for the vendored LGPL-3.0 AirPlay stack; macOS CI job covers the platform-specific discovery code.

## 0.1.14

- Stop Cast-PAUSE on PCM idle/underrun (iPhone could keep Streaming while Nest stayed paused forever). Cast PAUSE only on explicit AirPlay FLUSH; PCM again resumes Cast.

## 0.1.13

- Idle Cast PAUSE only after **2.5s** without PCM **and** an empty ring (stops mid-track PAUSE/PLAY thrash on buffered AP2). Explicit pause/FLUSH still pauses promptly.

## 0.1.12

- AirPlay volume UI: report Cast/Nest level on `GET_PARAMETER` (no hardcoded 100%); seed from warm Cast and after LOAD. Still preserve device volume on stream start.

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
