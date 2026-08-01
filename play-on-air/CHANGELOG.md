# Changelog

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
