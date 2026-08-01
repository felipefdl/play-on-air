# Changelog

## 0.1.1

- Linux Chromecast discovery uses in-process mDNS (`mdns-sd`) instead of `avahi-browse`.
  Containers no longer need Avahi D-Bus; host network + multicast is enough.
- Slimmer runtime image (dropped `avahi-utils`).

## 0.1.0

- First Home Assistant OS app packaging for PlayOnAir.
- Multi-arch container image on GHCR (`amd64`, `aarch64`).
- Zero-setup default; optional `play-on-air.toml` on the addon config share for rename/hide.
