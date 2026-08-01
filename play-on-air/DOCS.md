# PlayOnAir for Home Assistant OS

Chromecast devices as AirPlay 2 speakers on the local network. Zero setup by default: start the app and Cast devices appear in the iOS/macOS AirPlay picker with their Cast names.

## Install

1. In Home Assistant: **Settings → Add-ons → Add-on store**.
2. Open the three-dot menu → **Repositories**.
3. Add `https://github.com/felipefdl/play-on-air` and wait for the store to refresh.
4. Find **PlayOnAir**, install, then **Start**.

Host networking is enabled in the app config (`host_network: true`). That is required for mDNS discovery (in-process multicast, no Avahi D-Bus), AirPlay advertisement, and Cast control on the LAN. The tradeoff is that the container shares the host network namespace (not an isolated bridge network).

## Optional rename / hide

Config is optional. Product defaults apply when no file is present.

To rename or hide devices, place a TOML file at:

`/addon_configs/<repo_hash>_play_on_air/play-on-air.toml`

(inside the container this is `/config/play-on-air.toml`, via `PLAY_ON_AIR_CONFIG`).

Example:

```toml
[[device]]
id = "Living Room"
name = "TV"

[[device]]
id = "bedroom"
hide = true
```

`id` is a case-insensitive substring of the Cast friendly name or UUID.

## Image

Supervisor pulls the multi-arch image from GHCR using the app version as the tag:

`ghcr.io/felipefdl/play-on-air:0.1.3`

Architectures: `amd64`, `aarch64`.

## Limits

| Fact | Detail |
|------|--------|
| AirPlay 2 only | No AirPlay 1 stack |
| Chromecast only | Google Cast devices; no UPnP / Sonos / DLNA |
| Audio only | No video, screen mirroring, or A/V lip sync |
| LAN process | Same network as the Cast devices; no cloud or accounts |
| Host network | Required for mDNS / AirPlay / Cast |
| Quality path | Decode AirPlay once; Cast hop is lossless (FLAC or WAV/LPCM). Never defaults to MP3/AAC for Cast egress |

## Logs

Use the Add-on **Log** tab. Default log level is `info` (`RUST_LOG` / tracing env filter when set).

## Support

Source and issues: [github.com/felipefdl/play-on-air](https://github.com/felipefdl/play-on-air)
