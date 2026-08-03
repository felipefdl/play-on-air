# PlayOnAir for Home Assistant OS

Chromecast devices as AirPlay 2 speakers on the local network. Zero setup by default: start the app and Cast devices appear in the iOS/macOS AirPlay picker with their Cast names.

## Install

1. In Home Assistant: **Settings → Add-ons → Add-on store**.
2. Open the three-dot menu → **Repositories**.
3. Add `https://github.com/felipefdl/play-on-air` and wait for the store to refresh.
4. Find **PlayOnAir**, install, then **Start**.

Host networking is enabled in the app config (`host_network: true`). That is required for mDNS discovery (in-process multicast, no Avahi D-Bus), AirPlay advertisement, and Cast control on the LAN. The tradeoff is that the container shares the host network namespace (not an isolated bridge network).

## Configuration tab

Open the app → **Configuration**. Everything here is optional. Discovery and playback work with empty defaults — **you do not register Chromecasts in this tab**.

| Field | Default | Purpose |
|-------|---------|---------|
| **Log level** | `info` | Verbosity for the Log tab (`trace`, `debug`, `info`, `warn`, `error`). Prefer `info` day to day. |
| **Optional rename / hide** | empty list | Cosmetics only. Leave empty unless you need a custom AirPlay name or want to hide a speaker. |

### Optional rename / hide (not a device registry)

Chromecasts appear automatically via mDNS. This list is **not** an inventory of speakers to add.

Only add a row when you want to:

- **Rename** how a speaker shows in the AirPlay picker, or
- **Hide** a Chromecast so it is not advertised as AirPlay at all

| Field | Required | Meaning |
|-------|----------|---------|
| **Match id** | yes | Case-insensitive substring of the Cast friendly name or UUID |
| **AirPlay name** | no | Name shown in the AirPlay picker; leave empty to keep the Cast name |
| **Hide from AirPlay** | no | When enabled, do not advertise this device as AirPlay |

Examples:

- Match id `Living Room`, AirPlay name `TV` → picker shows **TV**.
- Match id `bedroom`, Hide enabled → that speaker never appears in AirPlay.

Save, then **Restart** the app so the entrypoint reloads options.

### How options become runtime config

On every container start the entrypoint:

1. Reads `/data/options.json` (what you set in the Configuration tab).
2. Sets `RUST_LOG` from **Log level** unless `RUST_LOG` is already set in the environment.
3. Writes `$PLAY_ON_AIR_CONFIG` (default `/config/play-on-air.toml`) from the **Optional rename / hide** list.
4. Starts `play-on-air --config …`.

**Home Assistant Configuration is the source of truth.** The generated TOML is overwritten on each start. Do not hand-edit that file while using the Configuration tab.

Empty **Optional rename / hide** → product defaults (identity names, nothing hidden). Missing config is never an error.

Advanced path (without the UI): the same TOML format can live at `/config/play-on-air.toml` when not using generated options, for example:

```toml
[[device]]
id = "Living Room"
name = "TV"

[[device]]
id = "bedroom"
hide = true
```

## Image

Supervisor pulls the multi-arch image from GHCR using the app version as the tag:

`ghcr.io/felipefdl/play-on-air:0.2.2`

Architectures: `amd64`, `aarch64`.

## Logs

Use the app **Log** tab. Default level is `info` from Configuration (or `RUST_LOG` if you set it outside options).

## Troubleshooting

| Symptom | What to check |
|---------|----------------|
| iOS/macOS AirPlay picker is empty | App is **Started**; host and phone on the same LAN; wait a few seconds after start for discovery; Log tab for discovery errors. |
| Device name wrong or still visible | Configuration **Optional rename / hide** match id is a substring of the real Cast name; app was restarted after save. |
| Ghost AirPlay after Hey Google | Use 0.1.10+; Log should show `Cast ownership lost` then kick. Restart app if still on an older image. |
| Silent after re-cast (post steal) | Use 0.1.11+; Log should show Cast load ok without an immediate `AirPlay paused` right after. |
| iPhone volume shows 100% but Nest is quieter | Use 0.1.12+; AirPlay reported volume is seeded from Cast. Reconnect after app update. |
| Nest silent while iPhone still Streaming | Cast pause follows real AirPlay pause/flush events; a stalled stream re-loads automatically. |
| Nest Mini / Nest speakers cut out or sound wrong | Use a current image (playback pacing and underrun handling are fixed). Prefer stable LAN Wi‑Fi; avoid forcing extreme log levels during normal listening. |
| Nothing after install | Confirm `host_network: true` is still set in the published app config; do not run PlayOnAir on a bridge-only network without multicast. |
| Config changes ignored | Restart the app after saving Configuration. |

## Limits

| Fact | Detail |
|------|--------|
| AirPlay 2 only | No AirPlay 1 stack |
| Chromecast only | Google Cast devices; no UPnP / Sonos / DLNA |
| Audio only | No video, screen mirroring, or A/V lip sync |
| LAN process | Same network as the Cast devices; no cloud or accounts |
| Host network | Required for mDNS / AirPlay / Cast |
| Quality path | Decode AirPlay once; Cast hop is lossless (FLAC or WAV/LPCM). Never defaults to MP3/AAC for Cast egress |
| Cast steal | If Google Assistant, YouTube, or another Cast app takes the speaker while PlayOnAir was bridging, the bridge ends and AirPlay clients are disconnected so the phone leaves Now Playing |
| AirPlay supersede | If another phone AirPlays to the same speaker, the new stream wins; prior audio is aborted (iOS may take a moment to clear Now Playing) |

## Store assets

Optional `icon.png` and `logo.png` next to `config.yaml`. Specs: [ASSETS.md](ASSETS.md).

## Support

Source and issues: [github.com/felipefdl/play-on-air](https://github.com/felipefdl/play-on-air)
