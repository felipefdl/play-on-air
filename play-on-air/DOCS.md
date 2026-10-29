# PlayOnAir for Home Assistant OS

Chromecast devices as AirPlay 2 speakers on the local network. Zero setup by default: start the app and Cast devices appear in the iOS/macOS AirPlay picker with their Cast names.

## Install

1. In Home Assistant: **Settings → Apps → Install app**.
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

Two Chromecasts with the same name get distinct AirPlay names without any configuration: the model is appended when it differs (`Kitchen (Nest Audio)`), otherwise a short id (`Kitchen (7e56)`). Names are stable across restarts. A configured AirPlay name is used exactly as written. While a speaker is in Apple Home, its name in the Home app wins (see [Apple Home](#apple-home)).

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

## Apple Home

Add a speaker to the Home app (**+ → Add Accessory → More options…**, then pick the speaker) to assign it to a room and play to it with Siri, by speaker name or by room; your iPhone streams to it. Anyone on the LAN can still AirPlay to it; Home does not restrict access.

While a speaker is in Apple Home, the Home app owns its AirPlay name. Adding it keeps the name it had, renaming it in the Home app renames its AirPlay speaker within a few seconds (after the current stream ends, if one is playing), and renaming the Chromecast in Google Home or an **AirPlay name** set in the Configuration tab leaves it alone. Removing the speaker from Home hands its name back to Google Home or the configured AirPlay name.

Each speaker's pairing identity, the controllers Home registers, and its Home name live in the app's private `/data`, so Home keeps the speakers across restarts and updates, and Home Assistant backups include them. Uninstalling the app deletes them: after reinstalling, remove the speakers from Home and add them again.

## Image

Supervisor pulls the multi-arch image from GHCR using the app version as the tag:

`ghcr.io/felipefdl/play-on-air:0.5.2`

Architectures: `amd64`, `aarch64`.

## Logs

Use the app **Log** tab. Default level is `info` from Configuration (or `RUST_LOG` if you set it outside options).

## Troubleshooting

| Symptom | What to check |
|---------|----------------|
| iOS/macOS AirPlay picker is empty | App is **Started**; host and phone on the same LAN; wait a few seconds after start for discovery; Log tab for discovery errors. |
| Device name wrong or still visible | Configuration **Optional rename / hide** match id is a substring of the real Cast name; app was restarted after save. A suffix such as `(Nest Audio)` or `(7e56)` means another Chromecast has the same name; rename one of them in the Google Home app or set an AirPlay name here. A speaker in Apple Home keeps its name from the Home app: rename it there (the Log shows `AirPlay keeps the name from the Apple Home app` after a Google Home rename). |
| Speaker still shows AirPlay after "Hey Google" | When Google Assistant or another Cast app takes the speaker, the Log shows `Cast ownership lost` and PlayOnAir disconnects the AirPlay client. iOS can take a moment to clear Now Playing. If that line never appears, restart the app. |
| Silent after AirPlaying again (after another Cast app took the speaker) | Each new AirPlay stream starts a fresh Cast session. The Log shows `bridge session Cast load ok` when the speaker accepted it; if it is missing, check the Log for Cast errors around that time. |
| iPhone volume shows 100% but Nest is quieter | PlayOnAir reports the Cast speaker's volume to AirPlay once it can reach the speaker. Disconnect and reconnect AirPlay on the phone to pick up the current value. |
| Speakers selected together drift or one stays silent | The Log shows `updating Cast multizone group` and `Cast multizone PLAYBACK_SESSION_UPDATED` when a speaker joins. `Cast could not add the speaker to the group; it plays standalone` means Google refused that speaker; it then plays its own stream, not in sync. |
| Nest silent while iPhone still Streaming | Cast pause follows real AirPlay pause/flush events; a stalled stream re-loads automatically. |
| Nest Mini / Nest speakers cut out or sound wrong | Keep the host and speakers on a stable LAN (wired host or strong Wi-Fi). Use `info` log level for normal listening; `debug` and `trace` add load. |
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
| Several PlayOnAir speakers | Selected together in the AirPlay picker, they play in sync through Google Cast multizone: the first starts the stream and the others join it. Pause, skip, and seek act on the group; volume stays per speaker; deselecting any speaker (including the first) leaves the rest playing |
| Mixed speakers | PlayOnAir speakers selected together with HomePods or other AirPlay speakers are not in sync with them: Cast cannot take Apple multi-room timing |
| Speaker groups | Temporary Google speaker groups (created when a stream expands to more speakers) are not shown in AirPlay; speaker groups made in the Google Home app are |
| Apple Home status | The Home tile of a PlayOnAir speaker shows **No Response** (it shows what is playing while your iPhone streams to it), with no controls. The home hub connects only to add, remove, or rename the speaker, never to check status, and Apple reserves Home controls for its own devices, TVs, and receivers with Apple's licensed authentication |

## Store assets

Optional `icon.png` and `logo.png` next to `config.yaml`. Specs: [ASSETS.md](ASSETS.md).

## Support

Source and issues: [github.com/felipefdl/play-on-air](https://github.com/felipefdl/play-on-air)
