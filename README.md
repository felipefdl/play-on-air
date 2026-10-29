<br/>
<p align="center">
  <img src="play-on-air/mark.svg" width="200px" alt="PlayOnAir"></img>
</p>

# PlayOnAir

AirPlay 2 for your Google Cast speakers.

Nest speakers, Chromecasts, and TVs with Chromecast built-in show up in the AirPlay picker on iPhone, iPad, and Mac. Play to one, or select several and they play in sync. Add them to the Apple Home app, put them in rooms, and ask Siri to play there.

- **Instant controls.** Pause and switching to another output reach the speaker in about a tenth of a second; skip, seek, and resume start in well under a second. The speaker still keeps 8 s of audio queued, so Wi-Fi hiccups don't interrupt playback.
- **Lossless to the speaker.** AirPlay audio is decoded once and sent to Cast as FLAC (WAV when a device rejects FLAC), never re-encoded to a lossy format.
- **Multi-room in sync.** Speakers selected together play through Google Cast multizone, the same mechanism the Google Home app uses.
- **Apple Home and Siri.** Assign speakers to rooms and play to them by name or by room.
- **Zero setup.** Install and start. No config file, no accounts, no cloud, no telemetry. Runs as a Home Assistant app, a Docker image, or a single binary.

---

## Install

Pick one path. All need the same LAN as your Chromecasts. Containers need **host network** so mDNS and Cast control reach the LAN.

### Home Assistant OS

1. **Settings → Apps → Install app → ⋮ → Repositories**
2. Add `https://github.com/felipefdl/play-on-air`
3. Install **PlayOnAir**, then **Start**

Cast devices appear in the AirPlay picker under their Cast names. App details: [play-on-air/DOCS.md](play-on-air/DOCS.md).

### Container image

```bash
docker run --rm --network host -v play-on-air-state:/data ghcr.io/felipefdl/play-on-air:latest
```

The `play-on-air-state` volume keeps each speaker's AirPlay identity and Apple Home pairing when the container is recreated.

| Image | Notes |
|-------|--------|
| `ghcr.io/felipefdl/play-on-air:latest` | Latest release |
| `ghcr.io/felipefdl/play-on-air:<version>` | Pinned release (see GitHub Releases) |

Architectures: `linux/amd64`, `linux/arm64`.

Optional config via a volume when you need rename/hide (not required to play). The image reads `/config/play-on-air.toml`, so put the file at `./config/play-on-air.toml`:

```bash
docker run --rm --network host \
  -v play-on-air-state:/data \
  -v "$PWD/config:/config" \
  ghcr.io/felipefdl/play-on-air:latest
```

### Binary (macOS / Linux)

```bash
# From the repository root (Rust 1.99.1+):
cargo run -p play-on-air

# Or install the binary, then run it:
cargo install --path crates/play-on-air
play-on-air
```

No config file and no flags required. Keep the process running on a machine on the same LAN as the Chromecasts and the phone.

**macOS:** grant **Local Network** access for the app that hosts the binary (Terminal, iTerm, your IDE, and so on) under **System Settings → Privacy & Security → Local Network**. Without it, discovery may work but Cast TCP fails with `No route to host`. If the app is not listed yet, run `play-on-air` once from Terminal.app so macOS can prompt.

## Using it

Open the AirPlay picker (Control Center, the Music app, or the menu bar on a Mac) and pick a Cast speaker. Pause, skip, seek, and volume work as with any AirPlay speaker; switching to another output stops the speaker at once.

**Several speakers at once:** select them together and they play in sync. The first one starts the stream; the others join it through Google Cast multizone. Pause, skip, and seek act on the whole group; volume stays per speaker. Deselecting a speaker removes it from the group, including the one that started. Temporary Google speaker groups created this way never appear as extra AirPlay speakers; speaker groups made in the Google Home app still do.

**If something else takes the speaker:** Google Assistant, YouTube, or another Cast app ends the bridge and disconnects AirPlay so the phone leaves Now Playing. Another phone AirPlaying to the same speaker wins; prior audio stops (iOS may take a moment to clear Now Playing).

## Optional: rename or hide

Never required to play audio. Empty defaults keep Cast names and show every discovered device.

Two Chromecasts with the same name get distinct AirPlay names: the model is appended when it differs (`Kitchen (Nest Audio)`), otherwise a short id (`Kitchen (7e56)`). Names are stable across restarts. A configured rename is used exactly as written. While a speaker is in Apple Home, its name in the Home app wins (see [Apple Home](#apple-home)).

- **Home Assistant:** Configuration tab (log level, rename, hide). Full field reference: [play-on-air/DOCS.md](play-on-air/DOCS.md).
- **Binary / container:** optional TOML (`--config PATH`, `$PLAY_ON_AIR_CONFIG`, or `./play-on-air.toml`).

```toml
# Example only. Missing file = product defaults.
# [[device]]
# id = "Living Room"
# name = "TV"
#
# [[device]]
# id = "bedroom"
# hide = true
```

`id` is a case-insensitive substring of the Cast friendly name or UUID. On Home Assistant, use the Configuration tab instead of editing the generated file by hand.

## Apple Home

Add a speaker to the Home app (**+ → Add Accessory → More options…**, then pick the speaker) to assign it to a room and play to it with Siri, by speaker name or by room; your iPhone streams to it. Anyone on the LAN can still AirPlay to it; Home does not restrict access.

While a speaker is in Apple Home, the Home app owns its AirPlay name. Adding it keeps the name it had, renaming it in the Home app renames its AirPlay speaker within a few seconds (after the current stream ends, if one is playing), and renaming the Chromecast in Google Home or a configured rename leaves it alone. Removing the speaker from Home hands its name back to Google Home or the configured rename.

PlayOnAir keeps each speaker's pairing identity, the controllers Home registers, and its Home name in a state directory, so Home keeps the speaker across restarts:

| Run | State directory |
|-----|-----------------|
| Home Assistant | The app's private `/data` (included in Home Assistant backups) |
| Container | `/data`: the `play-on-air-state` volume in the commands above |
| Binary | `--state-dir PATH`, `$PLAY_ON_AIR_STATE_DIR`, or the platform default: `$XDG_STATE_HOME/play-on-air` (`~/.local/state/play-on-air`) on Linux, `~/Library/Application Support/play-on-air` on macOS |

Deleting the directory gives every speaker a new identity: remove the speakers from Home and add them again.

## Limits

| | |
|--|--|
| AirPlay 2 only | No AirPlay 1 |
| Chromecast only | No UPnP, Sonos, or generic DLNA |
| Audio only | No video, screen mirroring, or A/V lip sync |
| LAN process | Same network as the Cast devices; no cloud or accounts |
| No mixed-vendor sync | Group PlayOnAir speakers only with other PlayOnAir speakers, or Apple speakers only with Apple speakers. Cast cannot take Apple multi-room timing, so a group that mixes PlayOnAir speakers with HomePods or other AirPlay speakers plays out of sync. |
| Apple Home status | The Home tile of a PlayOnAir speaker shows **No Response** (it shows what is playing while your iPhone streams to it), with no controls. The home hub connects only to add, remove, or rename the speaker, never to check status, and Apple reserves Home controls for its own devices, TVs, and receivers with Apple's licensed authentication. shairport-sync has the same limit. |

## More

- [VISION.md](VISION.md): product intent and scope
- [SECURITY.md](SECURITY.md): LAN-only model, host network tradeoff, reporting issues
- [CONTRIBUTING.md](CONTRIBUTING.md): MSRV, quality gate, conventions

## Sponsors

If PlayOnAir is useful, you can [sponsor the maintainer on GitHub](https://github.com/sponsors/felipefdl).

## License

PlayOnAir first-party code is **MIT** (Copyright (c) 2026 Felipe Lima). The vendored AirPlay stack under `vendor/shairplay/` is **LGPL-3.0-or-later** and is included in the binary and Docker images. Full terms and third-party notes: [LICENSE.md](LICENSE.md).
