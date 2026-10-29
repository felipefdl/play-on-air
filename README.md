<br/>
<p align="center">
  <img src="play-on-air/mark.svg" width="200px" alt="PlayOnAir"></img>
</p>

# PlayOnAir

Chromecast devices as AirPlay 2 speakers on the local network.

Your Chromecasts show up in the AirPlay picker on iPhone, iPad, and Mac. One process on your LAN. No accounts, no cloud, no telemetry.

PlayOnAir discovers Google Cast devices, advertises each as an AirPlay 2 receiver, and bridges audio to the matching Chromecast. The Cast hop is lossless FLAC when the device accepts it (WAV fallback otherwise), never a default second lossy encode.

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
docker run --rm --network host ghcr.io/felipefdl/play-on-air:latest
```

| Image | Notes |
|-------|--------|
| `ghcr.io/felipefdl/play-on-air:latest` | Latest build from `main` |
| `ghcr.io/felipefdl/play-on-air:<version>` | Pinned release (see GitHub Releases) |
| `ghcr.io/felipefdl/play-on-air:sha-<short>` | Immutable short SHA |

Architectures: `linux/amd64`, `linux/arm64`.

Optional config via a volume when you need rename/hide (not required to play). The image reads `/config/play-on-air.toml`, so put the file at `./config/play-on-air.toml`:

```bash
docker run --rm --network host \
  -v "$PWD/config:/config" \
  ghcr.io/felipefdl/play-on-air:latest
```

### Binary (macOS / Linux)

```bash
# From the repository root (Rust 1.88+):
cargo run -p play-on-air

# Or install the binary, then run it:
cargo install --path crates/play-on-air
play-on-air
```

No config file and no flags required. Keep the process running on a machine on the same LAN as the Chromecasts and the phone.

**macOS:** grant **Local Network** access for the app that hosts the binary (Terminal, iTerm, your IDE, and so on) under **System Settings → Privacy & Security → Local Network**. Without it, discovery may work but Cast TCP fails with `No route to host`. If the app is not listed yet, run `play-on-air` once from Terminal.app so macOS can prompt.

## Optional: rename or hide

Never required to play audio. Empty defaults keep Cast names and show every discovered device.

Two Chromecasts with the same name get distinct AirPlay names: the model is appended when it differs (`Kitchen (Nest Audio)`), otherwise a short id (`Kitchen (7e56)`). Names are stable across restarts. A configured rename is used exactly as written.

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

Add a speaker to the Home app (**+ → Add Accessory → More options…**, then pick the speaker) to assign it to a room. Anyone on the LAN can still AirPlay to it; Home does not restrict access.

PlayOnAir keeps each speaker's pairing identity and the controllers Home registers in a state directory, so Home keeps the speaker across restarts:

| Run | State directory |
|-----|-----------------|
| Home Assistant | The app's private `/data` (included in Home Assistant backups) |
| Container | `/data`. Mount a volume so pairing survives recreating the container: `docker run --network host -v play-on-air-state:/data ghcr.io/felipefdl/play-on-air:latest` |
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
| Apple Home status | A speaker added to Home plays from the AirPlay picker, but its Home tile shows **No Response** (it can show what is playing while your iPhone streams to it), and Siri can refuse to play to it ("select the speaker from the AirPlay menu"). The home hub never connects to third-party AirPlay receivers for status, and Apple reserves Home controls for its own devices, TVs, and receivers with Apple's licensed authentication. shairport-sync has the same limit. |

**Several PlayOnAir speakers at once:** select them together in the AirPlay picker and they play in sync. The first one starts the stream; the others join it through Google Cast multizone, the same mechanism the Google Home app uses to add speakers to a playing stream. Pause, skip, and seek act on the whole group; volume stays per speaker. Deselecting a speaker removes it from the group, including the one that started. Temporary Google speaker groups created this way never appear as extra AirPlay speakers; speaker groups made in the Google Home app still do.

**If something else takes the speaker:** Google Assistant, YouTube, or another Cast app ends the bridge and disconnects AirPlay so the phone leaves Now Playing. Another phone AirPlaying to the same speaker wins; prior audio stops (iOS may take a moment to clear Now Playing).

## More

- [VISION.md](VISION.md): product intent and scope
- [SECURITY.md](SECURITY.md): LAN-only model, host network tradeoff, reporting issues
- [CONTRIBUTING.md](CONTRIBUTING.md): MSRV, quality gate, conventions

## Sponsors

If PlayOnAir is useful, you can [sponsor the maintainer on GitHub](https://github.com/sponsors/felipefdl).

## License

PlayOnAir first-party code is **MIT** (Copyright (c) 2026 Felipe Lima). The vendored AirPlay stack under `vendor/shairplay/` is **LGPL-3.0-or-later** and is included in the binary and Docker images. Full terms and third-party notes: [LICENSE.md](LICENSE.md).
