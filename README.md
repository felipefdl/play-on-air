<br/>
<p align="center">
  <img src="play-on-air/logo.png" width="200px" alt="PlayOnAir"></img>
</p>

# PlayOnAir

Chromecast devices as AirPlay 2 speakers on the local network.

Your Chromecasts show up in the AirPlay picker on iPhone, iPad, and Mac. One process on your LAN. No accounts, no cloud, no telemetry.

PlayOnAir discovers Google Cast devices, advertises each as an AirPlay 2 receiver, and bridges audio to the matching Chromecast. The Cast hop is lossless FLAC when the device accepts it (WAV fallback otherwise), never a default second lossy encode.

---

## Install

Pick one path. All need the same LAN as your Chromecasts. Containers need **host network** so mDNS and Cast control reach the LAN.

### Home Assistant OS

1. **Settings → Add-ons → Add-on store → ⋮ → Repositories**
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

Optional config via env and a volume when you need rename/hide (not required to play):

```bash
docker run --rm --network host \
  -e PLAY_ON_AIR_CONFIG=/config/play-on-air.toml \
  -v "$PWD/config:/config" \
  ghcr.io/felipefdl/play-on-air:latest
```

### Binary (macOS / Linux)

```bash
# From the repository root (Rust 1.88+):
cargo run -p play-on-air

# Or after install:
play-on-air
```

No config file and no flags required. Keep the process running on a machine on the same LAN as the Chromecasts and the phone.

**macOS:** grant **Local Network** access for the app that hosts the binary (Terminal, iTerm, your IDE, and so on) under **System Settings → Privacy & Security → Local Network**. Without it, discovery may work but Cast TCP fails with `No route to host`. If the app is not listed yet, run `play-on-air` once from Terminal.app so macOS can prompt.

## Optional: rename or hide

Never required to play audio. Empty defaults keep Cast names and show every discovered device.

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

## Limits

| | |
|--|--|
| AirPlay 2 only | No AirPlay 1 |
| Chromecast only | No UPnP, Sonos, or generic DLNA |
| Audio only | No video, screen mirroring, or A/V lip sync |
| LAN process | Same network as the Cast devices; no cloud or accounts |
| No multi-room clock sync | Cast cannot take Apple multi-room timing; no fake promises |

**If something else takes the speaker:** Google Assistant, YouTube, or another Cast app ends the bridge and disconnects AirPlay so the phone leaves Now Playing. Another phone AirPlaying to the same speaker wins; prior audio stops (iOS may take a moment to clear Now Playing).

## Security and contributing

- [SECURITY.md](SECURITY.md): LAN-only model, host network tradeoff, reporting issues
- [CONTRIBUTING.md](CONTRIBUTING.md): MSRV, quality gate, conventions

## Sponsors

If PlayOnAir is useful, you can [sponsor the maintainer on GitHub](https://github.com/sponsors/felipefdl).

## License

PlayOnAir first-party code is **MIT** (Copyright (c) 2026 Felipe Lima). The vendored AirPlay stack under `vendor/shairplay/` is **LGPL-3.0-or-later** and is included in the binary and Docker images. Full terms and third-party notes: [LICENSE.md](LICENSE.md).
