<br/>
<p align="center">
  <img src="play-on-air/logo.png" width="200px" alt="PlayOnAir"></img>
</p>

# PlayOnAir

Chromecast devices as AirPlay 2 speakers on the local network.

PlayOnAir discovers Google Cast devices, exposes each as an **AirPlay 2** receiver, and bridges audio from iPhone, iPad, or Mac to the matching Chromecast. One process on your LAN. No accounts, no cloud, no telemetry.

---

## Home Assistant OS

1. **Settings → Add-ons → Add-on store → ⋮ → Repositories**
2. Add `https://github.com/felipefdl/play-on-air`
3. Install **PlayOnAir**, then **Start**

Zero setup: Cast devices appear in the AirPlay picker under their Cast names.

The app uses **host network** (`host_network: true`) so mDNS discovery, AirPlay advertisement, and Cast control work on the LAN. That shares the host network namespace with the container (not an isolated bridge). Details: [play-on-air/DOCS.md](play-on-air/DOCS.md).

### Configuration tab

In the app UI you can set:

- **Log level:** `info` by default (`trace` / `debug` / `info` / `warn` / `error`)
- **Devices:** optional list to **rename** or **hide** Chromecasts in the AirPlay picker

**Optional rename / hide** list stays empty for normal use (Chromecasts are discovered automatically — you do not register devices there). Full field reference: [play-on-air/DOCS.md](play-on-air/DOCS.md).

## Container image

| Image | Notes |
|-------|--------|
| `ghcr.io/felipefdl/play-on-air:0.2.3` | Version tag |
| `ghcr.io/felipefdl/play-on-air:latest` | Latest build from `main` |
| `ghcr.io/felipefdl/play-on-air:sha-<short>` | Immutable short SHA |

Architectures: `linux/amd64`, `linux/arm64`.

Standalone run (host network required for mDNS):

```bash
docker run --rm --network host \
  -e PLAY_ON_AIR_CONFIG=/config/play-on-air.toml \
  -v "$PWD/config:/config" \
  ghcr.io/felipefdl/play-on-air:0.2.3
```

---

## Quick start (developers)

```bash
# From the repository root (Rust 1.88+):
cargo run -p play-on-air

# Or after install:
play-on-air
```

No config file and no flags are required. PlayOnAir discovers Chromecasts on the LAN, advertises each as an AirPlay 2 speaker, and bridges audio when you pick one from iPhone, iPad, or Mac.

| Platform | Discovery backend |
|----------|-------------------|
| macOS | system `dns-sd` (Bonjour) |
| Linux | in-process `mdns-sd` (multicast UDP; no Avahi daemon) |

**macOS:** keep the process running on a machine on the same LAN as the Chromecasts and the iPhone. On the iPhone Control Center AirPlay list, look for names matching your Cast devices (for example speaker names from the Google Home app).

macOS requires the **Local Network** permission for the app that hosts `play-on-air` (Terminal, iTerm, your IDE, and so on). Without it, mDNS discovery still works (it goes through the system daemon) but every Cast TCP connection fails with `No route to host`. Grant access in **System Settings → Privacy & Security → Local Network**. If the hosting app is not listed yet, run `play-on-air` once from Terminal.app so macOS can show the permission prompt.

**Linux / HAOS:** use host networking so mDNS multicast reaches the LAN. No Avahi package is required.

## Optional config (CLI / non-HA)

Optional TOML may **rename** or **hide** devices. It is never required to play audio.

Path resolution:

1. `--config PATH`
2. `$PLAY_ON_AIR_CONFIG`
3. `./play-on-air.toml`

Missing file → product defaults (identity name map, nothing hidden).

```toml
# Example only: rename this file or point --config at it.
# [[device]]
# id = "Living Room"
# name = "TV"
#
# [[device]]
# id = "bedroom"
# hide = true
```

Match `id` is a case-insensitive substring of the Cast friendly name or UUID.

On Home Assistant, prefer the **Configuration** tab instead of hand-editing the generated file.

## Limits

| Fact | Detail |
|------|--------|
| AirPlay 2 only | No AirPlay 1 stack |
| Chromecast only | Google Cast devices; no UPnP / Sonos / DLNA |
| Audio only | No video, screen mirroring, or A/V lip sync |
| LAN process | Same network as the Cast devices; no cloud or accounts |
| Quality path | Decode AirPlay once; Cast hop is lossless FLAC over a live stream, with a WAV fallback when a device rejects FLAC. Never defaults to MP3/AAC for Cast egress |
| Timing | No multi-room clock sync promises across vendors |
| Cast steal | If Google Assistant, YouTube, or another Cast app takes the speaker, PlayOnAir ends the bridge and disconnects AirPlay clients so the phone leaves Now Playing |
| AirPlay supersede | If another phone AirPlays to the same speaker, the new stream wins; prior audio is aborted (iOS may take a moment to clear Now Playing) |

## Commands

```bash
just check          # fmt --check, clippy -D warnings, tests, deny, audit, machete
just fmt
just lint
just test
cargo run -p play-on-air
cargo run -p play-on-air -- --config ./play-on-air.toml
```

Without `just`:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

## Security and contributing

- [SECURITY.md](SECURITY.md): LAN-only model, host network tradeoff, reporting issues
- [CONTRIBUTING.md](CONTRIBUTING.md): MSRV, quality gate, conventions

## Sponsors

If PlayOnAir is useful, you can [sponsor the maintainer on GitHub](https://github.com/sponsors/felipefdl).

## License

PlayOnAir first-party code is **MIT** (Copyright (c) 2026 Felipe Lima). The vendored AirPlay stack under `vendor/shairplay/` is **LGPL-3.0-or-later** and is included in the binary and Docker images. Full terms and third-party notes: [LICENSE.md](LICENSE.md).
