# PlayOnAir

Chromecast devices as AirPlay 2 speakers on the local network.

See [VISION.md](VISION.md) for product intent and non-goals.

---

## Quick start

```bash
# From the repository root (Rust 1.88+):
cargo run -p play-on-air

# Or after install:
play-on-air
```

No config file and no flags are required. PlayOnAir discovers Chromecasts on the LAN, advertises each as an AirPlay 2 speaker, and bridges audio when you pick one from iPhone, iPad, or Mac.

## Optional config

Optional TOML may **rename** or **hide** devices. It is never required to play audio.

Path resolution:

1. `--config PATH`
2. `$PLAY_ON_AIR_CONFIG`
3. `./play-on-air.toml`

Missing file → product defaults (identity name map, nothing hidden).

```toml
# Example only — rename this file or point --config at it.
# [[device]]
# id = "Living Room"
# name = "TV"
#
# [[device]]
# id = "bedroom"
# hide = true
```

Match `id` is a case-insensitive substring of the Cast friendly name or UUID.

## Limits

| Fact | Detail |
|------|--------|
| AirPlay 2 only | No AirPlay 1 stack |
| Chromecast only | Google Cast devices; no UPnP / Sonos / DLNA |
| Audio only | No video, screen mirroring, or A/V lip sync |
| LAN process | Same network as the Cast devices; no cloud or accounts |
| Quality path | Decode AirPlay once; Cast hop is lossless WAV (continuous live) with FLAC encode kept exercised on session snapshots. Never defaults to MP3/AAC for Cast egress |
| Timing | No multi-room clock sync promises across vendors |

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

## License

MIT — Copyright (c) 2026 Felipe Lima. See [LICENSE.md](LICENSE.md).
