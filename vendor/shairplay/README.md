<div align="center">

# shairplay-rust

**Pure Rust AirPlay server library**

[![CI](https://github.com/metaneutrons/shairplay-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/metaneutrons/shairplay-rust/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/shairplay.svg)](https://crates.io/crates/shairplay)
[![docs.rs](https://docs.rs/shairplay/badge.svg)](https://docs.rs/shairplay)
[![License: LGPL-3.0](https://img.shields.io/badge/license-LGPL--3.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)

A complete AirPlay 2 audio receiver as a Rust library, with buffered audio, encrypted transport, HomeKit pairing, and experimental screen mirroring. `#![forbid(unsafe_code)]`.

> **PlayOnAir fork:** the AirPlay 1 stack (RSA / `airport.key`, legacy Ed25519 pairing, SDP ANNOUNCE, DACP remote control) is removed. See [PLAYONAIR.md](PLAYONAIR.md).

**This is a clean-room Rust implementation — not a wrapper or FFI binding.** Every protocol handler, codec, and cryptographic primitive has been reimplemented from scratch in safe Rust. No C code from shairplay, shairport-sync, or any other project is linked or called.

</div>

---

> ⚠️ **Pre-1.0 notice:** This crate is under active development. Minor version bumps (e.g. 0.1 → 0.2) may include breaking API changes. Pin your dependency to a specific minor version (`shairplay = "0.2"`) and review the [CHANGELOG](CHANGELOG.md) before upgrading.

## Features

|  | Feature | Details |
|--|---------|---------|
| 🎵 | **AirPlay 2** | Buffered AAC, SRP-6a pairing, ChaCha20-Poly1305 encrypted RTSP |
| 🔊 | **Multichannel** | 5.1 and 7.1 AAC decode with ITU-R BS.775 mixdown to stereo |
| 🔄 | **Resampling** | Automatic sample rate conversion via rubato |
| 🔐 | **HomeKit pairing** | Transient (PIN 3939) and normal (persistent key storage) |
| 📺 | **Video** | Screen mirroring — work in progress, behind `video` feature gate |
| 🎬 | **HLS Video** | YouTube and other HLS streams — receiver relays URL to app |
| 🌐 | **Cross-platform** | macOS (native Bonjour) + Linux (pure Rust mDNS) |
| 🔒 | **Pure safe Rust** | `#![forbid(unsafe_code)]`, no C code in this crate¹ |
| ⚡ | **Async** | Built on [tokio](https://tokio.rs) |
| 🛎️ | **Error notifications** | Pairing, SETUP, and codec failures surfaced to your app via `AudioHandler::on_error` |

> ¹ The crate itself contains no `unsafe` code. On macOS, mDNS service registration uses [astro-dnssd](https://crates.io/crates/astro-dnssd), which internally calls Apple's Bonjour C API via FFI. On Linux, [mdns-sd](https://crates.io/crates/mdns-sd) provides a pure Rust mDNS implementation with no native dependencies.

## Quick Start

```rust
use std::sync::Arc;
use shairplay::{RaopServer, AudioHandler, AudioSession, AudioFormat};

struct MyHandler;
impl AudioHandler for MyHandler {
    fn audio_init(&self, format: AudioFormat) -> Box<dyn AudioSession> {
        println!("Stream: {}ch {}bit {}Hz", format.channels, format.bits, format.sample_rate);
        Box::new(MySession)
    }
}

struct MySession;
impl AudioSession for MySession {
    fn audio_process(&mut self, samples: &[f32]) {
        // F32 interleaved PCM — same format for buffered and realtime streams
    }
}

#[tokio::main]
async fn main() -> Result<(), shairplay::ShairplayError> {
    let mut server = RaopServer::builder()
        .name("My Speaker")
        .output_sample_rate(48000) // optional resampling
        .output_max_channels(2) // optional multichannel mixdown
        .build(Arc::new(MyHandler))?;
    server.start().await?;
    tokio::signal::ctrl_c().await.unwrap();
    server.stop().await;
    Ok(())
}
```

## Builder Options

| Method | Default | Feature | Description |
|--------|---------|---------|-------------|
| `.name()` | `"Shairplay"` | | AirPlay display name |
| `.hwaddr()` | random locally administered address | | 6-byte MAC address for mDNS |
| `.port()` | `5000` | | RTSP listening port |
| `.password()` | none | | HTTP Digest auth password |
| `.max_clients()` | `10` | | Maximum concurrent connections |
| `.bind()` | all interfaces | | Bind to specific IPs (multi-interface) |
| `.output_sample_rate()` | source rate | | Resample all audio to this rate |
| `.output_max_channels()` | source channels | | Mix down to this channel count |
| `.pin()` | transient (no PIN) | | PIN for HomeKit pairing |
| `.pairing_store()` | `MemoryPairingStore` | | Persistent key storage |
| `.video_handler()` | none | `video` | Video session factory |
| `.hls_handler()` | none | `hls` | HLS video playback handler |

## Feature Flags

| Flag | Dependencies | Description |
|------|-------------|-------------|
| *(default)* | — | AirPlay 2 audio |
| `video` | aes, cbc, ctr | Legacy (UxPlay) feature set for screen mirroring (`0x527FFEE6`), including the legacy RTP audio it needs |
| `hls` | (implies `video`) | HLS video playback (YouTube, etc.) — receiver relays URL to app |

## Implementation Status

### ✅ AirPlay 2 Audio — Production Ready

Full pipeline: SRP-6a pairing → encrypted RTSP → FairPlay → PTP timing → buffered AAC decode → f32 PCM output. Multichannel 5.1/7.1 with ITU-R BS.775 mixdown. Automatic resampling. Both stream types implemented:

- **Type 103 (buffered)** — AAC over TCP with timed playout buffer. Used for music.
- **Type 96 (realtime)** — ALAC over UDP with immediate delivery. Used for Siri, phone calls, system sounds.

Connections are **instant** (starts streaming in under 50ms) using deterministic key persistence and correct `eventPort` handling on `isRemoteControlOnly` channels, matching commercial receiver capabilities.

### 🧪 Video (Screen Mirroring) — Work in Progress

Behind the `video` feature gate. Screen mirroring uses the legacy/UxPlay-compatible
feature set; AP2 buffered audio is intentionally disabled while video is enabled.

The video feature switches to a UxPlay-compatible legacy feature set
(`0x527FFEE6`) to receive screen mirroring data from iOS 18+. AP2
buffered audio is not available — the iPhone falls back to legacy ALAC
(type 96) which is fully supported with FairPlay key decryption and
NTP timing.

Video decryption is implemented for the legacy feature-set path; AP2+video
hybrid operation remains research work. See [AP2-STATUS.md](AP2-STATUS.md)
for details.

### 🔬 Remote Control

Third-party AP2 receivers cannot send playback commands (play/pause/skip) to the iPhone. All paths require Apple ecosystem trust:

- **Type 130 MRP data channel** — requires HomeKit seed (Home app pairing)
- **Companion-link protocol** — requires same Apple ID
- **DACP** — iPhone doesn't send Active-Remote header in AP2 (the AP1 DACP client is not part of this fork)

See [AP2-STATUS.md](AP2-STATUS.md) for the full research.

## Example Player

The included example plays AirPlay audio through the system's default output device:

```bash
cargo run --example player

# With resampling to match output device rate (e.g. 44100→96000 Hz)
cargo run --example player -- --resample

# With persistent device identity (stable MAC + paired keys across restarts)
cargo run --example player -- --persist state.json

# Custom name and interface binding
cargo run --example player -- --name "Kitchen" --bind 192.168.1.100 --persist state.json --resample

# HLS video (YouTube) — requires mpv installed
cargo run --example player --features hls -- --resample
```

Without `--persist`, the device gets a random MAC each run and the iPhone treats it as a new device. With `--persist`, the MAC and paired keys are saved to a JSON file.

Without `--resample`, audio is delivered at the source's native rate (44100 Hz). If your output device runs at a different rate (e.g. 96000 Hz), use `--resample` to convert in the example app.

## Architecture

``` plain
src/
├── raop/                AirPlay 2 server, RTSP handlers
│   ├── buffered_audio   AP2 timed playout buffer with decrypt/decode/resample
│   ├── realtime_audio   AP2 realtime (type 96) ALAC receiver
│   ├── event_channel    AP2 encrypted event channel
│   ├── rtp, buffer      Legacy RTP audio for screen mirroring (`video`)
│   ├── video            Video handler traits (experimental)
│   └── video_stream     Video stream receiver (experimental)
├── crypto/              Ed25519+Curve25519, FairPlay
│   ├── pairing_homekit  AP2 SRP-6a + HomeKit pairing + pair-verify
│   ├── chacha_transport AP2 ChaCha20-Poly1305 encrypted RTSP
│   ├── playfair         FairPlay key decryption (`video`)
│   ├── video_cipher     AES-128-CTR streaming cipher for video
│   └── tlv              AP2 TLV codec for pairing messages
├── codec/               Audio decoders
│   ├── alac             ALAC decoder (realtime streams)
│   ├── aac              AAC decoder via symphonia (buffered streams)
│   └── resample         Sample rate conversion + channel mixdown
├── proto/               HTTP/RTSP, DMAP metadata, HTTP Digest auth
├── net/                 Async TCP server, mDNS, PTP timing, feature flags
└── error/               Error types
```

## Test Coverage

Pairing vectors are C-verified against [pair_ap](https://github.com/ejurgensen/pair_ap); FairPlay vectors were generated from the original [shairplay](https://github.com/juhovh/shairplay) C source:

```plain
cargo test                    # AirPlay 2 tests
cargo test --features video   # + screen mirroring / legacy RTP tests
```

## Development

```sh
git config core.hooksPath .githooks
```

This enables pre-commit (auto-format) and pre-push (fmt check + clippy + audit) hooks.

## Acknowledgments

This project builds on the work of many contributors to the AirPlay open-source ecosystem:

- **[shairplay](https://github.com/juhovh/shairplay)** — Original C library this project is a complete rewrite of
- **[shairport-sync](https://github.com/mikebrady/shairport-sync)** — AirPlay 2 C reference implementation by Mike Brady
- **[pair_ap](https://github.com/ejurgensen/pair_ap)** — HomeKit pairing reference (SRP-6a, TLV, HKDF) by Scott Ickle / ejurgensen. 17 C-verified test vectors generated from this codebase
- **[AirPlay 2 Internals](https://emanuelecozzi.net/docs/airplay2/)** — Protocol documentation by Emanuele Cozzi
- **[Unofficial AirPlay Specification](https://openairplay.github.io/airplay-spec/)** — Legacy protocol documentation
- **[rairplay](https://github.com/r4v3n6101/rairplay)** — Rust AirPlay 2 receiver with video support
- **[pyatv](https://github.com/postlund/pyatv)** — Python Apple TV library (companion-link protocol research)
- **[Dissecting the Media Remote Protocol](https://edc.me/posts/dissecting-the-media-remote-protocol/)** — Evan Coleman's Apple TV reverse engineering

## License

LGPL-3.0-or-later

## Disclaimer

All resources in this repository are written using only freely available information from the internet. The code and related resources are meant for educational purposes only. It is the responsibility of the user to make sure all local laws are adhered to. Parts of this project are generated with the help of AI and the expert-in-the-loop approach.
