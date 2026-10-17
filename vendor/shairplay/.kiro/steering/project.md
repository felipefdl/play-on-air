# Project: shairplay

Pure Rust AirPlay server library. Published on crates.io as `shairplay`.

## Repository

- GitHub: `metaneutrons/shairplay-rust`
- Local: `~/Source/shairplay`
- License: LGPL-3.0-or-later

## Architecture

```
src/
├── raop/           Server core: RTSP dispatch, RTP audio, buffered audio
│   ├── types.rs        Public API types and traits
│   ├── server.rs       RaopServer builder + lifecycle
│   ├── connection.rs   Per-connection state + dispatch
│   ├── rtsp.rs         Route-table RTSP dispatch
│   ├── handlers.rs     Connection state + shared RTSP handlers (fp-setup, OPTIONS, GET/SET_PARAMETER)
│   ├── handlers_ap2.rs AP2 RTSP handlers
│   ├── rtp.rs          Legacy RTP audio (`video` only)
│   ├── ntp.rs          NTP timing responder (`video` only)
│   ├── buffer.rs       Legacy RTP buffer + AES-CBC decrypt + ALAC decode (`video` only)
│   ├── buffered_audio  AP2 timed playout buffer
│   ├── realtime_audio  AP2 realtime ALAC
│   └── event_channel   AP2 encrypted event channel
├── video/          Screen mirroring (experimental)
├── hls/            HLS video playback (YouTube)
├── crypto/         Ed25519, FairPlay, SRP-6a, ChaCha20
├── codec/          ALAC decoder, AAC decoder, resampler
├── proto/          HTTP/RTSP, DMAP, HTTP Digest
├── net/            Async TCP server, mDNS, PTP timing, feature flags
└── error/          Error types (thiserror)
```

## Feature Flags

| Flag | Implies | Description |
|------|---------|-------------|
| *(default)* | — | AirPlay 2 audio (no AirPlay 1 stack) |
| `video` | — | Screen mirroring + its legacy RTP audio |
| `hls` | `video` | HLS video playback |

## Public API

All types re-exported from `lib.rs`. Users write `shairplay::RaopServer`, never `shairplay::raop::*`.

Key traits:
- `AudioHandler` — factory for audio sessions + lifecycle callbacks
- `AudioSession` — receives PCM samples, metadata, volume, progress
- `HlsHandler` / `HlsSession` — HLS URL relay + playback state
- `VideoHandler` / `VideoSession` — video NAL unit delivery
- `PairingStore` — persistent HomeKit key storage
