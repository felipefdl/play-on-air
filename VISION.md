# PlayOnAir

## What it is

PlayOnAir is a small local network bridge: Chromecast devices on the LAN appear as **AirPlay 2** speakers. You run one binary on a machine that can reach those devices. iPhone, iPad, and Mac send AirPlay 2 audio to PlayOnAir; PlayOnAir re-streams that audio to the matching Chromecast at the highest fidelity the wire allows.

It is personal open source (MIT), maintained by Felipe Lima. It is not a TagoIO product and has no cloud account, dashboard, or telemetry.

## Problem

Apple devices speak AirPlay. Many homes already have Google Cast speakers and TVs. AirConnect and similar tools bridge that gap, but the goals here are narrower and stricter:

- AirPlay **2 only** (no AirPlay 1)
- Chromecast / Google Cast **only** (no UPnP, Sonos, or DLNA)
- **Zero setup** for the happy path
- **Audio quality and performance first**, not feature surface

## Success looks like

1. Install or build the binary.
2. Run it with no flags and no config file.
3. Every reachable Chromecast on the LAN shows up in the AirPlay picker under a clear name.
4. Music and system AirPlay audio play stably, with no second lossy encode when the source is already lossless, and with a tight, low-overhead path when the source is buffered AAC.
5. Optional cosmetic control (rename or hide a device) lives in a TOML file that is never required to play audio.

## Non-goals

| Out of scope | Why |
|---|---|
| AirPlay 1 | AP2 only; no dual stack |
| UPnP / Sonos / generic DLNA | Chromecast path only |
| Video / screen mirroring / A/V lip sync | Hard protocol mismatch (timed RTP push vs Cast HTTP pull); audio product |
| Cloud, accounts, remote control from the internet | LAN process only |
| Required configuration, wizards, or XML | Defaults must just work |
| Multi-room clock sync across mixed vendors | Cast sink cannot take Apple PTP timing; no fake multi-room promises |
| Hi-res (24/96+) end-to-end | AirPlay speaker casting caps the source; do not advertise what the wire cannot deliver |
| Enterprise MDM / multi-tenant / compliance packaging | Single-user / household LAN tool |

## Product principles

### 1. Zero friction default

Running the binary is enough. Discover Cast devices, advertise each as AirPlay 2, bridge audio. No config file, no interactive setup, no “first-run wizard.” Missing optional config means product defaults, not failure.

### 2. Quality over codecs menu

The audio path is fixed for quality:

- Decode AirPlay 2 input (realtime ALAC and buffered AAC as clients send them).
- Re-encode for Chromecast with **FLAC** (or lossless WAV/LPCM only if a device cannot take FLAC).
- **Never** default to MP3 or AAC for the Cast hop. Do not add a “quality vs CPU” codec switch that invites lossy second encodes.

Preserve bits when the source is lossless. When Apple already sent lossy AAC, decode once and do not damage it further.

### 3. Performance is a feature

Hot path: decrypt/decode → ring buffer → lossless encode → HTTP body to the Cast player. No alloc thrash in the steady state. Control plane (mDNS, Cast JSON, volume, pause) stays off the sample path. Prefer established crates over hand-rolled crypto and codecs when they meet the quality bar.

### 4. Optional config only for identity cosmetics

An optional TOML file may rename or hide devices. It must not be required for discovery, pairing, or playback. Core behavior is not gated on “did the user write a config.”

### 5. Honest limits

Document present-tense facts only. AirPlay 2 buffered music is often AAC from the client. Classic CD-rate lossless is the realistic ceiling for speaker casting. Do not invent product version numbers for feature availability. Unsupported means **not supported**, not “later release.”

### 6. One process, one job

Single Rust binary (or a small workspace with one user-facing binary). Host: macOS and Linux first (including always-on hosts and Pi-class machines). Same LAN as the Chromecasts; host networking if containerized.

## Shape (architecture intent)

```
Chromecast mDNS discovery
        │
        ▼
  one AP2 advertisement per device
        │
iOS/macOS ──AirPlay 2──► PlayOnAir ──FLAC HTTP + Cast──► Chromecast
```

- **Ingress:** AirPlay 2 receiver (pairing, FairPlay, encrypted RTSP, buffered and realtime audio).
- **Core:** session state, prebuffer/drift policy, flush/skip, volume mapping.
- **Egress:** local HTTP media URL + Cast v2 control (play, pause, stop, volume).
- **Config (optional):** rename / hide only.

## Operator promise

| Promise | Detail |
|---|---|
| No required config | `play-on-air` (name TBD for binary) runs with defaults |
| Automatic discovery | Continuous Cast discovery; AirPlay ads follow device presence |
| Names | Chromecast’s advertised name by default |
| Optional TOML | Rename and hide only; path documented when the binary lands |
| Privacy | No telemetry; no cloud |

## Quality bar for the codebase

Maximum. Match the Rust high bar used on Zyrdon’s server workspace: edition 2024, strict Clippy, rustfmt 120/2, no `unwrap` in production paths, typed errors, supply-chain checks, tests for new surfaces. Details live in `AGENTS.md`.

## Name

**PlayOnAir** is the product name. Repository / folder: `play-on-air`. Crate and binary names stay English kebab/snake as decided in implementation; they must not invent TagoIO or third-party branding.
