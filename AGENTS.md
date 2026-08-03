# PlayOnAir

Agent map for this repository. Maintainer: **Felipe Lima**. Personal OSS, **MIT**.

**Default quality: maximum.** Correctness, modern APIs, tests, and format/lint gates beat speed. Read current docs before using crates. Do not relax gates to land work.

Product intent: [`VISION.md`](VISION.md).

## Product posture

| Rule | Meaning |
|------|---------|
| **Zero setup** | Binary runs with no config file and no flags for the happy path. Discover Chromecasts, expose each as AirPlay 2, bridge audio. Missing optional TOML = product defaults, not error. |
| **AP2 only** | AirPlay 2 receiver only. No AirPlay 1 stack, no dual protocol. |
| **Chromecast only** | Google Cast devices only. No UPnP, Sonos, or generic DLNA. |
| **Quality first** | Decode AirPlay input once; egress **FLAC** over chunked HTTP with Cast `streamType` LIVE (WAV/LPCM BUFFERED fallback when a sink rejects FLAC). Never default to MP3/AAC for the Cast hop. Prefer realtime ALAC when the client offers it; fully support buffered AAC without a second lossy encode. ~2 s Cast-side cushion via silence preroll and maintained lead. |
| **Performance first** | Steady-state audio path: no alloc thrash, no blocking Cast control on the sample thread, pre-sized ring buffers, structured `tracing` only. |
| **Optional cosmetics only** | Optional TOML may rename or hide devices. It must not be required for discovery, pairing, or playback. |
| **Honest limits** | No video A/V sync promises. No hi-res end-to-end claims beyond what AirPlay speaker casting delivers. Unsupported is **not supported**. |
| **LAN process** | Single machine on the same network as the Cast devices. No cloud, no accounts, no telemetry. |

**No invented product versions (hard).** Docs and user-facing prose are present-tense facts. Do not invent product `v1` / `v2`, “current release,” or “may expand” language for feature availability. Package/crate semver is fine. Full rule: personal `lima-dev` skill.

**Unshipped is not legacy (hard).** Surfaces that exist only on this branch / open PR are unfinished work. Rename by rewriting; no dual routes, aliases, or compatibility shims for unshipped names.

## Stack

| Field | Value |
|-------|--------|
| Language | Rust only for product code |
| Edition | 2024 |
| MSRV | 1.88 (`rust-version` in workspace; required by shairplay) |
| Layout | Cargo workspace at repo root when crates exist; `crates/<name>/` with matching package name |
| Format | `rustfmt.toml`: max width **120**, tab spaces **2**, edition 2024 |
| Clippy | Workspace lints at **deny** (Zyrdon server bar); `clippy.toml` thresholds; `-D warnings` in CI/gate |
| Unsafe | `unsafe_code = forbid` unless a crate documents a tiny, reviewed exception with reason |
| Supply chain | `cargo deny` + `cargo audit` + `cargo machete` when the workspace exists |
| Tests | Prefer `cargo nextest` when available; unit + integration for new surfaces |
| Time | UTC for stored and wire times |
| Logging | `tracing` only. No `println!` / `eprintln!` in non-test product code |

## Lint policy (non-negotiable)

Mirror the Zyrdon server high bar once `Cargo.toml` exists:

- `unsafe_code = forbid`
- Clippy groups `all`, `pedantic`, `nursery`, `cargo` at **deny**, with a short allow-list only for known noise
- Restriction group not enabled wholesale; selected panic/silent-fail lints denied individually (`unwrap_used`, `expect_used`, `indexing_slicing`, `string_slice`, `await_holding_lock`, …)
- Tests may unwrap/expect/index (`allow-*-in-tests` in `clippy.toml`); production code may not
- `#[allow(...)]` without `reason` is denied; prefer `#[expect(..., reason = "...")]`
- Public items: `missing_docs = deny` when the library surface is public
- Do not weaken workspace lints or turn off deny gates to land a change

New crates must set `[lints] workspace = true`.

## Conventions

- American English in code, commits, docs, and agent-facing text.
- Public APIs: rustdoc on public items.
- Errors: typed `Error` / `Result`, `?`, no stringly panics on I/O or protocol paths.
- Dependencies: established crates only; check latest compatible version and **current docs** before adopting. Prefer libraries over hand-rolled crypto, codecs, mDNS, and Cast protocol.
- Config injection: parse optional TOML into a typed struct; do not scatter `std::env::set_var` / `remove_var` (disallow in clippy when configured).
- Architecture forks (AirPlay stack crate choice, Cast client, encode path) with more than one valid option → **ask the maintainer before implementing**.

## Audio and protocol rules

1. **No second lossy encode** on the default path. Decode once; egress FLAC over chunked HTTP (`streamType` LIVE). WAV/LPCM BUFFERED is the fallback when a device rejects FLAC.
2. Support AP2 **realtime** and **buffered** audio as clients send them. Do not require the user to pick a mode.
3. Discovery is continuous. Device gone → withdraw AirPlay advertisement; device back → re-advertise.
4. Default AirPlay name = Chromecast advertised name (identity map). Optional TOML overrides name or sets hide.
5. Pairing / FairPlay / encrypted RTSP stay on the AP2 path. Do not ship an AP1 “fallback that just works.”
6. Video, screen mirroring, and lip-synced TV audio are **not supported**.

## Layout

```
play-on-air/
  AGENTS.md          # this file
  CLAUDE.md          # symlink → AGENTS.md
  VISION.md          # product intent
  LICENSE.md         # MIT, Felipe Lima
  README.md
  repository.yaml    # Home Assistant app store repository
  Dockerfile         # multi-stage binary image (GHCR)
  rustfmt.toml
  clippy.toml
  deny.toml
  Cargo.toml         # virtual workspace
  crates/
    play-on-air/     # binary + library
  play-on-air/       # HAOS app folder (config.yaml, DOCS.md, …)
  .github/workflows/ # ci + docker multi-arch GHCR
  docs/              # design notes, plans (optional)
```

Put reusable logic in a library crate, not only in `main`. HAOS packaging lives under root `play-on-air/` (app slug folder) and pulls `ghcr.io/felipefdl/play-on-air` with `host_network: true`.

## Quality gate

Before claiming work done (once the workspace exists):

```bash
# Prefer a root justfile when present; otherwise from workspace root:
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace   # or cargo test --workspace
cargo deny check
cargo audit
cargo machete
```

Do not claim done on green unit tests alone if format, clippy, or supply-chain gates fail.

## Git

- Commits: conventional `type(scope): subject`, subject under 72 chars, lowercase subject, no trailing period.
- Scopes: crate or area (`airplay`, `cast`, `bridge`, `discover`, `config`, `docs`).
- Branches: `type/description` kebab-case (`feat/…`, `fix/…`, `chore/…`).
- Never commit secrets, real device pairing material, or private keys.
- Never commit or push without explicit approval (plan/spec-only commits follow personal lima-dev exception if used).
- Never amend published history; never add `Co-Authored-By`.
- On public GitHub: never mention AI/LLMs/tooling in commits, PR bodies, or comments.

## Deploy and release (version sync — hard)

One version string everywhere. Never ship GHCR without a matching GitHub Release (and never leave a Release for a version that was not built).

| Surface | Source of truth |
|---------|-----------------|
| Cargo workspace | root `Cargo.toml` → `[workspace.package] version` |
| Cargo.lock package | same after `cargo update -p play-on-air` / build |
| HA app | `play-on-air/config.yaml` → `version` |
| Docker default | `Dockerfile` → `ARG BUILD_VERSION` |
| Docs image tags | root `README.md`, `play-on-air/DOCS.md` |
| Changelog | `play-on-air/CHANGELOG.md` section `## x.y.z` |
| GHCR tags | `ghcr.io/felipefdl/play-on-air:x.y.z` (+ `latest` on main) |
| Git tag + GitHub Release | `vX.Y.Z` created by `.github/workflows/docker.yml` **release** job after image merge |

### Ship flow (agents and humans)

1. Bump **all** version surfaces in the same change (list above).
2. Add a `## x.y.z` block at the top of `play-on-air/CHANGELOG.md`.
3. Merge to `main` (or push tag `vX.Y.Z`).
4. Wait for **docker** workflow: build → multi-arch merge → **release** job.
5. Confirm:
   - GHCR: `ghcr.io/felipefdl/play-on-air:x.y.z`
   - GitHub Releases: `vX.Y.Z` with the same version and changelog notes
6. Only then tell users to upgrade the HA app / pull the image.

Do **not** create ad-hoc GitHub Releases by hand with a different version than `config.yaml`. Do **not** bump GHCR-only without the release job. If the release job fails, fix and re-run; do not claim the version shipped.

## README and repo files

Personal MIT project. Do **not** apply TagoIO `repo-standards` branding (no assets.tago.io logos, no “Built by the TagoIO team”).

When public files exist:

- `LICENSE.md` — MIT, Copyright (c) year **Felipe Lima**
- Root `README.md` — product name, one-line description, run instructions, link to `VISION.md` and `LICENSE.md`
- Optional later: `SECURITY.md`, `CONTRIBUTING.md` when the repo is public and needs them

## Rules

1. New public API or protocol surface → test in the same change.
2. New dependency → current docs read + deny/audit still green.
3. No `unwrap` / `expect` / `panic!` in non-test product code.
4. Do not add required config, AP1, UPnP, or lossy Cast defaults “for convenience.”
5. Architecture fork with more than one valid option → ask first.
6. Read `VISION.md` before changing product behavior.
