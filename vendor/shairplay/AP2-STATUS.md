# AirPlay 2 — Implementation Status & Research

This fork is AirPlay 2 only: the AirPlay 1 stack (RSA `airport.key`, legacy
pairing, ANNOUNCE/SDP, DACP remote control) is removed. See
[PLAYONAIR.md](PLAYONAIR.md) item 21 for what was removed and which AirPlay 1-era
pieces the `video` feature still compiles in.

## Complete

| Feature | Stream Type | Details |
|---------|-------------|---------|
| mDNS discovery | — | `_airplay._tcp` + `_raop._tcp`, `et=0,3,5` |
| SRP-6a transient pairing | — | PIN 3939, automatic, no persistence |
| Normal HomeKit pairing | — | Configurable PIN, PairingStore key persistence |
| Encrypted RTSP transport | — | ChaCha20-Poly1305, HKDF-SHA512 key derivation |
| FairPlay handshake | — | Full fp-setup M1/M2 (required: features advertise `Authentication4`). Key decryption (`playfair`) is `video`-only |
| PTP timing | — | Passive follower on UDP 319/320 (overridable via `RaopServerBuilder::ptp_ports`); per-grandmaster offset from Sync/Follow_Up with a min-delay filter; SETUP `timingPeerInfo` / SETPEERS / SETPEERSX define accepted peers. Never transmits (no master election, no multi-room) |
| Buffered audio | 103 | AAC decode (symphonia), per-packet ChaCha20 decrypt |
| Multichannel | 103 | 5.1/7.1 AAC → stereo mixdown (ITU-R BS.775) |
| Resampling | 103 | rubato StreamResampler, any rate → output rate |
| Timed playout buffer | 103 | Anchor timeline per `docs/ap2-receiver-spec.md` §1–2: release at presentation − `Audio-Latency`, prune behind playhead, rate 0/1 only, 30 ms fade-in, 120 s high water with TCP back-pressure, 24-bit FLUSHBUFFERED rules. Unlocked PTP → receipt-pinned anchor fallback |
| Metadata forwarding | — | `docs/ap2-receiver-spec.md` §5: every `text/parameters` line (`volume`, `progress`), any `image/*` artwork (with its MIME type), DMAP `mlit` (title, artist, album, genre, composer, duration, track/disc number and count, stream flag, item ids), and `POST /command` `updateMRNowPlayingInfo` (`mergePolicy` update/replace, `artworkData`). Applied on arrival (the `RTP-Info` apply time is not honored) |
| Event channel | — | Bidirectional encrypted TCP; `updateInfo` sent on connect |
| Reverse control | — | `RaopServer::send_media_remote_command` (`sendMediaRemoteCommand` with a four-character `value`) and `RaopServer::report_volume` (`dvlc`, senders ≥ 354.54.6) as `POST /command` on the event channel of the active-audio connection (else the newest). No DACP |
| Realtime audio | 96 | ALAC decode, ChaCha20 decrypt, immediate delivery |
| **Video (screen mirroring)** | **110** | **AES-128-CTR decrypt, H.264 decode, working on iOS 18** |
| Unified output | — | Always F32LE interleaved PCM to app |

## Not implemented

- Sender `POST /command` types other than `updateMRNowPlayingInfo` (`updateMRSupportedCommands`,
  `updateMRPlaybackState`, `mute` / `unmute`, `changeRelativeVolume`, `duckAudio`, …) are
  acknowledged and logged only.
- `modernMediaRemoteCommand` numbers are unknown (spec open question); commands send `value` only.

## Video — Working (iOS 18)

**Screen mirroring is working.** iPhone screen successfully mirrored and
displayed in a window. The full pipeline is proven:

1. FairPlay key exchange (fp-setup M1/M2)
2. AES-128-CTR video decryption
3. H.264 decode (openh264) + display (minifb)

### Key Discovery: UxPlay Feature Set

The breakthrough was using UxPlay's exact feature bitmask:

```
Features = 0x527FFEE6 (bit 27 "legacy pairing" OFF)
```

With this feature set:
- iPhone **skips pair-setup and pair-verify entirely**
- iPhone sends `ekey` (72 bytes, FairPlay-encrypted) directly in SETUP
- No ECDH hash needed — raw FairPlay-decrypted key goes to Stage 3
- No AP2 bits (40, 41, 46, 48) — pure legacy protocol for video

### Video Key Derivation (Working)

Two-step process (no ECDH hash when bit 27 is off):

```
Step 1: aeskey = playfair_decrypt(keymsg_164, ekey_72)              → 16 bytes
Step 2: key    = SHA-512("AirPlayStreamKey{id}" || aeskey)[0..16]   → 16 bytes
        iv     = SHA-512("AirPlayStreamIV{id}"  || aeskey)[0..16]   → 16 bytes
```

Where `{id}` is the `streamConnectionID` from the type 110 SETUP, formatted
as unsigned decimal (`PRIu64`).

### Critical Bug Found: Truncated FairPlay Tables

The Rust port of `playfair_decrypt` had **6 truncated lookup tables** from the
original C-to-Rust conversion. Rust zero-filled the missing bytes, causing
`playfair_decrypt` to produce wrong keys silently:

| Table | Declared | Actual | Missing |
|-------|----------|--------|---------|
| TABLE_S1 | 10240 | 9600 | 640 |
| TABLE_S2 | 36864 | 34560 | 2304 |
| TABLE_S3 | 4096 | 3840 | 256 |
| TABLE_S4 | 36864 | 34560 | 2304 |
| TABLE_S9 | 1024 | 256 | 768 |
| TABLE_S10 | 4096 | 3840 | 256 |
| STATIC_SOURCE_2 | 47 | 46 | 1 |

Tables regenerated from original C source (`playfair/omg_hax.c`).

**⚠️ This bug invalidated ALL previous video decryption research.** Every
"tested approach" that failed was tested with a broken `playfair_decrypt`.
The previous conclusion that "iOS 18 doesn't send ekey" was wrong — it does
send ekey, but only with UxPlay's feature set (no AP2 bits).

### Current FairPlay Status

FairPlay decryption is implemented in safe Rust. The generated lookup tables are
checked in as Rust constants and are covered by test vectors.

### Previous Research — Needs Re-verification

Earlier video-key research was conducted with the broken `playfair_decrypt`
implementation and may no longer be accurate:

- "iOS 18 doesn't send ekey for screen mirroring" — **WRONG.** It does, with
  UxPlay features (`0x527FFEE6`).
- "ECDH hash is required for video key" — **WRONG for legacy mode.** With bit 27
  off, no pairing occurs and the raw FairPlay key is used directly.
- "All 13+ key derivation variants failed" — **All tested with wrong FairPlay key.**
  Need to re-test with AP2 features if AP2+video is desired.
- "Screen mirroring audio (type 96 usingScreen) has no shk" — Needs re-testing
  with UxPlay features.

### Current Limitations

- **No AP2 audio with video** — UxPlay features disable AP2 buffered audio
- **openh264 decode errors** — Software decoder struggles at 30fps in debug mode
- **No screen mirroring audio** — Type 96 `usingScreen` audio not yet wired up

### Stream Type 120 — Video Relay (Not Implemented)

Sent by YouTube, Apple Music (music videos), and other video apps. The SETUP
contains only `{"type": Integer(120)}` with no additional fields. Likely HLS
video relay where the app sends a video URL for the receiver to fetch directly.

### TODO
- [ ] Test AP2+video hybrid features (can we have both AP2 audio and video?)
- [ ] Wire up screen mirroring audio (type 96 `usingScreen`)
- [ ] Improve H.264 decode stability (release build, error recovery)
- [ ] Re-test previous key derivation approaches with correct FairPlay key

### Hypothesis: HLS with AP2 Audio

HLS video playback (`/play`, `/playback-info`) is pure HTTP — it relays an
m3u8 URL to the application and doesn't use the RTP audio pipeline. It may
be possible to run HLS alongside AP2 buffered audio (type 103) instead of
requiring the UxPlay legacy feature set. The iPhone could send the video URL
via HLS while streaming audio via AP2.

Currently untested: all open-source implementations (UxPlay, AirShow) use
legacy features for HLS. The `hls` feature implies `video` (legacy features)
as a safe default. Decoupling `hls` from `video` to test with AP2 features
is a potential future experiment.

## Known Issues (Resolved)

### RC Connection Delay (~10s) — FULLY RESOLVED ✅

Previously, the iPhone opened a "Remote Control Only" RTSP connection ~10 seconds before starting the audio connection. 

This has been **fully resolved** by:
1. Deriving a **stable and deterministic Pairing Identifier (`pi`)** from the receiver's MAC address (advertised consistently in mDNS).
2. Correctly completing the GET `/info` response plist payload to return `"pi"`, `"name"`, `"macAddress"`, and `"deviceID"`.
3. Binding and returning a proper `eventPort` in the `isRemoteControlOnly` SETUP response plist, and spawning the encrypted event channel handler.

With these changes, the trust relationship is established instantly, and audio streaming begins in under 50ms without any delay!

## AP2 Remote Control

Implemented per `docs/ap2-receiver-spec.md` section 5: the receiver sends
`POST /command` `{type: "sendMediaRemoteCommand", value: <code>}` on the encrypted event
channel (no DACP, no `Active-Remote`). Volume reports use `value: "dvlc"` with `volume`
(slider `0..1`) and `isMuted`, both top level and in `params`; senders older than 354.54.6
(they expect DACP) get `RemoteError::SenderTooOld`. A report equal to the current reported
volume (including one the sender just set) is not sent.

## Test Coverage

Default build (AirPlay 2): 126 lib + 21 integration + 33 unit tests; `--features video`
adds the legacy RTP buffer, playfair, and video cipher tests (140 lib + 37 unit).
17 C-verified vectors from the pair_ap reference implementation:
- TLV codec, HKDF-SHA512, ChaCha20 transport framing
- ADTS framing, audio packet decryption, server keypair
- Anchor time calculation, channel mixdown, SSRC mapping
- Full M1→M4 SRP integration test over real TCP
- AirPlay 1 requests (legacy pair-setup, ANNOUNCE, `Transport` SETUP) are not served
- `video`: video cipher streaming AES-CTR partial block tests, FairPlay cross-validation
  against C `playfair_decrypt` output

## References

- [AirPlay 2 Internals — Features](https://emanuelecozzi.net/docs/airplay2/features/)
- [AirPlay 2 Internals — RTSP](https://emanuelecozzi.net/docs/airplay2/rtsp/)
- [Unofficial AirPlay Specification](https://openairplay.github.io/airplay-spec/)
- [UxPlay](https://github.com/FDH2/UxPlay) — working screen mirroring reference
- [pair_ap](https://github.com/ejurgensen/pair_ap)
- [shairport-sync](https://github.com/mikebrady/shairport-sync)
