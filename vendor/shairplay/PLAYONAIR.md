# PlayOnAir vendored shairplay

Upstream: [shairplay](https://crates.io/crates/shairplay) 0.7.0 (LGPL-3.0-or-later).

## Why vendored

Stock `RaopServer::stop()` only signals the accept loop. Accepted RTSP connections,
event channels, and buffered-audio tasks stay alive. After Cast steal, PlayOnAir
kicked the speaker advertisement but **iPhone Now Playing stayed connected**.

## PlayOnAir changes (relative to 0.7.0)

1. `HttpServer` tracks accept-loop and connection `AbortHandle`s; `stop()` aborts them.
2. `RaopShared::hard_stop_sessions()` stops active audio and aborts registered AP2 tasks.
3. Event / RC / realtime / buffered-audio tasks register abort handles.
4. `RaopServer::stop()` calls `hard_stop_sessions()` then `httpd.stop()`.
5. Reported volume: `RaopShared` stores the dB returned by `GET_PARAMETER volume` (default `0.0`);
   `SET_PARAMETER` updates it; `RaopServer::set_reported_volume_db` / `reported_volume_db` let the host
   seed the value from Chromecast so the iOS slider matches device level.
6. Buffered delivery: drop old `AudioSession` before `audio_init` on format change; run `audio_init`
   outside the playout mutex (avoids Drop clearing the new ring).
7. `AudioSession::on_rate` / `on_flush` for AP2 buffered SetRate/FLUSHBUFFERED; delivery drains
   pending flags so pause/flush reach the host while rate is 0.
8. Buffered playout: `PlayoutStop` sync stop + receive-task Drop cleanup so hard-stop before task
   abort unblocks the delivery thread (async `PlayoutCommand::Stop` alone was abort-unsafe).
9. TEARDOWN calls `hard_stop_sessions()` (clears active_audio, aborts session tasks including
   realtime) and takes `playout_cmd` instead of a best-effort async Stop.
10. Playout anchors/scheduling use `mono_now_ns()` (`Instant`); wall `now_ns()` remains for PTP only.
11. Delivery caps due AAC packets per tick (`MAX_FRAMES_PER_TICK`) so catch-up cannot flood the host ring.
12. Playout RTP math uses `source_sample_rate`; FLUSHBUFFERED range compare is wrap-safe. Buffered map
    uses **read-driven flow control** (target ~60 s / resume ~50 s hysteresis, aligned with advertised
    `audioBufferSize = 0x100000`): when full, the receive loop stops reading TCP so the kernel window
    paces iOS. Delivery/`notify_all` after consume, flush, and stop unpark the waiter. Drop-oldest was
    removed — it punched holes at the playhead. Pathological backstop only: refuse **newest** past 3×
    target (~180 s).
13. Playout mutex/condvar locks use `unwrap_or_else(PoisonError::into_inner)` (poison-proof).
14. Realtime type 96: RTP seq reorder window (drop dups, silence for aged gaps; ALAC in order only).
    Decode failure fills the same silence length as a Lost packet (keeps PCM clock length).
15. RTSP `process_connection` read timeout (45s) so half-open iOS sockets release the connection semaphore; accept loop prunes finished `conn_aborts`.
16. Configurable `Audio-Latency` RECORD header (`RaopServerBuilder::audio_latency_samples`, default 96000 = 2s@48k).
17. Event channel: 1 MB ciphertext cap, disconnect on decrypt error, exit when command sender closes.
18. PTP sink abort handles stored and aborted in `RaopServer::stop` (avoids EADDRINUSE on restart).
19. FLUSHBUFFERED: parse `flushFromTS` / `flushUntilTS` (RTP timestamps) for map trim + epoch purge;
    while playing, arm a pending re-anchor resolved on the first post-flush packet that survives epoch
    enforcement (100 ms lead), instead of anchoring to `flushUntilTS` (wrong when Apple re-keys the
    RTP epoch on track skip — cross-epoch wrap-safe compare would firehose the whole prebuffer).
    If an old-epoch straggler claims that pending first, the next insert that collapses the map onto
    a new epoch (`epoch_dropped > 0`, survivor epoch-far from the current anchor) re-anchors with
    the same lead even when pending is already clear. Wire SETRATEANCHORTIME clears pending /
    overrides. Fixes ~30 s silence on same-epoch skip without SETRATE.
20. SETRATEANCHORTIME (SetRate rate!=0): purge buffered frames more than 2 s of source samples behind
    the wire RTP anchor before the earliest-frame 100 ms lead nudge. Wire anchor is authoritative;
    prevents one stray pre-seek packet from dragging playout tens/hundreds of seconds into the past
    after scrub.
21. **AirPlay 2 only — AirPlay 1 stack removed** (AGENTS.md "AP2 only"). Deleted: `airport.key` and
    `crypto::rsa` (Apple-Challenge / `Apple-Response`, `rsaaeskey`), `crypto::pairing` + `crypto::aes`
    (legacy Ed25519 pair-setup/verify fallback), `proto::sdp` + ANNOUNCE, `Transport`-header SETUP,
    AP1 RECORD, `dacp` + `RemoteControl` / `RemoteCommand` / `AudioHandler::on_remote_control`,
    `AirPlayMode` + `RaopServerBuilder::mode`, the AP1-only `AirPlayServiceInfo::new` TXT profile
    (the AirPlay 2 profile is now `AirPlayServiceInfo::new`), `CryptoError::{RsaKey, RsaDecrypt}`,
    `ProtocolError::Sdp`. The `ap2` and `resample` cargo features are gone: AirPlay 2 is the
    baseline (`default = []`); `rsa`, `sha1`, `base64` deps dropped (and with them the
    RUSTSEC-2023-0071 ignore in the root `deny.toml` / `.cargo/audit.toml`). `handlers_ap1.rs` became
    `handlers.rs` (connection state + shared handlers). ANNOUNCE and unpaired plist-less SETUP now
    return 404; non-TLV pair-setup/verify get an empty 200.

### AirPlay 2 dependencies kept

- **fp-setup / FairPlay M1/M2** (`crypto::fairplay`): the AP2 feature set advertises
  `Authentication4` (bit 14), so iOS runs fp-setup before SETUP.
- **HTTP Digest auth** (`password`), `_raop._tcp` TXT base fields (`cn`, `da`, `et=0,3,5`, `tp`,
  `md`, `vn=65537`): published by AirPlay 2 receivers (shairport-sync parity).
- **Plain RTSP `FLUSH`**: acknowledged (200) for AP2 senders; it only flushes legacy video RTP.

### `video` feature boundary (not removed)

UxPlay-style screen mirroring (`video`, `hls`) runs the legacy, unpaired protocol and still needs
AirPlay 1-era pieces, now compiled **only** with `video`: `raop::rtp` + `raop::buffer` (AES-CBC
ALAC over RTP, including the unused TCP-interleaved arm), `raop::ntp` (NTP timing responder),
`crypto::playfair` + FairPlay tables/garble (`FairPlay::decrypt` for `ekey`), `AlacConfig`, the
`aes`/`cbc`/`ctr` deps, and `AudioSession::audio_flush` callers. PlayOnAir does not enable `video`;
removing video is out of scope.

### Accepted deviations

- **Buffered `BTreeMap<u32, …>` RTP key order:** keys are raw `u32` RTP timestamps. Across a wrap, `BTreeMap` order is not wrap-aware (high keys sort after low keys). The ~60 s target depth (and 3× newest-drop backstop) keeps the live span ≪ 2^32 samples, so wrap-within-map does not occur for normal sessions. Revisit only if multi-hour sessions keep a map that can span a wrap.

Keep upstream license files. Prefer contributing hard-stop upstream and dropping the vendor when released.
