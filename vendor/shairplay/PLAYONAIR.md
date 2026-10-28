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
6. Buffered delivery: drop old `AudioSession` before `audio_init` on output format change; run
   `audio_init` outside the playout mutex (avoids Drop clearing the new ring). The session is
   created at the first released packet (decode runs on the delivery thread).
7. `AudioSession::on_rate` / `on_flush` for AP2 buffered SETRATEANCHORTIME / FLUSHBUFFERED, queued
   in order and delivered by the delivery thread, also while stopped. `on_flush` fires for a
   flush-all only (everything the host holds predates the next anchor); a range flush keeps
   playing and does not notify the host.
8. Buffered playout: `PlayoutHandle::stop` is a synchronous stop that wakes the delivery thread and
   the suspended reader, and the reader task's Drop guard calls it, so hard-stop before task abort
   never wedges delivery. RTSP handlers drive playout through the same handle synchronously (no
   command task), which lets SETRATEANCHORTIME answer `firstAvailableSampleTS` / `status`.
9. TEARDOWN calls `hard_stop_sessions()` (clears active_audio, aborts session tasks including
   realtime) and drops the connection's `playout` handle.
10. Playout scheduling and PTP offsets use one clock, `mono_now_ns()` (`Instant`).
11. **Buffered playout timing follows `docs/ap2-receiver-spec.md` section 1.** RTP timestamps are
    extended to 64 bits (queue keys, so order is wrap-safe). Sample `r` is presented at
    `H(anchor) + (r - anchorRtp) / rate`; a packet is released to the host at presentation minus
    the release lead (`RaopServerBuilder::release_lead`, default and minimum the advertised
    `Audio-Latency`), never on arrival. PlayOnAir sets the lead to its Cast read-ahead (8 s) so
    queued audio is in the host ring to burst when Cast (re)LOADs. Packets whose end is
    behind the playhead are pruned (anchor in the past joins mid-stream). Rate must be 0 or 1
    (else plist `status` -6705); rate 0 stops, keeps the buffer and clears the anchor; each anchor
    start fades in over 30 ms. Packets are queued compressed and decoded at release.
12. **Back-pressure, not drops.** The reader suspends TCP reads once 120 s of audio is queued and
    resumes below 119 s; nothing is dropped for being early. SETUP `audioBufferSize` is computed:
    120 s x 192000 B/s (48 kHz 16-bit stereo PCM bound of the accepted formats).
13. Playout mutex/condvar locks use `unwrap_or_else(PoisonError::into_inner)` (poison-proof).
14. Realtime type 96: RTP seq reorder window (drop dups, silence for aged gaps; ALAC in order only).
    Decode failure fills the same silence length as a Lost packet (keeps PCM clock length).
15. RTSP `process_connection` read timeout (45s) so half-open iOS sockets release the connection semaphore; accept loop prunes finished `conn_aborts`.
16. Configurable `Audio-Latency` RECORD header (`RaopServerBuilder::audio_latency_samples`, default 96000 = 2s@48k)
    and a separate buffered release lead (`RaopServerBuilder::release_lead`, item 11).
17. Event channel: 1 MB ciphertext cap, disconnect on decrypt error, exit when command sender closes.
18. **PTP follower** (`net::ptp`, spec section 3) replaces the drain-only sink: passive, per
    grandmaster clock identity, offset from two-step Sync/Follow_Up (or one-step Sync) through a
    16-sample minimum-delay filter with 3-sample step confirmation; locked after 4 samples, stale
    after 3 s. SETUP `timingPeerInfo` adds and `timingPeerList` / SETPEERS (strings) / SETPEERSX
    (peer dictionaries) replace the session peer set; only timelines from session peers map
    anchors. Ports come from `RaopServerBuilder::ptp_ports` (default 319/320, `0` = ephemeral,
    readback `RaopServer::ptp_ports`); fixed ports are bound once per process and shared by every
    server. Bind failure logs one warning and playback uses the unlocked fallback (item 20).
19. **FLUSHBUFFERED follows spec section 2.** 24-bit modulo sequence compare. No `flushFromSeq`
    (or no `flushFromTS`): flush-all stops playback, discards the queue, resets anchor and RTP
    epoch, and drops incoming seq before `flushUntilSeq` (exclusive). Range flush keeps playing,
    trims the queue from `flushFromTS` to the end, and drops seq in `[flushFromSeq, flushUntilSeq)`.
    Rules retire at `flushUntilSeq + 20`. Missing `flushUntilSeq` answers `status` -6705.
20. **Unlocked anchor fallback** (deviation, below). An anchor on a timeline the follower is
    *locking* (fresh session-peer timeline with fewer than 4 samples, e.g. a grandmaster that
    appeared just before the anchor) is deferred: the RTSP reply is immediate, the delivery thread
    holds release and re-maps every 20 ms, and plays through PTP as soon as the timeline locks, at
    most 1 s after receipt. Otherwise (unknown, foreign or stale timeline, no follower, or no lock
    within 1 s) the first anchor on a timeline is mapped to "release on receipt" (`H = receipt +
    Audio-Latency`, receipt being the SETRATEANCHORTIME arrival) and that offset is pinned for
    later anchors on the same timeline; it re-pins when an anchor lands more than 1 s from
    release-on-receipt. The pin is dropped as soon as any anchor maps through a locked timeline,
    so later anchors never reuse a pre-lock offset. Warns once per stream.
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
22. **Advertising follows the Apple receiver spec** (`docs/ap2-receiver-spec.md` section 4).
    Features `0x0001C340405FCA00` (adds bit 15 artwork and bit 17 DAAP now-playing to the
    shairport-sync base; the old "bits 15/17 break discovery" note described one failed combination,
    and the Sonos SDK receiver advertises both). `GET /info` sends `pk` as 32-byte data plus
    `keepAliveSendStatsAsBody`, `supportedFormats` (`audioStream` ALAC 18-21, `bufferStream` AAC-LC
    22-23), `supportedAudioFormatsExtended = {bufferStream: [22, 23]}`, `volumeControlType = 3`
    (Cast applies volume), `initialVolume` (the reported dB), and `txtAirPlay`; the SETUP-time
    `updateInfo` value is the same dictionary. TXT: `_airplay._tcp` matches the Sonos SDK key set
    (adds `rsf=0x0`; drops `vv=2`, `btaddr`, `osvers`), `_raop._tcp` sends `tp=UDP` and drops `pw`/`ov`.
    A password sets statusFlags bit 7 instead of `pw=true`.
23. `RaopServerBuilder::advertise(bool)` (default `true`) decides whether `start()` registers the
    mDNS services. It replaces the old "skip mDNS when the `CI` env var is set" check; in-process
    tests (vendor `tests/integration.rs`, PlayOnAir `tests/e2e_buffered`) pass `false`.
24. **Reverse control and now-playing** (`docs/ap2-receiver-spec.md` sections 5 and 6, module
    `raop::remote`). `RaopServer::send_media_remote_command(MediaRemoteCommand)` and
    `RaopServer::report_volume(slider, muted)` send `POST /command` (binary plist, CSeq from 1)
    on the event channel of the active-audio connection, else the newest; `RaopShared` keeps the
    event senders per connection (unregistered on TEARDOWN / disconnect / hard stop). `dvlc` is
    gated on the sender's `User-Agent: AirPlay/x.y.z` ≥ 354.54.6 and skipped when equal to the
    current reported volume. SET_PARAMETER reads every `text/parameters` line and any `image/*`
    type; `AudioHandler::on_coverart` gets the MIME type. DMAP reads the spec tag set (`asdn` is
    the disc number; `asdk` the stream flag). `POST /command` `updateMRNowPlayingInfo` is merged
    per connection (`mergePolicy`) and surfaced through `on_metadata` / `on_coverart`.
    `AudioHandler::on_progress` timestamps are at the stream sample rate.
25. **AirPlay group identity.** The initial SETUP (not `isRemoteControlOnly`) reads `groupUUID`
    (non-empty string) and `isMultiSelectAirPlay` (boolean, absent = false) into the public
    `SessionGroup` and calls `AudioHandler::on_session_group` (default no-op) before the stream
    SETUP and `audio_init` of that session. A SETUP without the keys reports
    `SessionGroup::default()` (no group). PlayOnAir uses it to map AirPlay multi-room sessions onto
    Google multizone groups.
26. **Multi-select sender volume follows `docs/ap2-receiver-spec.md` section 6** (module
    `raop::volume`). On a connection whose SETUP had `isMultiSelectAirPlay = true`, a
    `SET_PARAMETER volume` within 0.01 slider (0.3 dB) of the current volume is ignored (no
    `on_volume`, current volume unchanged), and an applied one is acknowledged to the sender with
    `dvlc` (`RaopShared::ack_sender_volume`, same version gate, no equal-value skip): the first at
    once, then at most one per 250 ms, the last value of a burst always sent by a trailing timer.
    The current volume (`GET_PARAMETER volume`, `/info` `initialVolume`) only holds applied values.
    Single-receiver sessions apply every value and send no acknowledgement.
27. **Apple Home accessory setup** (HAP pairing admin). `POST /pair-add`, `/pair-remove` and
    `/pair-list` take HAP TLV8 (`State = 1`, `Method` 3/4/5) and answer `State = 2`, with
    `Error` 1 (unknown: malformed body, wrong method, re-adding a known controller with a
    different key) or 2 (authentication). `/pair-list` reports every stored controller as admin.
    `POST /configure` answers the accessory identity (`Identifier`, `PublicKey`, `Device_Name`,
    `Enable_HK_Access_Control = true`, `Access_Control_Level = 0`), as shairport-sync does, and
    logs the request keys. All four need a connection that completed pair-setup (transient
    included) or pair-verify (else `Authentication` / RTSP 470); there is no admin-permission
    check (accepted deviations). `PairingStore::list()` enumerates controllers (default empty;
    `MemoryPairingStore` implements it). PlayOnAir persists the store per receiver.

### AirPlay 2 dependencies kept

- **fp-setup / FairPlay M1/M2** (`crypto::fairplay`): the AP2 feature set advertises
  `Authentication4` (bit 14), so iOS runs fp-setup before SETUP.
- **HTTP Digest auth** (`password`, advertised as statusFlags bit 7), `_raop._tcp` TXT base fields
  (`cn`, `da`, `et=0,3,5`, `tp=UDP`, `md`, `vn=65537`): published by AirPlay 2 receivers.
- **Plain RTSP `FLUSH`**: acknowledged (200) for AP2 senders; it only flushes legacy video RTP.

### `video` feature boundary (not removed)

UxPlay-style screen mirroring (`video`, `hls`) runs the legacy, unpaired protocol and still needs
AirPlay 1-era pieces, now compiled **only** with `video`: `raop::rtp` + `raop::buffer` (AES-CBC
ALAC over RTP, including the unused TCP-interleaved arm), `raop::ntp` (NTP timing responder),
`crypto::playfair` + FairPlay tables/garble (`FairPlay::decrypt` for `ekey`), `AlacConfig`, the
`aes`/`cbc`/`ctr` deps, and `AudioSession::audio_flush` callers. PlayOnAir does not enable `video`;
removing video is out of scope.

### Accepted deviations

- **Unlocked anchors still play:** Apple's receiver rejects SETRATEANCHORTIME until its PTP clock
  is locked on the anchor's grandmaster. PlayOnAir must play on hosts where 319/320 are taken
  (macOS with AirPlay Receiver on, non-root CI) and before lock, so it uses the item 20 fallback.
  An anchor that arrives while its timeline is locking waits up to 1 s for lock first, so a session
  that starts right after a new grandmaster still plays through PTP. Pacing stays real time; only
  the absolute start offset can differ from the sender's intent (bounded by 1 s after the first
  unlocked anchor). The PTP follower never transmits and does not take part
  in master election.
- **Pairing admin without admin permission:** HAP lets only admin controllers add, remove or
  list pairings. Here any connection that finished pair-setup can, transient included: the Home
  app's first `/pair-add` arrives on its transient session, and transient pairing already lets
  any LAN client play without a PIN (`acl=0`, `Access_Control_Level = 0`). A LAN client can
  remove the Home controllers; the speaker then has to be added to Home again.

Keep upstream license files. Prefer contributing hard-stop upstream and dropping the vendor when released.
