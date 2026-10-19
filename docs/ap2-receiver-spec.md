# AirPlay 2 receiver behavior spec (Apple reference receiver)

This note records how Apple's own AirPlay 2 receiver behaves, so the vendored AP2 stack in
`vendor/shairplay` can replace guesses with observed behavior. It is a clean-room description:
behavior, message shapes, constants, and algorithms in our own words. It contains no decompiled
code, no Apple tables, and no keys. Function names, log format strings, and addresses appear only
as references so the findings can be re-checked.

## Evidence base

| Source | Identity |
|---|---|
| Live receiver on this Mac | ControlCenter on `127.0.0.1:7000`, `Server: AirTunes/980.77.5`, macOS 27.0.1 (26A434), model Mac16,8 |
| Live receivers on the LAN | Apple TV 4K (AppleTV14,1, `AirTunes/980.77.2`, tvOS build 24J361); Sonos Era 100 (`AirTunes/366.0`, `sdk: AirPlay;2.7.1`, Apple's licensed receiver SDK) |
| Binaries (arm64e, extracted from the dyld shared cache) | `AirPlayReceiver` (sha256 prefix `aceec85bae680346`), `AirPlaySupport` (`986cea3aa6ce4604`), `TimeSync` (`48a6f2c59b0f41fe`), `AirPlaySender` (`11b536177f42c917`, section 7) |
| Tools | `rea` 6.5.0 (6.7.0 for section 7) with the Ghidra 12.1.4 provider, `ipsw dyld disass` (symbolicated), `strings`, `nm`, a capstone ADRP/ADD cross-reference scan |
| Cast speakers (section 7) | Gym speaker (Nest Audio) and Pool Bathroom speaker (Google Nest Mini), Default Media Receiver, measured with a USB microphone on this Mac |

Addresses below are virtual addresses inside the shared cache image of the named framework.
Local symbols are present in this cache, so most functions are named; a few were inlined and are
referenced by the format strings they log.

Confidence scale: **High** (read in the decompiled control flow, consistent with live data),
**Medium** (control flow read, but one step is inferred or delegated to a framework not analyzed),
**Low** (strings or naming only). `[INFERENCE]` marks a conclusion not directly observed.

### Two buffered receivers inside Apple's code

Apple ships two implementations of the buffered audio stream (stream type 103):

- **Hose path** (`APReceiverAudioSessionBufferedHose*` in AirPlayReceiver, engine
  `hoseSBAR_*` / `APSEndpointStreamAudioHoseSBAR*` in AirPlaySupport). The network thread hands
  packets to an engine that renders through a sample-buffer audio renderer driven by a
  synchronizer timebase. This is the production path.
- **Classic path** (`APReceiverAudioSessionBuffered*`). A self-contained ring buffer with its own
  render callback.

`APReceiverAudioSessionCreate` (`0x229fa4958`) picks the Hose path for buffered audio unless the
setup options carry `ReadAudioWithoutRendering`, and the preference `bufferedReceiverHose`
defaults to on. Behavior below describes the Hose path first and notes classic differences where
they explain a constant. Confidence that Hose is the default on macOS 27: High.

## 1. Buffered audio playout timing

### Request shape (SETRATEANCHORTIME)

Parsed by `airplayReqProcessor_requestProcessSetRateAndAnchorTime` (`0x229fdaae8`). Body is a
binary plist dictionary:

| Key | Type | Required | Meaning |
|---|---|---|---|
| `rate` | integer | yes | Only 0 or 1 are accepted. Any other value fails with error -6705 (log `Rate may only be 0 or 1`). |
| `rtpTime` | integer (u32) | when rate is 1 | RTP timestamp of the anchor sample. Alternative form: `mediaTimeValue` + `mediaTimeScale` (CMTime style). |
| `firstAudibleRTPTime` | integer | no | First sample the sender wants heard. Alternative: `firstAudibleMediaTimeValue` / `firstAudibleMediaTimeScale`. |
| `networkTimeSecs` | integer (i32) | when rate is 1 | Anchor time, whole seconds on the PTP timeline. |
| `networkTimeFrac` | integer (u64) | when rate is 1 | Anchor time, binary fraction of a second (units of 2^-64 s). |
| `networkTimeTimelineID` | integer (u64) | when rate is 1 | Clock identity of the grandmaster whose timeline the anchor is expressed on. |
| `networkTimeFlags` | integer (u32) | when rate is 1 | Validity flags for the network time. |

When `rate` is not 1, none of the time keys are read.

Response (binary plist, only when non-empty):

- `firstAvailableSampleTS` (or `firstAvailableMediaTimeValue` / `firstAvailableMediaTimeScale`):
  the earliest sample the receiver still holds.
- `renderStartNetworkTimeSecs`, `renderStartNetworkTimeFrac`, `renderStartNetworkTimeTimelineID`,
  `renderStartNetworkTimeFlags`: present on rate 1 when the receiver chose the start time itself
  (see "receiver-chosen anchor" below).
- `status`: the error code, when the request failed.

Confidence: High.

### Time conversion

`ptpClock_ConvertNetworkTimeToUpTicks` (AirPlaySupport `0x1b852038c`) turns the anchor into host
time:

- Nanoseconds = `secs * 1e9 + ((frac >> 32) * 1e9 >> 32)`. Only the top 32 bits of the fraction
  are used.
- The conversion fails unless `networkTimeTimelineID` equals the current grandmaster identity of
  the receiver's PTP clock (error -71976, log `grandmasterID %llu, inNetworkTime.timelineID %llu`).
- The clock time must also be valid (locked); otherwise the conversion fails.

Confidence: High.

### Rate 1: how each sample's play time is decided

Hose engine, `hoseSBAR_audioHoseSetRateAndAnchorTimeCallbackDispatchInternal`
(`0x1b85440b4`) and `hoseSBAR_audioHosePrepareRTPTimestampsAndTransferBufferForSBAR`
(`0x1b8545248`):

1. The receiver keeps a media timeline: RTP timestamps are extended to 64 bits by counting
   rollovers on the assumption that RTP time increases (`RTP Rollover Detection` logs), then
   converted to media time as `(extended_rtp - base) / sample_rate`.
2. The anchor network time is converted to host time (above). The anchor RTP time is converted to
   media time.
3. The synchronizer timebase is set to rate 1.0 such that media time `M(anchor_rtp)` occurs at host
   time `H(anchor_network_time)`. Every later sample with RTP `r` is presented at
   `H(anchor) + (r - anchor_rtp) / sample_rate`. The renderer and synchronizer apply output-device
   latency compensation themselves.
4. A 30 ms volume fade-in is applied at the anchor point on every start. An optional longer
   fade-in on first start exists behind preferences (`hoseSBARShouldFadeInOnFirstStart`, default
   off, `hoseSBARFadeInDurationOnFirstStartMSecs` default 3000).
5. `firstAudibleRTPTime`, when valid, is converted to media time and stored; it also updates the
   RTP rollover state.
6. Log line for verification: `New anchor data: net: ...; host: ...; media: ...; wall: ...;
   mediaToSynchronizerOffset: ...; bufferedSecs: ...`.

Receiver-chosen anchor: when the request's host time cannot be used, the engine takes the other
branch, converts the media time to host time through the synchronizer, then host time to network
time, and returns that as `renderStartNetworkTime*` (log `Anchored receiver TimingShim timebase:
media=%.6f at PTP=%.6f`). Preference key `hoseSBARSupportsReceiverChoosesAnchor` gates this mode.
Confidence: Medium (branch read). The sender only asks for it with a single receiver (section 7).

Classic path (`APReceiverAudioSessionBufferedSetRateAndAnchorTime`, `0x22a003f58`): on rate 1 it
first requires the clock to be locked (log `Cannot SetRateAndAnchorTime; IsLocked: NO.`), converts
network time to host time, stores the anchor triple (network time, RTP, host time), and the render
callback computes the RTP time due "now" from the anchor plus elapsed host time with skew
correction (`RTP Skew`, `Huge RTP skew reset` logs). It returns the RTP of the first queued packet
as `firstAvailableSampleTime`.

### How far ahead the anchor is

The sender chooses the anchor network time. The receiver does not enforce a minimum or maximum
lead; it only needs the clock locked on the same grandmaster. Confidence: Medium (no clamp found
in either path).

### Anchor in the past

The timebase is anchored at a host time that has already passed, so the media time "now" is
already beyond the anchor. Samples whose presentation time is before the current synchronizer time
are pruned (`hoseSBAR_pruneBufferQueueToTimestamp`, `Purging the bufferStash ... purgeUntil`) and
playback joins mid-stream. Classic path: packets whose end timestamp is before the playhead are
discarded (`Discarding late packet`, `endTS %u < fasAdjustedTS %u; discarding`). Confidence:
Medium.

### Rate 0 (pause)

Hose: `hoseSBAR_audioHoseSetRateAndAnchorTime_RateToZero` stops the synchronizer (timebase rate
0), records the transport time where it stopped, clears the stored anchor (network, host, media),
clears the fade curve, and moves stashed audio back into the input queue. **Buffered audio is kept.**
The next rate-1 request must carry a new anchor. Classic: platform controls `RASP::FlushAudio`
then `RASP::StopAudioIO` stop the output device; the queue is not touched. Confidence: High.

A pause can also be triggered locally: before the receiver forwards a `paus`, `stop`, or `plps`
command to the sender (section 5), it sets its own rate to 0 immediately if buffered audio is
playing. Confidence: High.

### Buffer limits and back-pressure

Hose path:

- The engine high-water mark is a duration: `APSEndpointStreamAudioHoseSBARCreate`
  (`0x1b8535bc8`) receives 120 seconds from `APReceiverAudioSessionBufferedHoseCreate`
  (`0x229febf24`).
- `hoseSBAR_hoseIsAboveHighWater` (`0x1b8543df0`) reports "full" when the queued duration reaches
  120 s, or, once anchored, when the audio already enqueued extends 120 s or more past the current
  synchronizer time and the renderer reports it needs no more data.
- The network thread (`audioSessionBufferedHose_networkThread`, `0x229ff7c74`) reads one framed
  packet at a time into a hand-off queue. When that queue holds more than 31 packets the engine has
  not pulled, it **stops reading the TCP socket** (log `Suspending network reads.`).
  `audioSessionBufferedHose_copyAudioDataBBufsCallback` (`0x229ffa3bc`) drains the whole hand-off
  queue when the engine pulls and resumes reads. The engine only pulls while below high water.
- Net effect: the receiver accepts up to about 120 s of audio ahead of the playhead, then TCP flow
  control throttles the sender. Nothing is dropped for being early.
- `audioBufferSize` in the SETUP stream response is computed, not fixed: 120 s multiplied by the
  largest bytes-per-second among the stream's supported formats (`hoseSBAR_getAudioBytesPerSec`,
  `0x1b8549184`: PCM uses rate times bits times channels; ALAC uses a per-bit-depth factor;
  compressed formats use a per-format bit rate).
- Large discontinuities: the engine holds a buffer across a gap larger than `maxEnqueueGap`
  (5 s, or 0.25 s for unordered sources) until the playhead reaches it.

Classic path constants (for comparison, `APReceiverAudioSessionCreate`):

| Setting (preference key) | Default |
|---|---|
| TCP receive buffer (`tcpBufSize`) | 512 KiB |
| Packet nodes (`nodeCount`) | 15034 for PCM/ALAC, 5200 for AAC and AAC-ELD |
| Ring buffer bytes (`audioBufferSize`) | 21,168,000 (120 s of 44.1 kHz 16-bit stereo) for PCM/ALAC, 5,000,000 for AAC and AAC-ELD |
| Max packet size (`maxPacketSize`) | 65535 |
| Idle data timeout (`tcpAudioDataTimeout`) | 120 s |

Classic reads are suspended when every node is in use or free ring space drops below one max
packet, and resumed when render or flush frees space (`Suspending network reads. Busy buffer node
count = %d`). Confidence: High.

### TCP framing (for reference)

Default buffered connection: each packet is prefixed with a 2-byte big-endian length that includes
the 2 length bytes. Connection type `APAP` uses a 4-byte big-endian length that includes itself.
Buffered RTP sequence numbers are compared modulo 2^24 everywhere (low 24 bits). Confidence: High
for framing, High for 24-bit comparison.

### What the fork does

| Topic | Fork | Apple |
|---|---|---|
| Anchor time | Locked PTP timeline of a session peer maps network time to local monotonic time (`vendor/shairplay/src/net/ptp.rs`); unlocked or foreign timeline uses the pinned receipt-time fallback (`buffered_audio.rs`, `Playout::resolve_anchor`) | Network time on the PTP timeline maps to host time; unlocked anchors are rejected |
| Release | Packet handed to the host at presentation minus the release lead, on a 64-bit RTP timeline. PlayOnAir sets the lead to its Cast read-ahead (8 s, `CAST_READ_AHEAD`, never below `Audio-Latency`) so audio the sender queued ahead is in the host ring when Cast (re)LOADs and goes out as one burst; Cast speakers start on buffered bytes, not time. The sender still sees `Audio-Latency` (2 s), and the speaker plays up to the lead ahead of the sender's timeline (no A/V sync promise) | Renderer presents at `H(anchor) + (r - anchor_rtp) / rate`, compensating device latency |
| Start alignment | No nudge; packets whose end is behind the playhead are pruned; 30 ms fade-in per anchor | Same |
| Rate 0 | Stop, buffer kept, anchor cleared | Same |
| Non-0/1 rates | Plist `status` -6705 | Same |
| Buffer depth | 120 s high water (resume below 119 s), reader suspends TCP reads, nothing dropped | 120 s high water, no drops, TCP back-pressure only |
| `audioBufferSize` | 120 s x 192000 B/s (PCM bound of 48 kHz 16-bit stereo; AAC per-format bit rates unknown) | 120 s times max bytes per second of the supported formats |
| Response | `firstAvailableSampleTS` when audio is buffered, `status` on error; no receiver-chosen anchor | `firstAvailableSampleTS`, optional `renderStartNetworkTime*`, `status` on error |

## 2. FLUSHBUFFERED

### Request shape

Parsed by `airplayReqProcessor_requestProcessFlushBuffered` (`0x229fd9cec`):

| Key | Required | Notes |
|---|---|---|
| `flushFromSeq` | no | Absence selects flush-all. |
| `flushFromTS` | no | Or `flushFromMediaTimeValue` / `flushFromMediaTimeScale`. |
| `flushUntilSeq` | yes | Any read error other than "present" fails the request. |
| `flushUntilTS` | yes in practice | Or `flushUntilMediaTimeValue` / `flushUntilMediaTimeScale`. The classic path rejects an invalid until time. |

A range flush ("within sample range") is selected only when `flushFromSeq` is present **and**
`flushFromTS` is a valid time. Otherwise the request is a flush-all up to `flushUntilSeq`. The
request fails if the session is not yet prepared for media.

Response: when the returned time is RTP based, an `RTP-Info: rtptime=<u32>` header; when media-time
based, a plist with `rtpTime` (or `mediaTimeValue` / `mediaTimeScale`). Confidence: High.

### Flush-all

Hose engine (`hoseSBAR_audioHoseFlushCallbackDispatchInternal`, `0x1b8541eb0`):

- If playing, the synchronizer is stopped (rate 0) and the drift monitor suspended. Playback stays
  stopped until the next SETRATEANCHORTIME.
- **All** buffered audio is discarded: the renderer, the internal queues, and the stash are reset.
- Anchor state (network, host, media), the RTP rollover state, and the first-audible time are
  reset, so the next packets may start a new RTP epoch.
- Pending flush operations are cleared and one new operation is added: incoming packets with
  sequence number **strictly before** `flushUntilSeq` are dropped. The packet whose sequence equals
  `flushUntilSeq` is kept (exclusive bound).
- The operation retires when a packet arrives at `flushUntilSeq + 20` or later; packets in
  `[flushUntilSeq, flushUntilSeq + 20)` pass through and keep it alive to catch reordered
  stragglers.
- Timed metadata scheduled against the old timeline is flushed.

Classic path (`audioSession_processFlushQueue`, `0x22a003608`): flushes the output device, sets
rate to 0, invalidates the anchor, and drops buffered packets with sequence before
`flushUntilSeq` (exclusive). A flush-all replaces any queued range flushes. Confidence: High.

### Range flush

Hose engine (`hoseSBAR_audioHoseFlushWithinSampleRangeDispatchInternal`, `0x1b8542584`):

- Playback is **not** stopped and the anchor is kept.
- Buffered audio whose media time is at or after `flushFromTS` is removed from the internal queues,
  and the renderer is asked to flush from that time onward (pulling more data pauses while that
  request is in flight, log `flush from time in flight`). This trims everything from the from-time
  to the end of what is buffered, not only up to `flushUntilTS`.
- A pending operation drops incoming packets with sequence in `[flushFromSeq, flushUntilSeq)`:
  from inclusive, until exclusive. It retires at `flushUntilSeq + 20`, like flush-all.
- The RTP rollover state is adjusted from `flushFromTS`; timed metadata in the range is flushed.

Classic path: drops buffered packets with sequence in `[flushFromSeq, flushUntilSeq)`; the
timestamp fields are logged but not used for the decision. Range requests that arrive while
another is pending are queued and applied as packets arrive. Confidence: High for sequence rules,
Medium for the "trim to end of buffer" consequence (renderer behavior is inside AVFoundation).

### RTP epoch across track skip

Seq numbers decide what is dropped; timestamps decide where the media timeline cuts. After a
flush-all the rollover and anchor state reset, so a new RTP epoch is accepted with no special
handling; the sender then sends SETRATEANCHORTIME with an anchor on the new epoch. After a range
flush the existing anchor stays valid, so the replacement audio must continue the old RTP epoch.
Confidence: Medium (`[INFERENCE]` that senders follow this, from the receiver's state handling).

### What the fork does

- Follows the rules above (`vendor/shairplay/src/raop/buffered_audio.rs`, `Playout::flush` /
  `Playout::admit`): 24-bit modulo compare, flush-all without `flushFromSeq` (or `flushFromTS`),
  range flush otherwise, from inclusive / until exclusive, rules retired at `flushUntilSeq + 20`.
  Flush-all stops playback and resets anchor and RTP epoch; range flush keeps playing, trims from
  `flushFromTS` to the end and continues the epoch from the cut.
- Only a flush-all notifies the host (`AudioSession::on_flush`): everything the host already holds
  predates the next anchor, so PlayOnAir re-LOADs Cast once that anchor's audio is queued. A range
  flush keeps playing and trims only the queue, so the host is not told (audio already handed over
  past `flushFromTS`, at most the release lead, still plays).
- Missing `flushUntilSeq` answers plist `status` -6705. A successful flush answers empty (no
  `RTP-Info`).

## 3. PTP clock

### Setup and peers

- SETUP carries `timingProtocol` (`PTP` or `NTP`). For PTP, `__TimingSetup` (`0x22a017224`)
  starts the receiver's network clock (`APSNetworkClockPTP`, backed by the system TimeSync stack:
  `APSTimeSyncNetworkClock` wrapping an 802.1AS clock object, kernel side `IOTimeSyncFamily` with
  the `IOgPTPPlugin`), then:
  - `timingPeerInfo` from the sender (a peer dictionary) is added as a peer;
  - `timingPeerList` (array of peer dictionaries) replaces the peer set;
  - the response carries `timingPeerInfo` = the receiver's own peer dictionary.
- Peer dictionary keys (serializable form, `APSNetworkClockCopyPeerAsSerializablePeerDictionary`
  `0x1b8509ea4`): `ID`, `Addresses` (array of address strings), optional `HTAddress`. Internal-only
  keys (`InterfaceName`, `PreferredAddress`, `IsOverridden`, `IsResponsiveAudioPeer`) are stripped.
  Peers may also carry `ClockID`, `ClockPorts`, `SupportsClockPortMatchingOverride`.
- SETPEERS and SETPEERSX (`airplayReqProcessor_requestProcessSetPeers`, `0x229fdb1f8`) are ignored
  unless the session's timing protocol is PTP. Body: a plist array. SETPEERS entries are address
  strings; SETPEERSX entries are peer dictionaries (above).
- `ptpClock_SetPeers` (`0x1b8520820`) treats the list as the complete set: peers missing from it are
  removed, the rest are added or updated, and per-peer unicast clock ports are opened for each
  usable address. If the receiver's own peer info changed, the response body is that dictionary;
  otherwise it is empty.
- When the receiver's local peer info changes later, it pushes
  `{type: "updateTimingPeerInfo", value: <peer dict>}` on the event channel
  (`__SendTimingPeerInfoAsyncIfNeeded`, `0x22a016cec`).

Confidence: High.

### Who is master

SETPEERS does not pick a master. It only defines which peers the clock talks to. Master selection
is the standard best-master election inside the system gPTP stack (TimeSync debug output lists
announce priority 1/2, clock class, accuracy, offset scaled log variance, steps removed). The
`networkTimeTimelineID` in every anchor is the elected grandmaster's clock identity, and anchors on
any other identity are rejected. Apple receivers can be grandmaster themselves. Confidence: High
for "SETPEERS defines peers, election picks the master"; the election parameters are in the kernel
and were not analyzed.

### Grandmaster changes and lock

- Lock state is tracked (`IsLocked`, `LockStateChanged`, metrics `TimeToLockMs`,
  `grandMasterChangesCount`, `TimeToChangeGrandMasterUs`).
- Before lock, or with a timeline mismatch, anchor conversion fails and SETRATEANCHORTIME returns an
  error `status` (classic path logs `Cannot SetRateAndAnchorTime; IsLocked: NO.`). Nothing plays
  until a valid anchor arrives. Confidence: High.
- After a grandmaster change the receiver re-expresses its anchor on the new timeline (logs
  `Error updating our anchor to a consistent GM on clock %@`, `Grandmaster ID mismatch: need %#llx
  used %#llx`). Confidence: Medium.
- Update rate is configurable (`ptpUpdateRateMs`, unset by default; `AirPlayPerf_PTPNonTightSyncRate`
  per-port sync rate).

### Unknowns

- PTP domain number, priority1/priority2 values, and offset smoothing: implemented in the kernel
  gPTP plugin, which is not on disk outside the kernelcache. To settle: capture Announce and Sync
  messages on UDP 319/320 during a session to this Mac (needs root for `tcpdump`), or extract
  `IOgPTPPlugin` from the kernelcache and read its announce construction.

### What the fork does

- Passive PTP follower (`vendor/shairplay/src/net/ptp.rs`): per grandmaster clock identity,
  offset from Sync/Follow_Up through a minimum-delay window filter; it never transmits, so it
  does not take part in master election. Ports default to 319/320 and are shared per process.
- SETUP `timingPeerInfo` adds and `timingPeerList` / SETPEERS / SETPEERSX replace the session
  peer set; SETPEERS is ignored unless SETUP negotiated PTP. Responses stay empty (the local peer
  info never changes), and no `updateTimingPeerInfo` event is sent.
- SETUP returns `timingPeerInfo` with only the local IP as `Addresses` and `ID`, which matches the
  key shape Apple uses.
- Before lock, on another timeline, or when the ports cannot be bound, anchors use the pinned
  receipt-time fallback instead of being rejected (PlayOnAir must play on such hosts).

## 4. Advertising

### Live values

| Field | Mac (receiver) | Apple TV | Sonos Era 100 | Fork (no PIN) |
|---|---|---|---|---|
| features | `0x38174FDE4A7FCFD5` | `0x3C177FDE4A7FDFD5` | `0x0801C340445F8A00` | `0x0001C340405FCA00` |
| featuresEx (`fex`) | `1c9/St5PFzg2IYxA` | `1d9/St5/Fzw4oY7cDg` | `AIpfREDDAQgQIQ` | absent |
| statusFlags | `0x204` | `0x18644` | `0x4` | `0x4` / `0x204` / `0x604` (`+0x80` with a password) |
| volumeControlType | 4 | 4 | 3 | 3 |

`fex` is base64 of the full little-endian feature bit array; its first 8 bytes equal `features`.
Extended bits (64 and up) observed: Mac 65, 66, 68, 69, 72, 77, 82, 83, 87, 94; Sonos 68, 72, 77.

### How Apple builds the feature set

`sysInfo_createFeaturesInternal` (`0x229fb2e64`) adds bits in this pattern (bit names are the
community names already used in `vendor/shairplay/src/net/features.rs`; Apple's binary carries no
names):

- Always: 9, 11, 14, 15, 16, 17, 18 to 22, 25, 50 (audio, redundancy, FairPlay auth, artwork,
  progress, DAAP text, audio format bits, bplist now-playing), 30, 38, 43, 48, 40 (buffered
  audio), 52 (SETPEERSX), 59, 60, 61, and extended 68, 77, 82.
- With the PTP clock enabled: 41 (PTP) and extended 94.
- Without a pairing-restriction flag: 46 (HomeKit pairing), 27 (legacy pairing), extended 65.
- Video and screen: 0, 2, 4, 6, 7, 8, 10, 33, 34, 35, 39, 42, 49, extended 69.
- Preference driven: extended 72 (`bufferedAPAP`, default on, buffered audio over the `APAP`
  framing), 92 (`APAT_Buffered`), 95 (`APAT_Multicast`), 84 (`BufferedQUIC`).
- Apple's own devices never set bit 47 (transient pairing); the Sonos SDK receiver does.

Confidence: High for the conditions, Low for any bit's semantic name.

### statusFlags

`sysInfo_getStatusFlags` (`0x229fb5918`) always sets bit 2, then adds: bit 3 or bit 7 (mutually
exclusive pairing/password modes), bit 9, bit 19 (only with bit 2), bit 6, bit 18, bit 8, bit 10,
bit 11, bit 17, bit 20 from individual receiver settings. The Apple TV's bits 15 and 16 come from
elsewhere. Public names (unconfirmed in the binary): 2 audio link, 3 PIN required, 6 supports
cloud, 7 password required, 9 one-time pairing required, 10 set up for HomeKit access control,
11 relay. Confidence: High for the bit sources, Low for names.

### /info key set

From `sysInfo_copyInfoDictInternal` (`0x229fb20c8`) and the three live responses:

- Identity: `deviceID`, `macAddress`, `name`, `model`, `pi`, `psi`, `pk` (plist **data**, 32 bytes),
  `sourceVersion`, `protocolVersion`, `osBuildVersion`.
- Capabilities: `features`, `featuresEx`, `statusFlags`, `keepAliveSendStatsAsBody`,
  `supportedFormats` (bitmask per stream kind: `audioStream`, `bufferStream`, `screenStream`,
  `lowLatencyAudioStream`), `supportedAudioFormatsExtended` (array of format indices per stream
  kind), `playbackCapabilities` (video and UI flags), `volumeControlType`, `initialVolume`, and
  conditionally `muteControlType`, `isMuted`, `maximumNumberOfOutputChannels`.
- Video only: `canRecordScreenStream`, `hasUDPMirroringSupport`, `receiverHDRCapability`,
  `screenDemoMode`, `forwardFrameUserData`.
- `txtAirPlay`: the `_airplay._tcp` TXT record bytes. `senderAddress` is echoed back.
- The SDK receiver (Sonos) instead sends `manufacturer`, `sdk`, `PTPInfo`, `OSInfo`,
  `firmwareBuildDate`, `firmwareRevision`, `hardwareRevision`, `build`, `keepAliveLowPower`,
  `nameIsFactoryDefault`, and omits `pk`, `supportedFormats`, `initialVolume`.

Audio format indices (bit `n` of a `supportedFormats` mask is index `n`;
`APAudioFormatCopyAudioFormatsToReadableString`, `0x1b857df88`): 18 ALAC/44100/16/2,
19 ALAC/44100/24/2, 20 ALAC/48000/16/2, 21 ALAC/48000/24/2, 22 AAC-LC/44100/2, 23 AAC-LC/48000/2,
24 AAC-ELD/44100/2, 25 AAC-ELD/48000/2, 41 AAC-ELD/48000/5.1, 42 AAC-ELD/48000/5.1.2. Mac
`bufferStream` = [21, 22, 23, 42, 25, 41]; Sonos `bufferStream` = [18, 22].

### TXT records

- `_airplay._tcp` (Mac): `act acl deviceid fex features rsf flags gid igl gcgl model at protovers pi
  psi pk srcvers`. Apple TV adds `btaddr osvers vv=1`; Sonos adds `fv manufacturer serialNumber
  bat` and sets `gid` equal to `pi`.
- `_raop._tcp` (Mac): `cn=0,1,2,3 da=true et=0,3,5 ft sf md=0,1,2 am pk tp=UDP vn=65537 vs vv`.
  Apple TV adds `ov`.

### Diff against the fork

The fork now follows this section (`build_info_dict` in `vendor/shairplay/src/raop/handlers_ap2.rs`,
`airplay_txt` and `AirPlayServiceInfo::new` in `vendor/shairplay/src/net/mdns.rs`,
`ap2_status_flags` in `vendor/shairplay/src/raop/config.rs`). Remaining differences, on purpose:

- `/info` omits `featuresEx` (until extended bit 72 exists), `psi`, `osBuildVersion`,
  `playbackCapabilities`, `senderAddress`, and the screen keys. `supportedFormats` has only
  `audioStream` (ALAC 18 to 21) and `bufferStream` (AAC-LC 22, 23); `supportedAudioFormatsExtended`
  is `{bufferStream: [22, 23]}`.
- `_airplay._tcp` uses the Sonos SDK key set: no `fex`, `psi`, `act`, `at`, `vv`, `btaddr`,
  `osvers`; `rsf=0x0`; `gid` equals `pi`; `deviceid` is lowercase hex.
- `_raop._tcp` keeps `cn=0,1` (the realtime path decodes PCM and ALAC only, as the SDK receiver
  advertises) and `fv`; it sends `tp=UDP` and no `pw` or `ov`.
- `updateInfo` carries the full `/info` dictionary but is still sent once at SETUP, not on
  system info changes.
- Bits 15 and 17 are set; bits 52 and 59 to 61 stay off (no protocol path). The exact mask still
  needs an iPhone discovery retest.

### Recommended audio-only mask

Keep the tested base and add metadata bits only once their handlers exist:

| Bit | Keep | Justification |
|---|---|---|
| 9 | yes | Audio receiver. Set by every receiver observed. |
| 11 | yes | Audio redundancy. Set by Apple and the SDK. |
| 14 | yes | FairPlay auth path the fork implements. The SDK uses MFi (26) instead, which the fork cannot do. |
| 16 | yes | Progress via `text/parameters`. Cheap to support (section 5). |
| 18, 19, 20, 22 | yes | Audio format bits shared with the SDK receiver. |
| 30 | yes | Unified advertiser info. Always set by Apple. |
| 38 | yes | Unified media control. Always set by Apple and the SDK. |
| 40 | yes | Buffered audio (stream type 103). |
| 41 | yes | PTP. Apple sets it only with a working PTP clock; the fork needs real clock tracking (section 3) to honor it fully. |
| 46, 48 | yes | HomeKit pairing and CoreUtils encryption, used by the fork's pairing. |
| 47 | yes when no PIN | Transient pairing, as the SDK receiver does. Apple devices omit it because they never use transient pairing. |
| 15, 17 | add, then retest | Artwork and DAAP text. The SDK sets both. The fork already parses `image/jpeg`, `image/png`, and DMAP in SET_PARAMETER (`vendor/shairplay/src/raop/handlers.rs:228-253`). |
| 50, 52, 59, 60, 61 | no | bplist now-playing, SETPEERSX, stream-connections SETUP, media data control, RFC 2198. Each implies a protocol path the fork lacks. |
| 0 to 8, 10, 33 to 35, 39, 42, 49 | no | Video, photo, screen. |

Resulting masks: base `0x0001C340405D4A00` (previous), with metadata `0x0001C340405FCA00` (adopted). Keep `fex`
unadvertised until extended bit 72 (APAP framing) is implemented. Advertise
`volumeControlType = 3` (absolute, receiver-side hardware volume, as the SDK receiver does,
because the Cast device applies volume) and `supportedAudioFormatsExtended = {bufferStream: [22]}`
plus any other format the buffered decoder handles.

## 5. Reverse control and metadata

### Receiver to sender

All of these travel on the event channel as `POST /command RTSP/1.0` with a binary plist body
(`AirPlayReceiverSessionSendCommandForObject`).

Playback commands (`AirPlayReceiverSessionSendMediaRemoteCommand`, `0x22a014e80`):

```
{
  "type": "sendMediaRemoteCommand",
  "modernMediaRemoteCommand": "<decimal MediaRemote command number>",   // when known
  "value": "<four-character command code>",                             // when known
  "params": { ... }                                                     // optional
}
```

Command codes (pairs read from `AirPlayReceiverServerSendDACPCommands`, `0x229fbea88`, which maps
each code to its DACP path for older senders):

| Code | Meaning | Code | Meaning |
|---|---|---|---|
| `play` | play | `paus` | pause |
| `plps` | play/pause toggle | `stop` | stop |
| `plrs` | play resume | `plsp` | play spec |
| `nitm` | next item | `pitm` | previous item |
| `rsti` | restart item | `nchp` / `plch` | next / previous chapter |
| `nctn` / `plcn` | next / previous container | `ndrp` / `plgr` | next / previous group |
| `bffd` / `brwd` | begin fast-forward / rewind | `dpua` | discrete pause |
| `shsn` / `shgt` | shuffle songs / toggle | `rpad` | repeat advance |
| `vlup` / `vldn` | volume up / down | `mutt` | mute toggle |
| `dvlc` | absolute volume (see below) | `pbal` / `pbpr` | allow / prevent playback |

Volume reports (`AirPlayReceiverSessionHandleVolumeUpdate`, `0x22a014a04`): when the local volume
or mute changes and differs from the last reported value, the receiver checks the sender's client
version:

- Sender version 354.54.6 or newer: `sendMediaRemoteCommand` with `value: "dvlc"` and
  `{volume: <slider 0..1>, isMuted: <bool, when the sender supports mute>}`, added both at top level
  and as `params`.
- Older senders: DACP HTTP request to the sender's remote server:
  `setproperty?dmcp.device-volume=<dB>` (slider converted with the curve in section 6).

Other receiver-initiated messages: `updateInfo` (section 4), `updateTimingPeerInfo` (section 3).

Confidence: High for message shape and codes. The MediaRemote command numbers were not extracted;
to settle, decompile `-[AirPlayReceiverPlatform handleMRCommand:translatedAPCommand:withOptions:]`
(`0x229fa9534`). Sending only `value` matches what pre-modern senders expect.

### Sender to receiver: SET_PARAMETER

`airplayReqProcessor_requestProcessSetParameter` (`0x229fd8cb0`) dispatches on Content-Type:

- `image/*`: artwork bytes, stored as `artworkData` with the MIME type as `artworkMIMEType`.
  `application/x-dmap-tagged` and image updates carry an apply time (`applyTS`, from the RTP-Info
  header) so they take effect when that RTP time plays. Confidence: Medium for `applyTS` source.
- `application/x-dmap-tagged`: an `mlit` container. Tags read: `minm` title, `asar` artist, `asal`
  album, `asgn` genre, `ascp` composer, `astm` duration (u32 ms), `astn` track number (u16), `astc`
  track count (u16), `asdn` disc number (u16), `asdc` disc count (u16), `asdk` stream flag (1 byte,
  1 = stream), `caps` play state (1 playing, 2 or 3 stopped/paused, 4 fast-forward, 5 rewind; mapped
  to rate 1, 0, 2, -1), `miid` item id (u32), `mper` persistent id (u64), `aeSI` (u32).
- `text/parameters`: lines of `name: value`.
  - `volume: <float dB>` (section 6). Ignored on a remote-control-only session.
  - `progress: <start>/<current>/<end>` as RTP timestamps. Elapsed is `(current - start) / rate`,
    duration is `(end - start) / rate` (`APReceiverAudioSessionBufferedConvertProgressRTPTimeToSecond`).

### Sender to receiver: POST /command

`airplayReqProcessor_requestProcessCommand` (`0x229fe0b1c`) reads `type` and `params`:

| `type` | Effect |
|---|---|
| `updateMRNowPlayingInfo` | Now-playing dictionary with `mergePolicy` (`update` or `replace`). Keys used: `title`, `artist`, `album`, `duration`, `elapsedTime`, `rate`, `trackNumber`, `totalTracks`, `uniqueID`, `artworkData`, `artworkMIMEType`. |
| `updateMRSupportedCommands` | Serialized list of commands the sender accepts (`mrSupportedCommandsFromSender`). Use it to enable or disable controls. |
| `updateMRPlaybackState` | `mrPlaybackState` (integer). |
| `updateMRNowPlayingClient` | Now-playing app descriptor. |
| `setMRInfo` | Opaque MediaRemote info. |
| `setSenderDisplayLatencyMs` | `value` in ms; subtracted from total latency. |
| `duckAudio`, `unduckAudio`, `fadeAudio` | Volume ducking and fades on the output. |
| `mute`, `unmute` | Mute state (`isMuted`). |
| `changeRelativeVolume` | Relative volume step. |

Senders use the plist path (`updateMRNowPlayingInfo`) when the receiver advertises bit 50 and the
DMAP path otherwise. `[INFERENCE]` from which bits Apple pairs with each handler; to settle, capture
a sender against a receiver that advertises 15, 16, 17 without 50.

### What the fork assumes

- `POST /command` bodies are logged and dropped (`vendor/shairplay/src/raop/handlers_ap2.rs:954-969`).
- The event channel only carries the SETUP-time `updateInfo`
  (`vendor/shairplay/src/raop/event_channel.rs:24-42`).
- No DACP client; the fork declares DACP out of scope (`vendor/shairplay/src/lib.rs:16`).
- SET_PARAMETER handles `volume`, `progress`, `image/jpeg`, `image/png`, and DMAP
  (`vendor/shairplay/src/raop/handlers.rs:216-257`). Differences from Apple: only the first
  `text/parameters` line is read, other `image/*` types are ignored, and `applyTS` timing is not
  honored, so metadata and artwork switch on arrival instead of when the matching RTP time plays.

Implementing playback control needs only the event channel message above with `value` set; volume
reports need `dvlc` with a slider value.

## 6. Volume

Conversions in AirPlaySupport (`APSVolumeConvert*`, `0x1b85986e4` to `0x1b8598918`):

| Conversion | Rule |
|---|---|
| dB to linear gain | `0` when dB is at or below -30, else `10^(dB/20)`, capped at 1 |
| linear gain to dB | `20 * log10(gain)`; `-144` when gain is 0 or less |
| slider to dB | linear map of slider `0..1` onto `-30..0` dB |
| dB to slider | linear map of `-30..0` dB onto `0..1` (a clamped variant exists) |
| slider to gain | slider to dB, then dB to gain |

So the wire range is `-30..0` dB for audible volume and `-144` means mute. The iOS slider position
is linear in dB across that range.

`SET_PARAMETER` `volume: <dB>` sets the session's software volume (or the system `VolumeDB` when no
session is bound); `GET_PARAMETER volume` answers `volume: %f`. Receivers report mute with the
`-144` sentinel and may emulate mute by setting gain to 0.

`volumeControlType` values (`APSGetAPVolumeControlTypeName`, `0x1b8598c94`): 0 none, 1 relative,
2 relative with current value, 3 absolute hardware, 4 absolute software. Mac and Apple TV report 4;
the Sonos SDK receiver reports 3.

What the fork assumes: the app maps dB straight to Cast level with `10^(dB/20)`
(`crates/play-on-air/src/airplay/mod.rs:719-745`), which leaves about 3% level at -30 dB where Apple
mutes, and makes the bottom half of the iPhone slider nearly silent on a Cast device whose level is
already perceptual. Mapping Cast level to Apple's slider value, `(dB + 30) / 30` clamped to `0..1`
with `-144` as 0, follows the iPhone slider one to one. `volumeControlType` is not advertised.

### Volume in multi-select groups

Question from live traces: in a multi-select session (`isMultiSelectAirPlay = true`, `groupUUID`
kept across a speaker switch) the iPhone streams `volume: <dB>` at about 100 Hz during a drag and
interleaves a constant `volume: -21.0` every 0.3 to 1 s on the same RTSP session.

**Receiver side (AirPlayReceiver).** Confidence: High.

- `SET_PARAMETER` `volume:` is parsed with `strtod` and written as the session property `volume`
  (`airplayReqProcessor_requestProcessSetParameter`, volume branch near `0x229fd9000`). With no
  session it goes to the system `VolumeDB`. A remote-control-only session refuses it.
- `AirPlayReceiverSessionPlatformSetProperty` (`0x229fabbb4`) converts dB to slider and calls
  `_SetSWVolumeSliderValue` (`0x229facc88`) with "not programmatic". That function ignores a value
  whose slider position differs from the current one by less than 0.01 (about 0.3 dB,
  `APSVolumeSliderValuesAreNoticeablyDifferent`), otherwise stores it, writes the system
  `VolumeDB`, and applies linear gain to every audio session of the receiver session
  (`_SetSessionSoftwareVolumeSliderValue`, `_ProcessSWVolumeChanged`).
- There is one volume per receiver session. No group or master volume, no multi-select branch
  (`isMultiSelectAirPlay` is only read at SETUP for parent-group bookkeeping), no ramp or
  smoothing on volume writes (the only fade is the 30 ms fade-in at a rate-1 anchor), and no
  filtering of repeated or stale values beyond the 0.01 slider dead band. Last write wins.
- Reporting back: `AirPlayReceiverSessionHandleVolumeUpdate` (`0x22a014a04`) sends `dvlc`
  (section 5) when the receiver volume changes and differs from the last value reported. It first
  checks an echo-suppression list (`_RemoveVolumeOrMuteRequest`, session offset `0x2d8`), but no
  code in this build appends to that list (only cleanup releases it), so suppression never hits.
  `[INFERENCE]` A sender-driven change therefore comes back to the sender as a `dvlc` report once
  the system volume notification fires. Confidence for the echo: Medium (the notification path
  from `VolumeDB` to `sendVolumeUpdate` was not traced end to end).

**Sender side (AirPlaySender, extracted for this question).** Confidence: Medium.

- Group volume lives entirely in the sender. A multi-select route is an aggregate endpoint
  (`APEndpointCluster`) that fans volume out per sub-endpoint (`<AirPlayVolumeSync> Setting %@ to
  %@ on subEndpoint`), and also listens for volume changes coming from each sub-endpoint
  (`endpointCluster_handleSubEndpointVolumeDidChange`: `received volume change ... from
  subEndpoint`, then either forwards or ignores it).
- The sender keeps a per-room volume: `Saving the volume level %@ from subEndpoint as room volume
  for room %@` and `Setting the subEndpoint volume level to %@ to match the room volume for room
  %@`. On activation it applies an initial volume to the volume leader and fixes zero, loud or
  invalid initial volumes to defaults of -20 dB or -27 dB
  (`endpointdelegate_getInitialVolumeDBAndIsMuted`). -21.0 is not one of those defaults.
- Per-endpoint send path drops duplicates (`Volume %.3f dB already sent to endpoint; Dropping`)
  and older values (`Newer volumeDB ... set on endpoint; Dropping`), so a value that keeps coming
  back is being re-set by a different owner, not retransmitted.
- `[INFERENCE]` The interleaved constant is the sender's stored room or sub-endpoint volume being
  re-asserted by its sync loop while the slider stream writes the live value. A receiver that
  never reports its own volume (`dvlc`) leaves that stored value unchanged, which matches the
  observation that it does not happen when a single receiver is picked directly (no cluster).

**volumeControlType.** 3 is absolute hardware, 4 absolute software. Both are absolute, so the
sender sends `volume:` dB either way; the sender uses the type for its supported-control flags
and mute handling. No code path found that would change the multi-select resend. Confidence:
Medium-low.

**Rule for PlayOnAir.**

1. Apply volume exactly as Apple does: last write wins, ignore changes under 0.01 slider
   (`|(dB1 - dB2) / 30| < 0.01`), no group special case.
2. After applying a sender volume, report it back with `sendMediaRemoteCommand` `value: "dvlc"`,
   `volume: <slider 0..1>` (section 5) on the event channel, coalesced so at most one report goes
   out per 250 ms and the final value of a drag is always reported. This is the only receiver
   output that feeds the sender's sub-endpoint and room volume state.
3. Advertise `initialVolume` as the volume actually applied, so the sender's stored value starts
   in sync.

Rule 2 is the untested part. To settle: run the multi-select switch with rule 2 enabled and check
that the constant stops; or capture the event channel of an Apple TV in the same multi-select
scenario and confirm it emits `dvlc` after sender-driven changes.

## 7. Multi-room sync

How Apple keeps several receivers in step, and what a Cast-backed receiver can reproduce.

### Sender: one anchor for the whole group

- `bufferedAudioEngine_handleSetRateToOne` (AirPlaySender `0x1c88f67e4`) builds one anchor and
  `bufferedAudioEngine_handleSetRateToOne_sendAnchorToAllHoses` (`0x1c88f7760`) sends the same
  SETRATEANCHORTIME to every receiver. The anchor's media time is the first media time minus a
  startup delay from `bufferedAudioEngine_getStartUpDelay` (`0x1c881ef80`): 400 ms (750 ms out of
  pause), 300 ms / 600 ms with the priming optimization, 200 ms when the sender also plays locally,
  or the `bufferedStartUpDelayMS` preference. Confidence: High.
- The buffered adapter moves the host anchor earlier by `audioLatency - audioLatencyAdjust`
  (`audioEngineBufferedAdapter_SetRateAndAnchorTime_Stage1`, `0x1c886cbd4`). Defaults come from
  the sender's own settings: 2000 ms (`APSAudioLatencyForAVMs`, AirPlaySupport `0x1b8507418`) and
  -250 ms (`APSAudioLatencyAdjustForAVMs`, `0x1b8507938`), so 2.25 s. Confidence: High.
- No receiver value moves the group anchor or delays the other receivers. On this path the sender
  reads only `SupportsReceiverChoosesAnchor` and `ClusterType` from a receiver. `audioLatencies` and
  `outputLatencyMicros` appear in none of the three binaries. Confidence: High.
- Receiver-chosen anchor: `bufferedAudioEngine_shouldUseReceiverChoosesAnchor` (`0x1c88f63ec`)
  requires a buffered (not real-time) engine, exactly one receiver, no `ClusterType`, and
  `SupportsReceiverChoosesAnchor`. The sender then sends GETANCHOR and starts its timebase on the
  receiver's answer (`bufferedAudioEngine_startRemoteMediaTimebaseWithReceiverAnchor`,
  `0x1c88f8e64`), retrying up to 50 failures. A group never uses it. Confidence: High.

### Receiver: output latency and drift

- Output latency, classic path: `__MainAudioEnsureSetup` (`0x229ffa9dc`) sums the output device's
  `kAudioDevicePropertyLatency`, `kAudioDevicePropertySafetyOffset` and the largest stream latency
  into `PlatformAudioLatency` (samples). `APReceiverAudioSessionBufferedUpdateLatency`
  (`0x22a006740`) sets `audioLatencyOffset = timelineOffset + senderMinLatency - platformLatency`,
  and `APReceiverAudioSessionBufferedReadAudio` (`0x22a0046d0`) adds it to packet timestamps, so
  audio reaches the device early by its own latency. The buffered path has no cap; the realtime
  path forces a -10 s offset when platform latency exceeds 500 ms and the sender's maximum.
  Confidence: Medium (field mapping read from log arguments).
- Hose path (the production path): output latency is applied inside the renderer, and the
  synchronizer follows the PTP timebase with the varispeed time-pitch algorithm by default
  (`hoseSBARPrefersVarispeedTimePitchAlgorithm`, default 1). `APSClockDriftMonitor`
  (`0x1b85671b0`) polls every 1 s and only logs when host and remote timebases drift apart by more
  than 15 ms. Confidence: High.
- Drift loop, classic path (inlined in `APReceiverAudioSessionBufferedReadAudio`): once per
  second, error = RTP due now on the PTP timeline minus the device playhead. A PID
  (`PIDInit(0.7, 0.2, 1.2, 0.97, -50, 50)`) turns it into a rate: drop one sample per period when
  behind, duplicate one when ahead, at most 1000 samples/s. At 4410 samples or more (100 ms at
  44.1 kHz) it steps the offset instead (`bufferedRTPOffsetResetThreshold`). Confidence: Medium
  (PID argument order inferred).
- No millisecond sync target appears in the binaries. The bounds are the 15 ms drift log and the
  ~100 ms step.

### Mapping to a Cast speaker

Sync lives in each receiver, so PlayOnAir has to make Cast play RTP `r` at
`H(anchor) + (r - anchor_rtp) / sample_rate` by itself:

| Apple receiver | Cast-backed equivalent |
|---|---|
| Device output latency from CoreAudio | Per-model PLAY-to-audible delay (measured below) |
| Device playhead | MEDIA_STATUS `currentTime` minus a per-model offset (measured below) |
| Drop or duplicate samples, at most 1000/s | Drop or duplicate samples in the FLAC PlayOnAir encodes; it reaches the speaker after the Cast buffer ahead of it |
| Step past ~100 ms | Re-LOAD at the due position |

### Cast measurements

Method: a FLAC 48 kHz stereo track with a 12 ms chirp every 2 s, served as chunked HTTP to the
Default Media Receiver from a sender on this Mac. A USB microphone 1 to 2 m away records the
speaker; each chirp is located by matched filter and timestamped from the CoreAudio ADC host time.
All times are on the Mac's monotonic clock and include the constant air and microphone path
(a few ms). Each trial: LOAD with `autoplay: false`, wait 2 s after PAUSED, send PLAY, poll media
status at 5 Hz for 12 s.

| | Nest Audio, BUFFERED (20) | Nest Audio, LIVE (10) | Nest Mini, BUFFERED (20) | Nest Mini, LIVE (10) |
|---|---|---|---|---|
| PLAY sent to media 0 audible, mean | 152 ms | 140 ms | 172 ms | 172 ms |
| Standard deviation | 12.8 ms | 4.8 ms | 6.7 ms | 5.9 ms |
| Range | 140 to 202 ms | 136 to 153 ms | 157 to 184 ms | 159 to 177 ms |
| `currentTime` ahead of audible, mean | 401 ms | 392 ms | -9 ms | -10 ms |
| `currentTime` per poll, standard deviation | 13.7 ms | 13.4 ms | 44.3 ms | 43.5 ms |
| `currentTime` 12 s mean, standard deviation across trials | 2.2 ms | 0.8 ms | 10.9 ms | 6.7 ms |

- The Nest Audio's PAUSED status sometimes reports `currentTime` 0.256 before PLAY (5 of 30
  trials). Playback still starts at media 0, with the same delay.
- The Nest Audio's `currentTime` runs about 0.4 s ahead of what is audible; the Nest Mini's trails
  it by about 10 ms. Both offsets are steady from trial to trial, so they work as per-model
  constants.

Long run, 20 minutes, `currentTime` polled at 1 Hz. The Nest Mini run served the track at most 8 s
ahead of real time, as PlayOnAir does; served all at once, the 20-minute track made the Nest Mini
go IDLE with an ERROR reason right after PLAY.

| | Nest Audio | Nest Mini |
|---|---|---|
| Output clock against the Mac clock | -17.6 ppm (21 ms later after 20 min) | +1.6 ppm (1.9 ms earlier after 20 min) |
| Chirp times against a straight line, standard deviation | 0.02 ms | 0.08 ms |
| `currentTime` rate against the Mac clock | -16.8 ppm | +1.1 ppm |
| `currentTime` ahead of audible, first third / last third | 392.2 / 392.5 ms | -11.5 / -11.7 ms |
| `currentTime` 10-poll average, standard deviation | 2.8 ms | 6.6 ms |

`currentTime` follows the speaker's output clock, so it can stand in for the device playhead in
the classic drift loop.

Not measured here: the iPhone grandmaster against this Mac's clock, HomePods in the same room,
other Cast models, and Cast multizone followers behind a leader.

### Prototype in a mixed group

A prototype build ran PlayOnAir in a Linux VM bridged onto the LAN. It put the Gym speaker (Nest
Audio) in one AirPlay group with this Mac's own output (Music on this Mac as the sender and PTP
grandmaster) and implemented the mapping above:

- Released packets carry their PTP presentation time to the host ring.
- The body drops audio due sooner than 2 s, so LOAD, PAUSED and PLAY fit.
- LOAD uses `autoplay: false`; PLAY goes out at stream frame 0's due time minus the PLAY delay.
- A 1 s loop compares `currentTime` minus the model offset with the due position and drops or
  duplicates at most 1000 samples/s in the FLAC.

The microphone heard the Mac and the Cast speaker in alternating 15 s windows (system mute for
the Mac, AirPlay volume for the Cast speaker).

The Mac's output is late against the AirPlay timeline by what CoreAudio does not report. That lag
was measured separately (chirp at the reported DAC time, heard by the same microphone): 84.5 ms
for the Edifier M60 USB speakers (sd 0.35 ms), about 16 ms for the MacBook Pro speakers.

| | Cast minus Mac output | Cast minus AirPlay timeline |
|---|---|---|
| No sync (shipping behavior), two sessions | +87 ms, +255 ms | random per session |
| Sync, `currentTime` offset 392 ms, three sessions (Edifier) | -113, -115, -115 ms | about -30 ms |
| Sync, offset 392 ms (MacBook Pro speakers) | -40 ms | about -24 ms |
| Sync, offset 365 ms, four sessions (Edifier) | -98.5, -92.5, -90.9, -90.8 ms | -14 to -6 ms |

- Timed PLAY lands within 1 to 2 ms of its target. The Cast command path adds about 150 ms
  over the standalone PLAY delay, which the loop removes in about 20 s at the 1000 samples/s cap.
- After convergence, the loop's smoothed error stays within about ±5 ms. Within one session the
  heard offset varies with a 1 to 7 ms standard deviation per 2-minute window; between sessions
  it varies by about ±4 ms.
- The prototype's Nest Audio table uses a 290 ms PLAY delay and a 357 ms offset, extrapolated from
  these runs.
- The Mac sender tears the AirPlay session down after exactly 300 s whenever the windows were
  alternated, so no Mac-sent session above ran past 5 minutes.
- After a flush and resume from the Mac (about 1 s queued), the Cast re-LOAD never left
  BUFFERING; a click track of digital silence also stalled the Cast buffer. Both are findings
  about the shipping LOAD path, not about sync.

### 30 minutes from an iPhone

An iPhone (Safari playing the click track) sent to one AirPlay group: this Mac's AirPlay Receiver
(Apple's receiver, out the Edifier) and the Gym speaker through the prototype, with the table
above. The Mac took over as PTP grandmaster when it joined the group. The microphone alternated
15 s windows by muting the Mac's system output only, so nothing reached the sender. Click times
are first arrivals (the earliest peak within 20 ms before the strongest, since room reflections
can be louder than the direct click), against a fit of the Mac's clicks that tracks the
microphone's own clock.

| Minutes | 0-6 | 6-12 | 12-18 | 18-24 | 24-30 |
|---|---|---|---|---|---|
| Cast minus AirPlay timeline, median of 2-minute medians | -0.2 ms | -7.4 ms | -12.7 ms | -12.9 ms | -5.9 ms |

- The session ran the full 30 minutes with no drop, re-LOAD or step. The Cast speaker stayed
  between +0.3 and -16.1 ms of the Mac receiver (2-minute medians), each 2-minute window with a
  1.5 to 4 ms standard deviation. Over the run, the drift loop applied a net -634 samples against
  the Cast clock, and its smoothed error stayed within about ±10 ms.
- The offset wandered slowly (about 16 ms peak to peak over 30 minutes) instead of drifting off.
  The playout maps the PTP timeline once, at the anchor, then releases by the VM's monotonic
  clock, which chrony slews (2.4 ppm correction, 1.5 ms RMS offset). `[INFERENCE]` The wander is
  that clock against the grandmaster; Apple's receivers slave their timebase to PTP continuously.
- The first start fell back to the unlocked anchor. The iPhone's `timingPeerList` lists the Mac's
  stable addresses, but the Mac sends PTP from its temporary IPv6 address, and the follower only
  mapped timelines whose source address is a listed peer. PlayOnAir now matches the timeline by
  the clock identity the sender names in SETRATEANCHORTIME.
- The Cast speaker joins about 2 s after the Apple receiver: the start skips audio due within the
  next 2 s, and the speaker plays its Cast session chime at the same time.
- By ear, the Edifier run never sounded together: the Mac's click came 89 ms after the Cast one
  (sd 2 ms), which is the Edifier's unreported 84.5 ms. In the same session, switching the Mac's
  output to its built-in speakers (about 16 ms unreported) put the Mac 7 ms after the Cast
  speaker, and the two sounded like one source; switching back to the Edifier split them again.
  The Cast speaker is on the AirPlay timeline; an Apple receiver is only as close to it as its
  output device's reported latency is to the real one.

### Shipping behavior and calibration

PlayOnAir times every session whose Cast model is in its table (`Nest Audio`, `Google Nest Mini`,
exact `md` strings) on live FLAC: LOAD with `autoplay: false`, skip audio due within 1 s, PLAY at
stream frame 0's due time minus the PLAY delay, then the 1 s drift loop. Other models, Google
Home speaker groups and the WAV fallback keep the unsynced 8 s read-ahead path. Playout re-maps
every packet through the locked PTP timeline instead of the anchor-time offset, which removes the
slow wander above.

Calibration runs: iPhone sender, this Mac's AirPlay Receiver out its built-in speakers (16.1 ms
unreported, measured the same way as the Edifier's 84.7 ms) as the reference, 12 to 15 s windows
that mute every source but one (the Mac by system mute, a Cast speaker by its receiver mute).
Click times align each click's whole matched-filter envelope to the speaker's average envelope and
take that template's first arrival: the Nest Mini smears a click into a train of near-equal peaks
over 25 ms, so picking a single peak jumps between them.

| Speaker | PLAY delay | `currentTime` offset | Cast minus AirPlay timeline (4 min) |
|---|---|---|---|
| Nest Audio (Gym) | 290 ms | 366 ms | +0.6 ms median, sd 2.3 ms, per-minute -2.1 to +1.3 ms |
| Google Nest Mini (Pool) | 270 ms | 0 ms | -2.7 ms median, sd 6.3 ms, per-minute -8.2 to +0.1 ms |

- The Nest Mini's `currentTime` is noisier (about 29 ms per one-second reading against about
  10 ms for the Nest Audio), so its error wanders more within a session.
- In a multi-select group of several PlayOnAir speakers, multizone moves the session to a group
  endpoint hosted on one of the speakers. Its `currentTime` is offset differently: with the
  owner's calibration the Nest Mini drifted to about +350 ms. Drift correction therefore pauses
  while a group endpoint hosts the session; the members stay together through multizone but are
  not aligned with Apple receivers in the same AirPlay group.

## Open questions

| Question | How to settle |
|---|---|
| PTP domain, priority1/2, offset filter | Capture UDP 319/320 during a session (root), or extract `IOgPTPPlugin` from the kernelcache. |
| Whether a HomePod in the same room hears the synced Cast speaker as one source | Group a HomePod with a Cast speaker in one room and listen; the Mac receiver stood in for it above. |
| MediaRemote command numbers for `modernMediaRemoteCommand` | Decompile `-[AirPlayReceiverPlatform handleMRCommand:translatedAPCommand:withOptions:]`. |
| Semantics of extended feature bits 68, 77, 82 | Unnamed in the binary; flip them on a test receiver and observe sender behavior. |
| statusFlags bits 15 and 16 on Apple TV | Find the caller that ORs them into `sysInfo_getStatusFlags`' result. |
| Whether bits 15, 17, 59 break discovery for the fork's exact mask | Retest on an iPhone; the SDK receiver shows they do not by themselves. |

## Appendix: commands run

```
printf 'GET /info RTSP/1.0\r\nCSeq: 1\r\nUser-Agent: AirPlay/800.0\r\n\r\n' | nc -w 2 127.0.0.1 7000
printf 'GET /info RTSP/1.0\r\nCSeq: 1\r\nUser-Agent: AirPlay/800.0\r\n\r\n' | nc -w 3 <lan-receiver-ip> 7000
dns-sd -B _raop._tcp
dns-sd -L "<name>" _airplay._tcp
dns-sd -L "<id>@<name>" _raop._tcp
dns-sd -G v4v6 <host>.local
ipsw dyld extract /System/Volumes/Preboot/Cryptexes/OS/System/Library/dyld/dyld_shared_cache_arm64e TimeSync --output /tmp/re
ipsw macho info /tmp/re/AirPlayReceiver
ipsw macho info -c /tmp/re/AirPlayReceiver
ipsw dyld disass <dsc> --vaddr <addr> --cache <a2s>
strings -a /tmp/re/AirPlayReceiver
strings -a /tmp/re/AirPlaySupport
strings -a /tmp/re/TimeSync
nm /tmp/re/AirPlayReceiver
nm /tmp/re/AirPlaySupport
shasum -a 256 /tmp/re/AirPlayReceiver /tmp/re/AirPlaySupport /tmp/re/TimeSync
rea doctor
rea decompile /tmp/re/AirPlayReceiver <symbol-or-address> --provider ghidra --snapshot <snap> --format md --filter-output raw_result.value
rea decompile /tmp/re/AirPlaySupport <symbol> --provider ghidra --snapshot <snap> --format md --filter-output raw_result.value
rea function /tmp/re/AirPlayReceiver _audioSession_suspendNetworkReads --provider ghidra --snapshot <snap> --format json
rea search /tmp/re/AirPlayReceiver "<format string>" --provider ghidra --snapshot <snap>
ipsw dyld extract <dsc> AirPlaySender AirPlayReceiver AirPlaySupport TimeSync -o /tmp/ap-re
rea trace /tmp/ap-re/AirPlaySender "<format string>" --provider ghidra --json
rea decompile /tmp/ap-re/AirPlaySender <symbol-or-address> --provider ghidra --json
plutil -p /System/Library/Extensions/IOTimeSyncFamily.kext/Contents/PlugIns/IOgPTPPlugin.kext/Contents/Info.plist
```

Helper scripts (not committed) resolved `__cfstring` entries to their text and scanned `__text`
with capstone for ADRP/ADD and BL references to map strings and callees to functions.
Section 7's Cast measurements used a throwaway pychromecast sender (Cast TCP relayed through
`/usr/bin/nc`, since Local Network privacy blocks non-platform binaries on this Mac) and a
PortAudio capture of the microphone; neither is committed.
