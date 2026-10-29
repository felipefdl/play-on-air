# Cast dynamic group (stream expansion) spec

This note records how a Google sender expands a playing Cast session onto more speakers, moves it
to another speaker, and drops speakers again, so PlayOnAir can ask Google's devices to keep
multi-speaker playback in sync instead of feeding each speaker its own buffer. It is a clean-room
description: behavior and message shapes in our own words. It contains no decompiled code.
Obfuscated class names appear only as references so the findings can be re-checked against the
same APK build.

## Evidence base

| Source | Identity |
|---|---|
| Google Home for Android | `com.google.android.apps.chromecast.app` 4.33.27.4 (version code 30585751), APKPure XAPK, base APK decompiled with JADX 1.5.6 (`--no-res`) |
| Live speakers | Patio speaker (Nest Mini, `192.0.2.145`), Den speaker (Nest Audio, `192.0.2.213`), Bedroom speaker (Nest Audio, `192.0.2.171`) |
| Live session | PlayOnAir LIVE FLAC on the Default Media Receiver, expanded from the iOS Google Home app at about 00:56:40Z on 2026-10-18 |
| Live probes (read-only) | Unauthenticated CASTV2 TLS sender in the `PlayOnAir Test` VM sending only `CONNECT`, receiver `GET_STATUS`, multizone `GET_STATUS`, multizone `GET_CASTING_GROUPS`; `avahi-browse -rpt _googlecast._tcp` |

Google Play services' Cast module was not decompiled. The Home app carries its own copy of the
logic, and the live expansion matches it, so the GMS path (Android Output Switcher) is assumed to
send the same message [INFERENCE].

Confidence scale: **High** (read in the control flow and seen live), **Medium** (read in the
control flow, not seen live), **Low** (strings or one live sample only). `[INFERENCE]` marks a
conclusion not directly observed.

### Where the logic lives in the APK

| Reference | Role |
|---|---|
| `pid` | Per-session "dynamic session client": member table, selection state machine, builds `GET_STATUS` and `SET_PLAYBACK_DEVICES` (log text "setPlaybackDevicesForSession") |
| `pig` / `phv` | Multizone channel: parses `MULTIZONE_STATUS`, `DEVICE_ADDED`, `DEVICE_UPDATED`, `DEVICE_REMOVED`, `PLAYBACK_SESSION_UPDATED`, `INVALID_REQUEST`; `phv` sends on `urn:x-cast:com.google.cast.multizone` |
| `pie` | `DynamicSessionInfo{isVideoContent, streamTransferSupported, appAllowsGrouping}` |
| `pih` | Member: deviceId, name, capabilities bitmask, volume level, muted |
| `pil` | Selection state enum: `DESELECTED`, `SELECTED`, `SELECTING`, `DESELECTING` |
| `phw` / `pfj` / `pfn` | Session connection: joins the running app by session id, registers the multizone listener, then sends `GET_STATUS` |
| `pgm.H` | Builds the session connection from a MediaRouter route carrying `com.google.android.gms.cast.EXTRA_SESSION_ID`; replays pending selections when the session endpoint moves (log "Session endpoint changed") |
| `wla.r` / `wla.u` / `wla` (~line 390) | UI entry points: add one device, transfer ("Transfer initiated: sessionId=%s, deviceId=%s"), remove one device |
| `acws.o` / `qza` case 3 | Send helper: multizone messages go to destination `receiver-0`, not to the app transport |
| `ahqd.L` | Treats a device whose running app is `531A4F84` or `705D30C6` as a member of some other device's dynamic session |

## Model

A dynamic group is not a saved speaker group. The playing device asks Google's stack to create a
temporary group, the "session endpoint", and moves the running receiver app onto it. Members follow
that endpoint in sync. Everything is local Cast traffic on the LAN.

Seen live after one expansion (High):

- The leader (the device that was playing) keeps its own Cast service on port 8009. Its receiver
  status now shows app `531A4F84` (display name "Default Media Receiver", `universalAppId`
  `531A4F84`) with the **original session id** `d70e5c00-…`.
- A new mDNS `_googlecast._tcp` service appears on the **leader's IP** at a new port (32228):
  instance `Google-Cast-Group-<32 hex>`, `md=Google Cast Group`, `id`/`cd` = the dashed group UUID
  `ec81f5df-e013-4088-9175-c852965c2c57`, `fn=Patio speaker + 1`, `ca=198756`.
- Its receiver status shows the real app `CC1AD845` (Default Media Receiver, namespaces `media`,
  `cac`, `debugoverlay`) with the **same session id and transport id** `d70e5c00-…`.
- Its multizone `GET_STATUS` lists both members and carries `playbackSession`.
- The follower (Den speaker) runs app `705D30C6` ("Default Media Receiver", media namespace only)
  under its own new session id. Its own multizone status lists only itself and
  `"isMultichannel": false`.
- PlayOnAir's existing sender connection and media control kept working after the move; no
  ownership-lost events.

## Channel and addressing

- Namespace: `urn:x-cast:com.google.cast.multizone`, JSON string payloads. (High)
- Destination: `receiver-0` on the device that currently hosts the session: the leader's port 8009
  before expansion, the session endpoint (`ipAddress:port` from `sessionEndpoint`) after. The Home
  app also joins the running app (`JoinOptions`) on that connection, but the multizone messages
  themselves go to `receiver-0`. (High for destination; the need to join first is Medium, see
  below)
- An unauthenticated sender that only sent `CONNECT` to `receiver-0` gets multizone `GET_STATUS`
  answers. No token, account, or cloud call appears anywhere in the request path. (High for
  `GET_STATUS`; Medium for `SET_PLAYBACK_DEVICES`, not sent yet)
- `requestId`: integer; the Home app starts at a random value and increments. Responses echo it, and
  the parser rejects any multizone message without a `requestId`. (High)
- `GET_CASTING_GROUPS` returns `INVALID_REQUEST` with reason `INVALID_COMMAND` on the leader, the
  group endpoint, and a follower in this firmware; the Home app never sends it. (High)

### Device identifiers

- Status messages carry dashed UUIDs (`d417a9b9-bf1e-be21-1639-512dad18be1d`). The Home app strips
  the dashes when parsing and keys members by the 32-hex form, which equals the speaker's mDNS
  `id` TXT value (`d417a9b9bf1ebe211639512dad18be1d`). (High)
- Requests send the device id as the sender's Cast device record holds it, which is the mDNS `id`
  value: 32 hex without dashes for speakers, dashed for groups. (Medium: field origin read in
  code; not sent live)
- Stereo pairs (capability 128 or 256) are sent twice, once as given and once with dashes removed.
  (Medium)

### Capability bits

| Bit | Meaning in the Home app | Live |
|---|---|---|
| 1 | Video out | |
| 4 | Audio out | set on all speakers |
| 32 | Multizone group (saved or dynamic) | set on the group endpoint |
| 64 | Dynamic group | set on the group endpoint |
| 128 / 256 | Multichannel group / member (stereo pair) | |
| 65536 | Supports dynamic sessions (expansion and transfer) | set on all speakers |
| 131072 | Not used by this code | set on all speakers |
| 2048, 512 | Only in mDNS `ca`, not in multizone status | |

Multizone status `capabilities` for a Nest Mini is 196612 (4 + 65536 + 131072). The mDNS `ca` adds
2048 (198660). The group endpoint adds 32 + 64 (198756). (High for values, Medium for meanings)

### Eligibility (Home app UI rule)

A candidate device is offered as "add to this session" only when all hold (Medium):

- the candidate and the session device both have bit 65536;
- the session's `playbackSession.appAllowsGrouping` is true (a missing or null value counts as true);
- `playbackSession.isVideoContent` is false;
- the candidate is not a dynamic group (bit 64) and not a saved non-stereo group (bit 32 without 128).

It is offered as a transfer target when it has bit 65536, `streamTransferSupported` is true, it is
not a dynamic group, and the content is audio (video also needs bit 1 and not a saved group).

## Messages

### `GET_STATUS` (sender → `receiver-0`)

```json
{"requestId": 123, "type": "GET_STATUS"}
```

### `MULTIZONE_STATUS` (device → sender)

```json
{"requestId": 123, "type": "MULTIZONE_STATUS",
 "status": {
   "devices": [{"deviceId": "<dashed uuid>", "name": "…", "capabilities": 196612,
                "volume": {"level": 0.65, "muted": false}}],
   "playbackSession": {"appAllowsGrouping": true, "isVideoContent": false,
                       "streamTransferSupported": true},
   "isMultichannel": false,
   "multichannelDevices": []}}
```

- `devices` is the current member set of the session hosted here. (High)
- `playbackSession` is present only while this endpoint hosts a groupable session. It was present
  on the leader before expansion and on the group endpoint after, absent on the leader's own 8009
  after expansion and on followers. The Home app ignores a status that drops `playbackSession`
  once it had one ("Dynamic session info becomes null. Ignore the status."). (High)
- `isMultichannel` / `multichannelDevices` describe stereo pairs; the Home app reads
  `multichannelDevices` instead of `devices` when `isMultichannel` is true and a server flag
  allows it. (Medium)

### `SET_PLAYBACK_DEVICES` (sender → `receiver-0` of the session host)

The one command that expands, shrinks, and transfers. It carries the **complete desired member
set**, not a delta. (High from code; matches the live result)

```json
{"requestId": 124, "type": "SET_PLAYBACK_DEVICES",
 "sessionId": "d70e5c00-57e2-42db-9e69-58beb876aa16",
 "deviceList": [
   {"deviceId": "d417a9b9bf1ebe211639512dad18be1d", "ipAddress": "192.0.2.145", "port": 8009},
   {"deviceId": "56cb7fe7e13f325625325c4a304c57fa", "ipAddress": "192.0.2.213", "port": 8009}],
 "sessionName": "Patio speaker + 1"}
```

- `sessionId`: the receiver app's session id from `RECEIVER_STATUS` (for PlayOnAir this equals
  the transport id). The Home app takes it from the MediaRouter route extra
  `com.google.android.gms.cast.EXTRA_SESSION_ID`; live, `sessionEndpoint.sessionId` echoed
  PlayOnAir's session id. (High)
- `deviceList`: every member in state `SELECTED` or `SELECTING`, including the current device.
  `ipAddress` and `port` are the member's own Cast address and are omitted when unknown. (High)
- `sessionName`: optional (`putOpt`). One member: that member's name. Several: a localized
  "<first name> + <n-1>" built from the first-selected device. The live endpoint used
  "Patio speaker + 1". (High)
- The Home app keeps one `SET_PLAYBACK_DEVICES` in flight per session, waits for its answer or a
  server-configured timeout, and only sends after the first `GET_STATUS` on a fresh connection has
  been answered. (High)

### Answers and events (device → sender)

| Type | Fields | Meaning |
|---|---|---|
| `PLAYBACK_SESSION_UPDATED` | `requestId`, `name`, optional `failedDevices` (array of device id strings), optional `sessionEndpoint {deviceId, localDeviceId, sessionId, ipAddress, port}` | Completes a `SET_PLAYBACK_DEVICES` (matched by `requestId`). With `sessionEndpoint`, the session moved: the sender drops its connection state and reconnects to `ipAddress:port`. (High) |
| `DEVICE_ADDED` | `device {deviceId, name, capabilities, volume}` | Member joined; marked `SELECTED`. (Medium) |
| `DEVICE_UPDATED` | `device {…}` | Member name, capabilities, or volume changed. (High: seen live) |
| `DEVICE_REMOVED` | `deviceId` | Member left; marked `DESELECTED`. (Medium) |
| `INVALID_REQUEST` | `requestId`, `reason` | Request refused; completes both pending `GET_STATUS` and `SET_PLAYBACK_DEVICES` with that id. Seen live as `INVALID_COMMAND`. (High) |

Live `sessionEndpoint` on the first expansion: `deviceId` = `localDeviceId` =
`ec81f5df-e013-4088-9175-c852965c2c57` (the new group UUID), `ipAddress` `192.0.2.145`, `port`
32228, `sessionId` unchanged. (High) Whether the live message carried `requestId` was not recorded;
the Home app parser needs it [INFERENCE: it does].

Success is confirmed by `PLAYBACK_SESSION_UPDATED` without the member in `failedDevices`, followed
by `MULTIZONE_STATUS` from the session endpoint listing the member. Per-member failure is reported
in `failedDevices`, not as an error. (High for the fields, Medium for the exact sequence)

## Operations

### Expand (add speakers)

`SET_PLAYBACK_DEVICES` with current members plus the new ones, sent to the session host. The first
expansion moves the session to a new group endpoint on the leader's IP and a new port. (High)

### Remove a speaker

`SET_PLAYBACK_DEVICES` with the member omitted, sent to the group endpoint. The Home app marks it
`DESELECTING` and waits for `DEVICE_REMOVED` or a status without it. (Medium: code only)

Seen live with PlayOnAir: shrinking a two-speaker group (Den leader on group endpoint
`192.0.2.213:32158`, Patio member) to Patio alone answered with a
`sessionEndpoint` on Patio's own `192.0.2.145:8009`; the group endpoint went away. A
`CONNECT` plus media `GET_STATUS` there on the session's **old** transport id made the receiver
close the connection (EOF, "failed to fill whole buffer"). (High)

### Following a moved session

After any `sessionEndpoint`, the sender drops its connection state and reconnects to
`ipAddress:port` (`pgm.java`, "Session endpoint changed. Removing old session connection for
session id"), then joins the app by session id. PlayOnAir does the same: receiver `GET_STATUS` on
the new host, the application whose `sessionId` is `sessionEndpoint.sessionId` (else the session's
own id) and that lists the media namespace, `CONNECT` to that application's `transportId`, media
`GET_STATUS` for the media session id. The old transport id is never reused on the new host.
(High for the reconnect; Medium that the transport id changes, inferred from the EOF above)

If that fails, the session no longer plays where PlayOnAir's control was, so PlayOnAir re-LOADs a
fresh session on the group owner's own speaker and adds the remaining members back with
`SET_PLAYBACK_DEVICES`.

### End the group

There is no separate "dissolve" command in the Home app. Either shrink to one device with
`SET_PLAYBACK_DEVICES`, or stop the receiver app on the group endpoint (standard receiver `STOP`
with the session id), which ends playback on every member. (Medium)

### Transfer (move the session)

`SET_PLAYBACK_DEVICES` whose `deviceList` contains only the target (`wla.u` deselects every other
member and selects the target). The answer carries a `sessionEndpoint` on the target; the sender
reconnects there. If the single target is a saved group (bit 32), the Home app first clears all
other pending selections. Requires `playbackSession.streamTransferSupported`. (Medium)

### Volume per member

The multizone channel has no volume command in the Home app; it only reads member volume from
`MULTIZONE_STATUS` and `DEVICE_UPDATED`. A member's volume is set the usual way, receiver
`SET_VOLUME` on that member's own `receiver-0` at port 8009, which stays reachable while it follows
(the Den speaker answered status on 8009 while grouped). (High)

A Google Home speaker group's own endpoint (the "Den and Patio" group, port 32128) reports a
`master` volume. Setting it scales the members only while that endpoint hosts the session. Live,
after the group joined a session that Cast then hosted on a dynamic endpoint (`192.0.2.213:32230`),
`SET_VOLUME` 0.398 on the group endpoint left the members at 0.616 and 0.766, and the session
host reported 0.691, their mean. The idle group endpoint still answers multizone `GET_STATUS`
(`apps` empty) with its members and their levels:

```json
{"requestId":901,"type":"MULTIZONE_STATUS","status":{"devices":[
  {"deviceId":"d417a9b9-bf1e-be21-1639-512dad18be1d","name":"Patio speaker","capabilities":196612,"volume":{"level":0.766,"muted":false}},
  {"deviceId":"56cb7fe7-e13f-3256-2532-5c4a304c57fa","name":"Den speaker","capabilities":196612,"volume":{"level":0.616,"muted":false}}]}}
```

(High) So PlayOnAir sets a speaker group's volume on its endpoint only while its session plays
there; otherwise it reads the members from that status and sets each member so their mean is the
target, scaling all levels by the same factor (`crates/play-on-air/src/bridge/volume.rs`).

### LIVE Default Media Receiver sessions

Yes. A PlayOnAir LIVE FLAC session on the Default Media Receiver (`CC1AD845`) reported
`appAllowsGrouping: true`, `isVideoContent: false`, `streamTransferSupported: true`, was expanded
from the Google Home app, and played in sync on both speakers. (High)

## Blockers and unknowns

- **No cloud auth or device certificate is needed by a sender for reads.** CASTV2 device
  authentication proves the receiver to the sender, not the sender to the receiver. Whether the
  receiver accepts `SET_PLAYBACK_DEVICES` from a sender other than Google's stack is unproven until
  the test below runs.
- Whether the sender must be joined to the app session (connected to the app transport id) before
  `SET_PLAYBACK_DEVICES` is accepted. PlayOnAir already is, as the session owner.
- Device id form in `deviceList` (32 hex vs dashed): the code points to the mDNS `id`; try the
  dashed form if refused.
- Timeouts come from server flags and were not read.
- Removal and transfer were not observed live.

## Live test to run (needs someone present; plays audio)

Preconditions: PlayOnAir plays on the Patio speaker (`192.0.2.145:8009`) and its
receiver status shows app `CC1AD845` with session id `S`. Den speaker is idle, mDNS
`id=56cb7fe7e13f325625325c4a304c57fa`, `192.0.2.213:8009`. The PlayOnAir sender stays connected
throughout, and all messages below use a separate sender id on the same TLS connection or the
existing one.

1. On `192.0.2.145:8009`: `CONNECT` to `receiver-0`, then multizone
   `{"requestId":1001,"type":"GET_STATUS"}`. Expect `MULTIZONE_STATUS` with only Patio and
   `playbackSession.appAllowsGrouping: true`.
2. Same connection, multizone:
   `{"requestId":1002,"type":"SET_PLAYBACK_DEVICES","sessionId":"S","deviceList":[{"deviceId":"d417a9b9bf1ebe211639512dad18be1d","ipAddress":"192.0.2.145","port":8009},{"deviceId":"56cb7fe7e13f325625325c4a304c57fa","ipAddress":"192.0.2.213","port":8009}],"sessionName":"Patio speaker + 1"}`.
   Expect `PLAYBACK_SESSION_UPDATED` (requestId 1002, empty or absent `failedDevices`,
   `sessionEndpoint {deviceId: G, ipAddress: 192.0.2.145, port: P, sessionId: S}`), a new mDNS
   `Google-Cast-Group-…` at `192.0.2.145:P`, Den running `705D30C6`, audio in sync.
   If refused with `INVALID_REQUEST`, retry once with dashed device ids; if still refused, retry
   after `CONNECT` to transport `S` (app join).
3. Connect to `192.0.2.145:P`: `CONNECT` `receiver-0`, receiver `GET_STATUS` (expect `CC1AD845`,
   session `S`), multizone `GET_STATUS` (expect both members and `playbackSession`).
4. Remove Den, on `192.0.2.145:P`:
   `{"requestId":1003,"type":"SET_PLAYBACK_DEVICES","sessionId":"S","deviceList":[{"deviceId":"d417a9b9bf1ebe211639512dad18be1d","ipAddress":"192.0.2.145","port":8009}],"sessionName":"Patio speaker"}`.
   Record: `DEVICE_REMOVED`, any new `sessionEndpoint` (back to port 8009 or not), the Den speaker's
   app list, and whether PlayOnAir's media control still works.
5. Transfer (optional), on whichever endpoint hosts `S`: `deviceList` with only the Den entry.
   Record the `sessionEndpoint` and whether PlayOnAir's connection survives.
6. End: receiver `STOP` with `sessionId` `S` on the current endpoint, and confirm every member
   returns to idle.
