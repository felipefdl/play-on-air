# Live findings: Apple senders, Apple Home, and Cast

What real devices do, recorded from PlayOnAir 0.5.x runs in October 2026: an iPhone and a Mac
as AirPlay senders, two Apple TVs (one of them the active home hub), and Nest Audio / Nest Mini
speakers. Each finding names the log lines that show it, so it can be re-checked. Read this
before changing Apple Home pairing, the Bonjour advertisement (`statusFlags`, `features`, TXT
keys), or how a session ends.

Logs are UTC. `warm Cast STOP ok`, `warm Cast PAUSE ok` and the ownership probe lines come
from `play_on_air::cast::pool`; the STOP line needs that module at `debug`.

## AirPlay sessions from an iPhone

| What the user does | What the sender sends | What PlayOnAir does |
|---|---|---|
| Pause | `SETRATEANCHORTIME` with rate 0 | Cast PAUSE (`warm Cast PAUSE ok`, about 100 ms on the wire) |
| Seek or skip | rate 0, `FLUSHBUFFERED`, then a new anchor | Cast PAUSE, then a re-LOAD: the Cast `media_session_id` goes up by one per flush |
| Switch output to the iPhone or another speaker, or stop | `TEARDOWN` | Cast STOP, acknowledged 78 ms after the `TEARDOWN` (`warm Cast STOP ok`, then `bridge session ended`) |
| Let the queue end (Autoplay off) | `FLUSHBUFFERED`, no `TEARDOWN` | Cast PAUSE. The session stays open and the iPhone keeps the speaker selected, paused |
| Ask Siri to play on the speaker or in its room | The iPhone opens a normal session itself | Same as playing from the picker. The home hub is not involved |

Before 0.5.3 the `TEARDOWN` row never reached the speaker: the Cast worker had already dropped
the session it should STOP, so the speaker played out its read-ahead for about 12 s.

## Several speakers (Cast multizone)

| What the user does | Log lines | Result |
|---|---|---|
| Add a second speaker to a playing one | `AirPlay group member follows the group owner's Cast session`, `Cast multizone PLAYBACK_SESSION_UPDATED`, `Cast group session moved; media control follows the session host` | Both play in sync. Media control moves to a `cast-session:<ip>:<port>` worker |
| Deselect the speaker that started the stream | `AirPlay group owner left; its Cast session continues fed by a remaining member`, then its `TEARDOWN` | No STOP. The rest keep playing |
| Deselect the last speaker | `TEARDOWN`, `warm Cast STOP ok device_id=cast-session:...` | The group's Cast session stops (191 ms after the `TEARDOWN`) |
| A speaker playing from one sender is added to another sender's group | `warm Cast STOP ok` for its solo session, then the multizone add | The solo stream stops (83 ms after the new session starts); the speaker joins the group 0.56 s later, in sync |

## Apple Home

The home hub talks to PlayOnAir only to add, remove, or rename the speaker: `/pair-add`,
`/pair-remove`, `/pair-list`, `/configure`, and short remote-control-only `SETUP` connections
around them. It never connects to check status.

| What the user does | Requests | Result |
|---|---|---|
| Add the speaker in the Home app | Transient pair-setup, `/pair-add`, `/pair-list`, `/configure` with `Access_Control_Level`, `clientPID`, `Password`, `Enable_HK_Access_Control` and **no** `Device_Name` | The advertised name is saved as the Home name (`accessory is in a home: its advertised name is its Home name`) |
| Type a different name during setup | About 18 s after the add, `/configure` with `Device_Name` from the hub; the other Home devices repeat it within 10 s | Saved and re-advertised within about 1 s |
| Rename in the Home app | `/configure` with `clientPID`, `Device_Name` from every Home device (hub, other Apple TV, iPhone, Macs), within about 13 s | Saved at once (`saved the speaker's Apple Home name`). Re-advertised about 1 s later, or 1 s after the current stream ends |
| Remove from Home | The hub sends `/pair-remove` for each controller, then a `/configure` without `Device_Name` | The last removal drops the Home name (`speaker left Apple Home`); the speaker is re-advertised under its Cast or configured name 0.3 s later |
| Rename the Chromecast in Google Home | None from Apple devices; Bonjour carries the new Cast `fn` | In Home: `Chromecast renamed; AirPlay keeps the name from the Apple Home app`, no change. Not in Home: the AirPlay name follows |

Other facts:

- **Siri does not depend on names.** Siri played by speaker name and by room while Home and
  AirPlay showed different names. Two refusals ("can't play directly on ...", "... can't do
  that") came within minutes of a re-add or rename; the same requests worked minutes later.
- **One removal sent no `/pair-remove`** (during the status bit 10 experiment below). The
  controller stayed in the pairing file until it was cleared by hand.
- **The Home tile shows No Response.** In the logs the active hub never opens a status
  connection, and a 5-minute packet capture of the other Apple TV showed no traffic to
  PlayOnAir at all (only ARP and its own Bonjour announcements). shairport-sync has the same
  limit ([#1802](https://github.com/mikebrady/shairport-sync/issues/1802),
  [#1869](https://github.com/mikebrady/shairport-sync/issues/1869),
  [#2182](https://github.com/mikebrady/shairport-sync/issues/2182)).
- **A Sonos speaker on the same LAN** advertises the same `flags=0x4`, yet its tile shows no
  badge and "Controls not available" when tapped. Its `features` differ: Apple MFi
  authentication (bit 26) and bit 59 set, FairPlay (bit 14) not.
- **Accessory identifier.** `/configure` reports `Identifier` = `pi`, while pair-setup M6 and
  pair-verify M2 sign with the MAC-style device id. That only matters once iPhones use
  pair-verify; with status bit 10 off they keep using transient pairing.

## Experiments that do not work

Each was run against a speaker that was in Apple Home, then reverted.

| Change | What happened |
|---|---|
| Advertise `statusFlags` bit 10 (`0x404`, "set up for HomeKit access control") plus a `psi` TXT key once a controller is stored | iPhones switched from transient pairing to pair-verify and hung up after M2 (identifier mismatch above). The picker then fell back to transient pairing, which sometimes failed ("no connection"); re-adding the speaker to Home failed with "Unable to connect". Siri stopped working. The hub still never connected; the tile still said No Response |
| The above, plus signing M6 and M2 with `pi` | Pair-verify completed (`Pair-verify: client signature verified`) and the picker connected on the first try. Siri still failed, the hub still never connected, the tile still said No Response |
| Advertise feature bit 59 (`SupportsAudioStreamConnectionSetup`, `features=0x405FCA00,0x801C340`, the Sonos high word) | The iPhone sent the stream `SETUP` and tore the session down within about 100 ms, on both tries. Playback broke; the tile did not change |
| Disable IPv6 (a shairport-sync workaround) | Not applicable: the container already has IPv4 only |

Apple MFi authentication needs Apple's licensed hardware, so PlayOnAir cannot advertise it.

## Running a live check

1. Build an image from the working tree on the Docker host that runs PlayOnAir:
   `git archive --format=tar HEAD | ssh <host> 'mkdir -p /tmp/poa-src && tar -x -C /tmp/poa-src && cd /tmp/poa-src && docker build -t play-on-air:test .'`
2. Next to the host's `compose.yaml`, write a `compose.test.yaml` override with
   `image: play-on-air:test` and, when needed,
   `environment: { RUST_LOG: "info,play_on_air::cast::pool=debug" }`. Start it with
   `docker compose -f compose.yaml -f compose.test.yaml up -d`. Pairing state in `/data`
   carries over.
3. Read the log with ANSI colors stripped:
   `docker logs --since 10m play-on-air 2>&1 | sed -E 's/\x1b\[[0-9;]*m//g'`.
   Useful patterns: `TEARDOWN|FLUSHBUFFERED|SETRATEANCHORTIME`, `bridge session`,
   `Cast (STOP|PAUSE)`, `pair-(add|remove|list)|configure request|Home name`,
   `group|multizone`, `ownership lost`.
4. Pairing state per speaker: `docker exec play-on-air jq . /data/pairing/<cast-id>.json`
   (`identity`, `controllers`, `home_name`).
5. What the speaker advertises, from a Mac on the LAN: `dns-sd -B _airplay._tcp local`, then
   `dns-sd -L "<name>" _airplay._tcp local` for `flags`, `features`, `pi`, `pk`.
6. Restore the release: `docker compose up -d && rm compose.test.yaml`, then remove the test
   image and `/tmp/poa-src`.

Every re-advertise logs `ss -K reported success but closed zero sockets` at `warn`. It is
harmless when nothing was connected.
