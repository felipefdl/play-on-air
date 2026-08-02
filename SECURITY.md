# Security

PlayOnAir is a **LAN-only** audio bridge. It does not create accounts, phone home, or send telemetry.

## Trust model

- Runs on a host (or Home Assistant OS app) on the **same local network** as your Chromecasts and Apple devices.
- Speaks AirPlay 2 receiver and Google Cast protocols on that LAN. Pairing and stream crypto stay on those protocol paths.
- Optional config only renames or hides devices and sets log level. There is no remote admin surface.

## Host network

The Home Assistant app sets `host_network: true` so mDNS multicast, AirPlay advertisement, and Cast control reach the LAN. That is required for discovery and playback. The tradeoff: the container shares the host network namespace instead of an isolated bridge. Treat the host as a trusted household machine.

## What this is not

- Not a cloud service
- Not a VPN or remote-access product
- Not a multi-tenant or enterprise control plane

## Reporting issues

Please open an issue on [github.com/felipefdl/play-on-air](https://github.com/felipefdl/play-on-air) with a clear description and reproduction notes. Do not include private keys, pairing secrets, or full LAN inventories beyond what is needed to debug.
