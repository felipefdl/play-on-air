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

Keep upstream license files. Prefer contributing hard-stop upstream and dropping the vendor when released.
