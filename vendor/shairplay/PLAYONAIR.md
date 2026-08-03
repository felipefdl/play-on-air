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
12. Playout RTP math uses `source_sample_rate`; FLUSHBUFFERED range compare is wrap-safe; map capped ~30s.
13. Playout mutex/condvar locks use `unwrap_or_else(PoisonError::into_inner)` (poison-proof).
14. Realtime type 96: RTP seq reorder window (drop dups, silence for aged gaps; ALAC in order only).
15. RTSP `process_connection` read timeout (45s) so half-open iOS sockets release the connection semaphore; accept loop prunes finished `conn_aborts`.
16. Configurable `Audio-Latency` RECORD header (`RaopServerBuilder::audio_latency_samples`, default 96000 = 2s@48k).

Keep upstream license files. Prefer contributing hard-stop upstream and dropping the vendor when released.
