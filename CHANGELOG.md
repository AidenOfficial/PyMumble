# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/): before 1.0 a **minor** release may contain breaking
changes, which are always listed under *Changed* or *Removed*.

## [Unreleased]

## [0.1.0] - 2026-10-15

First release on PyPI.

### Added

- `Connection`: the Mumble 1.5 client protocol as a sans-IO state machine (bytes in, events out,
  commands queue bytes), with an immutable `ServerState` snapshot of users, channels and server
  settings that is safe to read from any thread.
- `MumbleClient`: asyncio driver with a single event stream, keep-alive pings with RTT, dead-peer
  detection, and write backpressure instead of disconnecting on a congested socket.
- Pull-based audio: `play(source)` pulls one PCM frame per period, encodes with the system libopus
  (ctypes binding in `pymumble_rev.opus`), paces on absolute deadlines, bridges short underruns
  with silence, resyncs instead of bursting after a stall, and caps the bitrate to the server's
  bandwidth limit. `PcmBuffer` feeds audio from any thread; `volume` applies a click-free ramp.
- Receiving audio as `events.AudioReceived` (opt-in with `receive_audio=True`) and an Opus
  `Decoder` for turning it back into PCM.
- Whisper/shout: `set_voice_target()` registers a `VoiceTarget`, `play(..., target=)` and
  `send_audio(target=)` use it; `VOICE_TARGET_LOOPBACK` makes the server echo our own audio.
- `ThreadedClient`: the same API as blocking calls for applications built on threads, with
  `open_stream()` returning a `PcmBuffer` that is already playing.
- Text messages, moving users, self mute/deaf/comment/avatar, listening channels, access tokens
  (`set_tokens()`), blob requests, and permission-denied / server-config events.
- `ServerState.find_channel()` / `find_user()`, `path()`, `children()`, `users_in()`.
- Being kicked or banned closes the connection with a `kicked: ...` / `banned: ...` reason.
- `pymumble_rev.__version__`.

[Unreleased]: https://github.com/AidenOfficial/PyMumble/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/AidenOfficial/PyMumble/releases/tag/v0.1.0
