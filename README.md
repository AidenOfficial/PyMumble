# pymumble-rev

**Asyncio Mumble client library for Python — sans-IO protocol core and pull-based Opus audio, built for music and voice bots.**

[简体中文](README.zh-CN.md)

[![CI](https://github.com/AidenOfficial/PyMumble/actions/workflows/ci.yml/badge.svg)](https://github.com/AidenOfficial/PyMumble/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pymumble-rev)](https://pypi.org/project/pymumble-rev/)
[![Python](https://img.shields.io/pypi/pyversions/pymumble-rev)](https://pypi.org/project/pymumble-rev/)

> **Status: beta.** Every feature in scope is implemented and tested against a real Mumble
> server in CI. Until 1.0 a minor release may still change the API; such changes are listed in
> [CHANGELOG.md](CHANGELOG.md).

An asyncio client for the Mumble voice protocol, designed for long-running bots that stream audio.

- **Sans-IO core**: the protocol is a pure state machine (`bytes in → events out`, `commands → bytes out`), testable without sockets.
- **Pull-based audio**: the client asks your source for the next 20 ms frame, so there is no buffer to poll and skips take effect immediately.
- **Small on purpose**: client only, Opus over the TLS tunnel, Mumble 1.5+. No UDP/OCB2, no ACL editing, no positional audio.

## Install

Requires Python 3.12+. Audio needs the system libopus (`apt install libopus0`,
`brew install opus`, ...); text-only bots do not.

```sh
pip install pymumble-rev
```

## Quick start

```python
import asyncio
from pymumble_rev import ClientConfig, MumbleClient, events


async def main() -> None:
    config = ClientConfig(username="dj-bot", client_type="bot")
    async with MumbleClient("mumble.example.org", config) as client:
        if music := client.state.find_channel("Music"):
            await client.move(music.channel_id)
        me = client.state.myself
        await client.send_text(f"hello from {me.name}", channel_ids=[me.channel_id])

        async for event in client.events():
            match event:
                case events.TextMessage(actor=int(actor), message="!ping"):
                    await client.send_text("pong", sessions=[actor])
                case events.UserUpdated(user=user, changed=changed) if "channel_id" in changed:
                    print(f"{user.name} moved to {client.state.channels[user.channel_id].name}")
        print("disconnected:", client.close_reason)


asyncio.run(main())
```

- `client.state` is an immutable snapshot (users, channels, server limits); reading it from another
  thread is safe.
- `events()` ends when the connection does, and `close_reason` says why (`kicked: ...`,
  `no data from server for 20s`, ...). The client never reconnects by itself: put
  `async with MumbleClient(...)` in your own retry loop, and each attempt starts from a fresh,
  fully synced state.
- Commands are coroutines that wait for the socket to drain, so a slow link slows the sender down
  instead of being mistaken for a disconnect. A server that stops answering pings for
  `dead_after` seconds (default 20) is treated as gone.
- The server certificate is not verified by default, because nearly every Mumble server uses a
  self-signed one. Pass `ssl_context=` to verify it, or
  `default_ssl_context(certfile, keyfile)` to log in with a client certificate.

## Playing audio

The client pulls 20 ms of PCM (s16le, 48 kHz, mono or stereo) from a source every frame, encodes
it with the system libopus, and paces packets in real time. `PcmBuffer` lets any thread feed it:

```python
import asyncio, subprocess
from pymumble_rev import ClientConfig, MumbleClient
from pymumble_rev.audio import PcmBuffer


async def play_file(client: MumbleClient, path: str) -> None:
    buf = PcmBuffer(channels=2, capacity=0.5)

    def feed() -> None:  # runs in a worker thread
        cmd = ["ffmpeg", "-v", "error", "-i", path, "-f", "s16le", "-ar", "48000", "-ac", "2", "-"]
        with subprocess.Popen(cmd, stdout=subprocess.PIPE) as ff:
            while chunk := ff.stdout.read(3840):
                buf.write(chunk)  # blocks while 0.5 s is queued: playback paces the reader
        buf.close()  # play what is queued, then end the transmission

    feeder = asyncio.to_thread(feed)
    await asyncio.gather(feeder, client.play(buf))


async def main() -> None:
    async with MumbleClient("mumble.example.org", ClientConfig(username="dj"), channels=2) as c:
        c.volume = 0.6  # applied per frame with a short ramp, so changes are instant and click-free
        await play_file(c, "song.mp3")
```

- `play(source)` switches immediately and returns a future that resolves when the source ends,
  is replaced, or `stop_audio()` is called. `PcmBuffer.clear()` skips what is queued.
- Short underruns (a slow download) are bridged with silence for 200 ms; longer ones end the
  transmission cleanly and a new one starts when data returns.
- The bitrate (default 96 kbit/s) is capped automatically to the server's bandwidth limit,
  counting the TCP tunnel overhead the server charges per packet.
- libopus is loaded on the first `play()`; set `PYMUMBLE_REV_LIBOPUS` to point at a specific
  library file.

### Whisper and shout

Audio normally goes to the channel you are in. A *voice target* sends it to specific users or
channels instead, wherever you sit:

```python
from pymumble_rev import VoiceTarget

await client.set_voice_target(1, [VoiceTarget(channel_id=lobby.channel_id, children=True)])
await client.play(buf, target=1)  # shout into the lobby and its sub-channels
await client.set_voice_target(2, [VoiceTarget(sessions=[user.session])])
await client.play(buf, target=2)  # whisper to one user
```

Target IDs are 1–30; `VOICE_TARGET_LOOPBACK` (31) makes the server echo your own audio back,
which is handy for testing a pipeline without a second client.

### Receiving audio

With `receive_audio=True`, every tunnelled packet arrives as `events.AudioReceived`. The server
forwards Opus as is, so decode it per sender with `pymumble_rev.opus.Decoder` (48 kHz s16le):

```python
from pymumble_rev import opus

decoders: dict[int, opus.Decoder] = {}
async for event in client.events():
    if isinstance(event, events.AudioReceived):
        dec = decoders.setdefault(event.sender_session, opus.Decoder(channels=1))
        pcm = dec.decode(event.opus_data)  # 20 ms of PCM per packet
        if event.is_terminator:
            decoders.pop(event.sender_session)  # the user stopped talking
```

## From threaded code

`ThreadedClient` runs the asyncio client on a private loop thread and exposes the same API as
blocking calls, for applications that are built on threads:

```python
import threading
from pymumble_rev import ClientConfig, ThreadedClient, events

with ThreadedClient("mumble.example.org", ClientConfig(username="dj"), channels=2) as client:
    buf, done = client.open_stream(capacity=0.5)  # a PcmBuffer that is already playing

    def player() -> None:  # e.g. your existing ffmpeg reader thread
        for chunk in pcm_chunks():
            buf.write(chunk)  # blocks while 0.5 s is queued
        buf.close()

    threading.Thread(target=player, daemon=True).start()

    for event in client.events():  # blocks; ends when the connection ends
        if isinstance(event, events.TextMessage) and event.message == "!skip":
            buf.clear()
```

- Commands (`send_text`, `move`, ...) block until written and raise `TimeoutError` after
  `command_timeout` (10 s). Calling them from the loop thread raises instead of deadlocking.
- `play()` returns once the switch has happened, so a following `stop_audio()` or `volume`
  change always applies to the new source. Its `concurrent.futures.Future` completes when the
  source ends.
- `state` is an immutable snapshot and can be read from any thread without locking.

## The sans-IO core

`Connection` is the protocol without any I/O: feed it the bytes you read with `receive_data()`,
write what `data_to_send()` returns, and pass time in explicitly (`ping(now)`). `MumbleClient`
is a thin asyncio driver around it; use `Connection` directly to run the protocol on another
event loop, in a thread, or in tests.

## Why not pymumble?

[pymumble](https://codeberg.org/pymumble/pymumble) runs its own thread, calls your callbacks on
it while holding internal locks, writes to the TLS socket from several threads, and treats a
congested socket as a disconnect. Those are consequences of its threading model, not individual
bugs, so this library starts over with a single event loop and a protocol core that does no I/O.
The API is new and is **not** a drop-in replacement.

## Scope

| In scope | Out of scope |
|---|---|
| TLS control channel, Mumble 1.5+ protocol | UDP voice and OCB2 crypto |
| Users, channels, text messages, moving, self state | ACL and channel administration |
| Opus audio tunnelled over TCP (send and receive) | Positional audio, legacy codecs |

## Development

```sh
uv sync --group dev
uv run pytest
uv run ruff check && uv run ruff format --check
uv run mypy
```

The protobuf modules in `src/pymumble_rev/_proto` are generated from `proto/*.proto` (taken
from Mumble v1.5.939) with `uv run --group proto python scripts/gen_proto.py`.

`tests/test_integration.py` runs against a real server when `PYMUMBLE_REV_SERVER=host:port` is
set (CI starts a `mumble-server` from the Ubuntu repositories); see the module docstring for a
local setup.

### Releasing

Bump `version` in `pyproject.toml`, run `uv lock`, move the `[Unreleased]` entries in
`CHANGELOG.md` under the new version, merge, and push a `vX.Y.Z` tag. The Release workflow
checks that the tag matches, builds, publishes to PyPI via trusted publishing and creates the
GitHub release from the changelog section.

## License

MIT. The `.proto` files in `proto/` are © The Mumble Developers under the BSD-3-Clause license
(`proto/LICENSE`).
