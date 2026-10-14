# pymumble-rev

**Asyncio Mumble client library for Python — sans-IO protocol core and pull-based Opus audio, built for music and voice bots.**

[简体中文](README.zh-CN.md)

> **Status: pre-alpha.** The sans-IO protocol core and the asyncio client are in place; the audio
> pipeline is next. The API will change until 0.1.0.

An asyncio client for the Mumble voice protocol, designed for long-running bots that stream audio.

- **Sans-IO core**: the protocol is a pure state machine (`bytes in → events out`, `commands → bytes out`), testable without sockets.
- **Pull-based audio** *(planned)*: the client asks your source for the next 20 ms frame, so there is no buffer to poll and skips take effect immediately.
- **Small on purpose**: client only, Opus over the TLS tunnel, Mumble 1.5+. No UDP/OCB2, no ACL editing, no positional audio.

## Install

Requires Python 3.12+.

```sh
pip install "pymumble-rev @ git+https://github.com/AidenOfficial/PyMumble@main"
```

## Quick start

```python
import asyncio
from pymumble_rev import ClientConfig, MumbleClient, events


async def main() -> None:
    config = ClientConfig(username="dj-bot", client_type="bot")
    async with MumbleClient("mumble.example.org", config) as client:
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
- `events()` ends when the connection does. The client never reconnects by itself: put
  `async with MumbleClient(...)` in your own retry loop, and each attempt starts from a fresh,
  fully synced state.
- Commands are coroutines that wait for the socket to drain, so a slow link slows the sender down
  instead of being mistaken for a disconnect. A server that stops answering pings for
  `dead_after` seconds (default 20) is treated as gone.
- The server certificate is not verified by default, because nearly every Mumble server uses a
  self-signed one. Pass `ssl_context=` to verify it, or
  `default_ssl_context(certfile, keyfile)` to log in with a client certificate.

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

## License

MIT. The `.proto` files in `proto/` are © The Mumble Developers under the BSD-3-Clause license
(`proto/LICENSE`).
