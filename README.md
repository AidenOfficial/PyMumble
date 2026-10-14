# pymumble-rev

**Asyncio Mumble client library for Python — sans-IO protocol core and pull-based Opus audio, built for music and voice bots.**

[简体中文](README.zh-CN.md)

> **Status: pre-alpha.** The sans-IO protocol core is in place; the asyncio client and the audio
> pipeline are next. The API will change until 0.1.0.

An asyncio client for the Mumble voice protocol, designed for long-running bots that stream audio.

- **Sans-IO core**: the protocol is a pure state machine (`bytes in → events out`, `commands → bytes out`), testable without sockets.
- **Pull-based audio** *(planned)*: the client asks your source for the next 20 ms frame, so there is no buffer to poll and skips take effect immediately.
- **Small on purpose**: client only, Opus over the TLS tunnel, Mumble 1.5+. No UDP/OCB2, no ACL editing, no positional audio.

## Install

Requires Python 3.12+.

```sh
pip install "pymumble-rev @ git+https://github.com/AidenOfficial/PyMumble@main"
```

## Using the sans-IO core

`Connection` never touches a socket. You feed it the bytes you read and send the bytes it gives
you; it tells you what happened as typed events and keeps the server state (users, channels) as
immutable snapshots that are safe to read from any thread.

```python
import socket, ssl, time
from pymumble_rev import ClientConfig, Connection, events

conn = Connection(ClientConfig(username="dj-bot", client_type="bot"))
conn.initiate()  # queues Version + Authenticate

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE  # Mumble servers usually use self-signed certificates
sock = ctx.wrap_socket(socket.create_connection(("mumble.example.org", 64738)))

sock.sendall(conn.data_to_send())
while True:
    for event in conn.receive_data(sock.recv(65536)):
        match event:
            case events.Connected():
                me = conn.state.myself
                conn.send_text(f"hello from {me.name}", channel_ids=[me.channel_id])
            case events.TextMessage(actor=actor, message=text):
                print(conn.state.users[actor].name, text)
            case events.ConnectionClosed():
                raise SystemExit
    conn.ping(time.monotonic())  # call every few seconds in a real client
    sock.sendall(conn.data_to_send())
```

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
