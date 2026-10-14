"""A scriptable in-process Mumble server over real TLS, for client tests."""

from __future__ import annotations

import asyncio
import contextlib
import pathlib
import ssl
from collections.abc import Awaitable, Callable

from google.protobuf.message import Message

from pymumble_rev.framing import FrameDecoder
from pymumble_rev.messages import TCP_CLASSES, TcpType, pb

from .helpers import frame, initial_sync

DATA = pathlib.Path(__file__).parent / "data"

Handler = Callable[["FakeServer", Message | bytes], Awaitable[None]]


class FakeServer:
    """Accepts one client at a time.

    Default behaviour: answer Authenticate with :func:`initial_sync` and echo pings. Override
    with ``on_auth=None`` (never sync), ``echo_pings=False`` (go silent), or ``reject=(kind,
    reason)``. Everything the client sends is recorded in :attr:`received`.
    """

    def __init__(
        self,
        *,
        sync: bytes | None = None,
        on_auth: bool = True,
        echo_pings: bool = True,
        reject: tuple[str, str] | None = None,
    ) -> None:
        self.sync = sync if sync is not None else initial_sync()
        self.on_auth = on_auth
        self.echo_pings = echo_pings
        self.reject = reject
        self.received: list[Message | bytes] = []
        self.got_message = asyncio.Condition()
        self._server: asyncio.Server | None = None
        self._writer: asyncio.StreamWriter | None = None
        self.port = 0

    async def __aenter__(self) -> FakeServer:
        ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ctx.load_cert_chain(DATA / "cert.pem", DATA / "key.pem")
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0, ssl=ctx)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.drop()
        assert self._server is not None
        self._server.close()
        with contextlib.suppress(Exception):
            async with asyncio.timeout(2):
                await self._server.wait_closed()

    async def send(self, *messages: Message) -> None:
        await self.send_raw(frame(*messages))

    async def send_raw(self, data: bytes) -> None:
        assert self._writer is not None, "no client connected"
        self._writer.write(data)
        await self._writer.drain()

    def pause_reading(self, paused: bool = True) -> None:
        """Stop (or resume) reading from the client, so its socket buffers fill up."""
        assert self._writer is not None, "no client connected"
        transport = self._writer.transport
        assert isinstance(transport, asyncio.Transport)
        if paused:
            transport.pause_reading()
        else:
            transport.resume_reading()

    async def drop(self) -> None:
        """Close the client's connection from the server side."""
        writer, self._writer = self._writer, None
        if writer is not None:
            writer.close()
            with contextlib.suppress(Exception):
                async with asyncio.timeout(2):
                    await writer.wait_closed()

    async def wait_for(self, kind: type[Message], timeout: float = 2.0) -> Message:
        """Wait until the client has sent a message of ``kind`` and return the latest one."""
        async with asyncio.timeout(timeout), self.got_message:
            while True:
                for msg in reversed(self.received):
                    if isinstance(msg, kind):
                        return msg
                await self.got_message.wait()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._writer = writer
        decoder = FrameDecoder()
        try:
            while data := await reader.read(65536):
                for msg_type, payload in decoder.feed(data):
                    await self._on_frame(msg_type, payload)
        except (OSError, asyncio.IncompleteReadError):
            pass

    async def _on_frame(self, msg_type: int, payload: bytes) -> None:
        kind = TcpType(msg_type)
        msg: Message | bytes
        if kind is TcpType.UDPTunnel:
            msg = payload
        else:
            msg = TCP_CLASSES[kind]()
            msg.ParseFromString(payload)
        async with self.got_message:
            self.received.append(msg)
            self.got_message.notify_all()

        if isinstance(msg, pb.Authenticate):
            if self.reject is not None:
                await self.send(pb.Reject(type=self.reject[0], reason=self.reject[1]))
                await self.drop()
            elif self.on_auth:
                await self.send_raw(self.sync)
        elif isinstance(msg, pb.Ping) and self.echo_pings:
            await self.send(pb.Ping(timestamp=msg.timestamp))
