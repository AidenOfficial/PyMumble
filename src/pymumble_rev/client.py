"""Asyncio transport for :class:`Connection`."""

from __future__ import annotations

import asyncio
import logging
import ssl
from collections.abc import AsyncIterator, Coroutine, Iterable
from types import TracebackType
from typing import Any, NamedTuple, Self

from . import events as ev
from .audio import AudioSender, AudioSource
from .connection import ClientConfig, Connection, Phase
from .errors import ConnectionClosedError, ConnectionRejected, MumbleError
from .opus import Application
from .state import ServerState

log = logging.getLogger(__name__)

DEFAULT_PORT = 64738
_READ_SIZE = 64 * 1024


def default_ssl_context(certfile: str | None = None, keyfile: str | None = None) -> ssl.SSLContext:
    """TLS context that does not verify the server certificate.

    Almost every Mumble server uses a self-signed certificate, which is why the official client
    asks the user to accept it on first connect. Pass your own context to verify it.
    ``certfile``/``keyfile`` load a client certificate (PEM), which is how Mumble identifies
    registered users.
    """
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    if certfile:
        ctx.load_cert_chain(certfile, keyfile)
    return ctx


class _AudioSettings(NamedTuple):
    channels: int
    frame_ms: int
    bitrate: int
    application: Application


class MumbleClient:
    """One connection to a Mumble server, driven by the running event loop.

    .. code-block:: python

        async with MumbleClient("mumble.example.org", ClientConfig(username="bot")) as client:
            async for event in client.events():
                if isinstance(event, events.TextMessage):
                    await client.send_text("pong", sessions=[event.actor])

    The client never reconnects on its own: when the connection ends, :meth:`events` stops and
    :attr:`close_reason` says why. Wrap it in your own retry loop to reconnect; every
    connection starts from a fresh, fully synced :attr:`state`.

    All methods must be called from the loop that ran :meth:`connect`.
    """

    def __init__(
        self,
        host: str,
        config: ClientConfig,
        *,
        port: int = DEFAULT_PORT,
        ssl_context: ssl.SSLContext | None = None,
        connect_timeout: float = 10.0,
        ping_interval: float = 5.0,
        dead_after: float = 20.0,
        receive_audio: bool = False,
        max_queued_events: int = 4096,
        channels: int = 1,
        frame_ms: int = 20,
        bitrate: int = 96_000,
        opus_application: Application = Application.AUDIO,
    ) -> None:
        """
        :param connect_timeout: Seconds allowed for TCP, TLS and the server's initial sync.
        :param ping_interval: Seconds between keep-alive pings.
        :param dead_after: Close the connection when nothing at all arrived for this long.
        :param receive_audio: Deliver :class:`events.AudioReceived`. Off by default because a
            bot that does not listen would only queue packets it never reads.
        :param max_queued_events: Oldest events are dropped (with a warning) past this many
            unread events, so a consumer that stops reading cannot grow memory without bound.
        :param channels: 1 or 2; the PCM that sources return must match.
        :param frame_ms: Audio per packet (10, 20, 40 or 60 ms).
        :param bitrate: Opus bitrate; capped automatically to the server's bandwidth limit.
        """
        self.host = host
        self.port = port
        self.config = config
        self.ssl_context = ssl_context or default_ssl_context()
        self.connect_timeout = connect_timeout
        self.ping_interval = ping_interval
        self.dead_after = dead_after
        self.receive_audio = receive_audio

        self._conn = Connection(config)
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._tasks: list[asyncio.Task[None]] = []
        # 队列本身不设上限(关闭哨兵必须放得进去),上限在 _emit 里自己管
        self._events: asyncio.Queue[ev.Event | None] = asyncio.Queue()
        self._max_queued = max_queued_events
        self._dropped = 0
        self._closed_emitted = False
        self._connected: asyncio.Future[None] | None = None
        self._closed = asyncio.Event()
        self._last_rx = 0.0
        #: Most recent ping round-trip time in seconds.
        self.rtt: float | None = None
        self._audio_settings = _AudioSettings(channels, frame_ms, bitrate, opus_application)
        self._audio: AudioSender | None = None
        self._volume = 1.0

    # —— 生命周期 ——————————————————————————————————————————————————————

    @property
    def state(self) -> ServerState:
        """Immutable snapshot of users, channels and server settings. Safe from any thread."""
        return self._conn.state

    @property
    def connected(self) -> bool:
        return self._conn.phase is Phase.CONNECTED and not self._closed.is_set()

    @property
    def close_reason(self) -> str | None:
        return self._conn.close_reason if self._closed.is_set() else None

    async def connect(self) -> None:
        """Open the connection and wait until the server has synced.

        Raises :class:`ConnectionRejected` if the server refuses us, :class:`TimeoutError` if it
        takes longer than ``connect_timeout``, and :class:`OSError` for network failures.
        """
        if self._conn.phase is not Phase.IDLE:
            raise MumbleError("a MumbleClient connects only once; create a new one to reconnect")
        loop = asyncio.get_running_loop()
        self._connected = loop.create_future()
        try:
            async with asyncio.timeout(self.connect_timeout):
                self._reader, self._writer = await asyncio.open_connection(
                    self.host,
                    self.port,
                    ssl=self.ssl_context,
                    server_hostname=self.host,
                )
                self._last_rx = loop.time()
                self._conn.initiate()
                self._spawn(self._read_loop(), "read")
                self._spawn(self._ping_loop(), "ping")
                await self._flush()
                await self._connected
        except BaseException as exc:
            await self._shutdown(f"connect failed: {type(exc).__name__}: {exc}")
            raise

    async def close(self) -> None:
        """Close the connection. Idempotent; safe to call from any coroutine on the loop."""
        await self._shutdown("closed locally")

    async def wait_closed(self) -> str | None:
        """Wait until the connection ends and return :attr:`close_reason`."""
        await self._closed.wait()
        return self.close_reason

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def events(self) -> AsyncIterator[ev.Event]:
        """Yield events until the connection closes. Use a single consumer."""
        while True:
            item = await self._events.get()
            if item is None:
                # 留给下一个 events() 调用者:关闭后再迭代也能立即结束
                self._events.put_nowait(None)
                return
            yield item

    # —— 命令(写入后等 drain,对端读不动时这里自然限速)——————————————————

    async def send_text(
        self,
        message: str,
        *,
        sessions: Iterable[int] = (),
        channel_ids: Iterable[int] = (),
        tree_ids: Iterable[int] = (),
    ) -> None:
        self._conn.send_text(message, sessions=sessions, channel_ids=channel_ids, tree_ids=tree_ids)
        await self._flush()

    async def move(self, channel_id: int, session: int | None = None) -> None:
        self._conn.move(channel_id, session)
        await self._flush()

    async def set_self_state(
        self,
        *,
        self_mute: bool | None = None,
        self_deaf: bool | None = None,
        comment: str | None = None,
        texture: bytes | None = None,
        listen_add: Iterable[int] = (),
        listen_remove: Iterable[int] = (),
    ) -> None:
        self._conn.set_self_state(
            self_mute=self_mute,
            self_deaf=self_deaf,
            comment=comment,
            texture=texture,
            listen_add=listen_add,
            listen_remove=listen_remove,
        )
        await self._flush()

    async def request_blob(
        self,
        *,
        comments: Iterable[int] = (),
        textures: Iterable[int] = (),
        descriptions: Iterable[int] = (),
    ) -> None:
        self._conn.request_blob(comments=comments, textures=textures, descriptions=descriptions)
        await self._flush()

    async def send_audio(
        self, opus_data: bytes, *, frame_number: int, target: int = 0, is_terminator: bool = False
    ) -> None:
        self._conn.send_audio(
            opus_data, frame_number=frame_number, target=target, is_terminator=is_terminator
        )
        await self._flush()

    # —— 音频 ——————————————————————————————————————————————————————————

    def play(self, source: AudioSource) -> asyncio.Future[None]:
        """Start pulling audio from ``source`` now, replacing whatever was playing.

        The returned future resolves when the source ends, is replaced, or is stopped, and
        fails with :class:`ConnectionClosedError` if the connection drops first. The first call
        loads libopus.
        """
        if not self.connected:
            raise ConnectionClosedError(f"not connected ({self._conn.close_reason})")
        if self._audio is None:
            settings = self._audio_settings
            self._audio = AudioSender(
                self._send_audio_packet,
                channels=settings.channels,
                frame_ms=settings.frame_ms,
                bitrate=settings.bitrate,
                application=settings.application,
            )
            self._audio.volume = self._volume
            self._audio.limit_bandwidth(self.state.server.max_bandwidth)
            self._spawn(self._audio.run(), "audio")
        return self._audio.play(source)

    def stop_audio(self) -> None:
        """Stop the current source and end the transmission."""
        if self._audio is not None:
            self._audio.stop()

    @property
    def volume(self) -> float:
        """Gain applied to outgoing PCM (1.0 = unchanged). Changes take effect within a frame."""
        return self._volume

    @volume.setter
    def volume(self, value: float) -> None:
        self._volume = max(0.0, float(value))
        if self._audio is not None:
            self._audio.volume = self._volume

    @property
    def audio_channels(self) -> int:
        """Channel count that audio sources must produce (1 or 2)."""
        return self._audio_settings.channels

    @property
    def audio(self) -> AudioSender | None:
        """The audio sender, once :meth:`play` has been called (bitrate, stats, ...)."""
        return self._audio

    async def _send_audio_packet(self, packet: bytes, frame_number: int, terminator: bool) -> None:
        await self.send_audio(packet, frame_number=frame_number, is_terminator=terminator)

    # —— 内部 ——————————————————————————————————————————————————————————

    def _spawn(self, coro: Coroutine[Any, Any, None], name: str) -> None:
        self._tasks.append(asyncio.create_task(coro, name=f"mumble-{name}"))

    async def _flush(self, *, drain: bool = True) -> None:
        data = self._conn.data_to_send()
        if self._closed.is_set():
            raise ConnectionClosedError(f"connection closed ({self._conn.close_reason})")
        if not data or self._writer is None:
            return
        try:
            self._writer.write(data)
            if drain:
                await self._writer.drain()
        except (OSError, RuntimeError) as exc:
            # RuntimeError: 传输层已关闭时 write/drain 可能抛这个
            await self._shutdown(f"write failed: {exc}")
            raise ConnectionClosedError(f"connection closed ({self._conn.close_reason})") from exc

    def _emit(self, event: ev.Event) -> None:
        if isinstance(event, ev.ConnectionClosed):
            self._closed_emitted = True
        if self._events.qsize() >= self._max_queued:
            self._events.get_nowait()
            self._dropped += 1
            if self._dropped == 1 or self._dropped % 1000 == 0:
                log.warning("event queue full, dropped %d events so far", self._dropped)
        self._events.put_nowait(event)

    async def _read_loop(self) -> None:
        assert self._reader is not None
        loop = asyncio.get_running_loop()
        reason = "connection closed by peer"
        try:
            while True:
                data = await self._reader.read(_READ_SIZE)
                self._last_rx = loop.time()
                for event in self._conn.receive_data(data):
                    self._handle(event, loop.time())
                if self._conn.phase is Phase.CLOSED:
                    reason = self._conn.close_reason or reason
                    break
        except asyncio.CancelledError:
            raise
        except (OSError, MumbleError) as exc:
            # 包括 ssl.SSLError / ConnectionResetError / ProtocolError
            reason = f"{type(exc).__name__}: {exc}"
        except Exception as exc:
            # 本库自身的 bug 也要把连接关掉,否则 ping 照常往返,连接看起来活着却不再处理消息
            log.exception("unexpected error while reading from the server")
            reason = f"internal error: {type(exc).__name__}: {exc}"
        # 不能直接 await _shutdown:它会取消并等待本任务自己
        asyncio.get_running_loop().call_soon(self._shutdown_soon, reason)

    def _handle(self, event: ev.Event, now: float) -> None:
        match event:
            case ev.Connected():
                if self._connected and not self._connected.done():
                    self._connected.set_result(None)
            case ev.Rejected(kind=kind, reason=reason):
                if self._connected and not self._connected.done():
                    self._connected.set_exception(ConnectionRejected(kind, reason))
            case ev.Pong(sent_at=sent_at):
                self.rtt = max(0.0, now - sent_at)
            case ev.ServerInfoUpdated(server=server) if self._audio is not None:
                self._audio.limit_bandwidth(server.max_bandwidth)
            case ev.AudioReceived() if not self.receive_audio:
                return
        self._emit(event)

    async def _ping_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            await asyncio.sleep(self.ping_interval)
            idle = loop.time() - self._last_rx
            if idle > self.dead_after:
                await self._shutdown(f"no data from server for {idle:.0f}s")
                return
            self._conn.ping(loop.time())
            try:
                # 不等 drain:对端不读时 drain 会一直挂着,这个循环就再也检查不到 dead_after
                await self._flush(drain=False)
            except ConnectionClosedError:
                return

    def _shutdown_soon(self, reason: str) -> None:
        self._spawn(self._shutdown(reason), "shutdown")

    async def _shutdown(self, reason: str) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        self._conn.close(reason)
        log.info("mumble connection closed: %s", self._conn.close_reason)
        if self._connected and not self._connected.done():
            self._connected.set_exception(ConnectionError(self._conn.close_reason))
            # 没人 await 时避免 "exception was never retrieved"
            self._connected.exception()

        current = asyncio.current_task()
        for task in self._tasks:
            if task is not current and not task.done():
                task.cancel()
        writer, self._writer = self._writer, None
        if writer is not None:
            writer.close()
            # TLS 的正常关闭要先把写缓冲发完并等 close_notify;对端不读或不回时最多等 2 秒,
            # 然后直接 abort,让卡在 drain() 里的发送方立刻拿到错误
            try:
                async with asyncio.timeout(2):
                    await writer.wait_closed()
            except TimeoutError:
                writer.transport.abort()
            except Exception:
                pass
        others = [t for t in self._tasks if t is not current and not t.done()]
        if others:
            # asyncio.wait 不会把子任务的取消/异常抛给我们,但我们自己被取消时照常抛出
            await asyncio.wait(others)
        # 对端 EOF 时 Connection 已经产出过 ConnectionClosed,不要重复
        if not self._closed_emitted:
            self._emit(ev.ConnectionClosed())
        self._events.put_nowait(None)
