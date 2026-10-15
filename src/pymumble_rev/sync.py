"""Blocking wrapper around :class:`MumbleClient` for threaded applications.

The asyncio client runs on a private event loop in a daemon thread. Every method here may be
called from any thread except that loop's own thread (doing so would deadlock, so it raises).
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import logging
import queue
import threading
from collections.abc import Coroutine, Iterable, Iterator
from types import TracebackType
from typing import Any, Self, TypeVar

from . import events as ev
from .audio import AudioSender, AudioSource, PcmBuffer
from .client import MumbleClient
from .connection import ClientConfig, VoiceTarget
from .errors import ConnectionClosedError, MumbleError
from .state import ServerState

log = logging.getLogger(__name__)

T = TypeVar("T")


class ThreadedClient:
    """A :class:`MumbleClient` you can drive from ordinary threads.

    .. code-block:: python

        with ThreadedClient("mumble.example.org", ClientConfig(username="bot")) as client:
            for event in client.events():          # blocks; ends when the connection ends
                if isinstance(event, events.TextMessage) and event.actor is not None:
                    client.send_text("pong", sessions=[event.actor])

    Like :class:`MumbleClient` it owns one connection and never reconnects; create a new one
    to reconnect. Keyword arguments are passed through to :class:`MumbleClient`.
    """

    def __init__(
        self,
        host: str,
        config: ClientConfig,
        *,
        command_timeout: float = 10.0,
        max_queued_events: int = 4096,
        **client_kwargs: Any,
    ) -> None:
        self.command_timeout = command_timeout
        self._client = MumbleClient(
            host, config, max_queued_events=max_queued_events, **client_kwargs
        )
        self._events: queue.Queue[ev.Event | None] = queue.Queue()
        self._max_queued = max_queued_events
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    # —— 生命周期 ——————————————————————————————————————————————————————

    def connect(self) -> None:
        """Connect and wait for the server's sync. Raises like :meth:`MumbleClient.connect`."""
        with self._lock:
            if self._thread is not None:
                raise MumbleError("a ThreadedClient connects only once; create a new one")
            ready = threading.Event()

            def run_loop() -> None:
                loop = asyncio.new_event_loop()
                self._loop = loop
                asyncio.set_event_loop(loop)
                ready.set()
                try:
                    loop.run_forever()
                finally:
                    # 停循环时可能还有没跑完的任务(事件搬运等):取消并等它们结束,避免
                    # "Task was destroyed but it is pending"
                    pending = asyncio.all_tasks(loop)
                    for task in pending:
                        task.cancel()
                    if pending:
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                    loop.run_until_complete(loop.shutdown_asyncgens())
                    loop.close()

            self._thread = threading.Thread(target=run_loop, name="mumble-loop", daemon=True)
            self._thread.start()
            ready.wait()

        try:
            # connect 自己带 connect_timeout,这里不再叠加超时
            self._call(self._connect_and_pump(), timeout=None)
        except BaseException:
            self._stop_loop()
            raise

    async def _connect_and_pump(self) -> None:
        await self._client.connect()
        asyncio.get_running_loop().create_task(self._pump(), name="mumble-pump")

    async def _pump(self) -> None:
        # 把 asyncio 队列里的事件搬到线程安全的队列;连接结束时放入哨兵
        async for event in self._client.events():
            self._put(event)
        self._put(None)

    def _put(self, item: ev.Event | None) -> None:
        if item is not None and self._events.qsize() >= self._max_queued:
            with contextlib.suppress(queue.Empty):
                self._events.get_nowait()
            log.warning("threaded event queue full, dropped the oldest event")
        self._events.put(item)

    def close(self, timeout: float = 5.0) -> None:
        """Close the connection and stop the loop thread. Idempotent."""
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        if loop.is_running():
            try:
                self._call(self._client.close(), timeout=timeout)
            except TimeoutError:
                log.warning("close() timed out; stopping the loop anyway")
        self._stop_loop(timeout)

    def _stop_loop(self, timeout: float = 5.0) -> None:
        loop, thread = self._loop, self._thread
        if loop is not None and not loop.is_closed():
            loop.call_soon_threadsafe(loop.stop)
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout)
        # 连接从没建立时 pump 不会放哨兵,这里补一个,让 events() 能结束
        self._events.put(None)

    def wait_closed(self, timeout: float | None = None) -> str | None:
        """Block until the connection ends; return its close reason."""
        return self._call(self._client.wait_closed(), timeout=timeout)

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # —— 状态(只读,可在任意线程直接访问)———————————————————————————————————

    @property
    def state(self) -> ServerState:
        return self._client.state

    @property
    def connected(self) -> bool:
        return self._client.connected

    @property
    def close_reason(self) -> str | None:
        return self._client.close_reason

    @property
    def rtt(self) -> float | None:
        return self._client.rtt

    # —— 事件 ——————————————————————————————————————————————————————————

    def events(self) -> Iterator[ev.Event]:
        """Block for events until the connection ends. Use a single consumer."""
        while (event := self.get_event()) is not None:
            yield event

    def get_event(self, timeout: float | None = None) -> ev.Event | None:
        """Next event, or ``None`` once the connection has ended.

        Raises :class:`TimeoutError` if ``timeout`` expires first.
        """
        try:
            item = self._events.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError("no event within timeout") from None
        if item is None:
            self._events.put(None)  # 让后续调用同样立刻得到 None
        return item

    # —— 命令(阻塞到数据写入 socket)—————————————————————————————————————

    def send_text(
        self,
        message: str,
        *,
        sessions: Iterable[int] = (),
        channel_ids: Iterable[int] = (),
        tree_ids: Iterable[int] = (),
    ) -> None:
        self._command(
            self._client.send_text(
                message,
                sessions=list(sessions),
                channel_ids=list(channel_ids),
                tree_ids=list(tree_ids),
            )
        )

    def move(self, channel_id: int, session: int | None = None) -> None:
        self._command(self._client.move(channel_id, session))

    def set_self_state(
        self,
        *,
        self_mute: bool | None = None,
        self_deaf: bool | None = None,
        comment: str | None = None,
        texture: bytes | None = None,
        listen_add: Iterable[int] = (),
        listen_remove: Iterable[int] = (),
    ) -> None:
        self._command(
            self._client.set_self_state(
                self_mute=self_mute,
                self_deaf=self_deaf,
                comment=comment,
                texture=texture,
                listen_add=list(listen_add),
                listen_remove=list(listen_remove),
            )
        )

    def request_blob(
        self,
        *,
        comments: Iterable[int] = (),
        textures: Iterable[int] = (),
        descriptions: Iterable[int] = (),
    ) -> None:
        self._command(
            self._client.request_blob(
                comments=list(comments), textures=list(textures), descriptions=list(descriptions)
            )
        )

    def set_tokens(self, tokens: Iterable[str]) -> None:
        """Replace the channel access tokens (see :meth:`Connection.set_tokens`)."""
        self._command(self._client.set_tokens(list(tokens)))

    def set_voice_target(self, target_id: int, targets: Iterable[VoiceTarget]) -> None:
        """Register a whisper/shout target (see :meth:`Connection.set_voice_target`)."""
        self._command(self._client.set_voice_target(target_id, list(targets)))

    # —— 音频 ——————————————————————————————————————————————————————————

    def play(self, source: AudioSource, *, target: int = 0) -> concurrent.futures.Future[None]:
        """Switch to ``source`` and return once the switch has happened.

        The returned future completes when the source ends, is replaced or stopped, and fails
        with :class:`ConnectionClosedError` if the connection drops (see
        :meth:`MumbleClient.play`). ``source.read`` runs on the loop thread and must not block.
        """
        outer: concurrent.futures.Future[None] = concurrent.futures.Future()

        def relay(inner: asyncio.Future[None]) -> None:
            if inner.cancelled():
                outer.cancel()
            elif (exc := inner.exception()) is not None:
                outer.set_exception(exc)
            else:
                outer.set_result(None)

        async def start() -> None:
            # 在循环线程里同步完成切换:返回之后再调 stop_audio()/volume 一定排在它后面
            self._client.play(source, target=target).add_done_callback(relay)

        self._command(start())
        return outer

    def open_stream(
        self, capacity: float = 0.5, *, target: int = 0
    ) -> tuple[PcmBuffer, concurrent.futures.Future[None]]:
        """Start playing a new :class:`PcmBuffer` and return it with its completion future.

        Feed it with ``buffer.write(pcm)`` from your thread (it blocks while ``capacity``
        seconds are queued), ``buffer.clear()`` to skip, and ``buffer.close()`` at the end.
        """
        buffer = PcmBuffer(self._client.audio_channels, capacity)
        return buffer, self.play(buffer, target=target)

    def stop_audio(self) -> None:
        self._require_loop().call_soon_threadsafe(self._client.stop_audio)

    @property
    def volume(self) -> float:
        return self._client.volume

    @volume.setter
    def volume(self, value: float) -> None:
        def apply() -> None:
            self._client.volume = value

        try:
            loop = self._require_loop()
        except ConnectionClosedError:
            # 循环没在跑(还没连接或已关闭)就没有并发的音频任务,直接设即可
            apply()
            return
        loop.call_soon_threadsafe(apply)

    @property
    def receive_audio(self) -> bool:
        """Whether :class:`events.AudioReceived` is delivered (toggle at any time)."""
        return self._client.receive_audio

    @receive_audio.setter
    def receive_audio(self, value: bool) -> None:
        # 只是一个 bool 赋值,读写都是原子的,不必跳到循环线程
        self._client.receive_audio = bool(value)

    @property
    def audio(self) -> AudioSender | None:
        """Read-only access to sender statistics and bitrate."""
        return self._client.audio

    # —— 内部 ——————————————————————————————————————————————————————————

    def _require_loop(self) -> asyncio.AbstractEventLoop:
        loop = self._loop
        if loop is None or loop.is_closed() or not loop.is_running():
            raise ConnectionClosedError("not connected")
        return loop

    def _command(self, coro: Coroutine[Any, Any, T]) -> T:
        return self._call(coro, timeout=self.command_timeout)

    def _call(self, coro: Coroutine[Any, Any, T], timeout: float | None) -> T:
        loop = self._loop
        if loop is None or loop.is_closed() or not loop.is_running():
            coro.close()
            raise ConnectionClosedError("not connected")
        if threading.current_thread() is self._thread:
            coro.close()
            raise RuntimeError(
                "blocking ThreadedClient methods cannot be called from its own loop thread"
            )
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        try:
            return future.result(timeout)
        except concurrent.futures.TimeoutError:
            future.cancel()
            raise TimeoutError(f"no result within {timeout}s") from None
