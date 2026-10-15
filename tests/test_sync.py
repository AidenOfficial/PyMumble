from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Coroutine, Iterator
from contextlib import contextmanager
from typing import Any, TypeVar

import pytest

from pymumble_rev import ClientConfig, ThreadedClient, events
from pymumble_rev.errors import ConnectionClosedError, ConnectionRejected, MumbleError
from pymumble_rev.messages import UdpType, pb

from .fake_server import FakeServer
from .helpers import OWN_SESSION
from .test_opus import tone

T = TypeVar("T")


class ServerThread:
    """在独立线程的事件循环里跑 FakeServer,测试线程用同步代码驱动它。"""

    def __init__(self, server: FakeServer) -> None:
        self.server = server
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)

    def call(self, coro: Coroutine[Any, Any, T], timeout: float = 5) -> T:
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    def received(self, kind: type) -> list[Any]:
        return [m for m in list(self.server.received) if isinstance(m, kind)]


@contextmanager
def fake_server(**kwargs: Any) -> Iterator[ServerThread]:
    st = ServerThread.__new__(ServerThread)
    st.loop = asyncio.new_event_loop()
    st.thread = threading.Thread(target=st.loop.run_forever, daemon=True)
    st.thread.start()

    async def make() -> FakeServer:
        server = FakeServer(**kwargs)
        return await server.__aenter__()

    st.server = st.call(make())
    try:
        yield st
    finally:
        st.call(st.server.__aexit__(None, None, None))
        st.loop.call_soon_threadsafe(st.loop.stop)
        st.thread.join(5)
        st.loop.close()


def client_for(st: ServerThread, **kwargs: Any) -> ThreadedClient:
    return ThreadedClient(
        "127.0.0.1", ClientConfig(username="dj-bot"), port=st.server.port, **kwargs
    )


def wait_until(predicate: Any, timeout: float = 3) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        time.sleep(0.01)


def test_connect_events_and_commands() -> None:
    with fake_server() as st, client_for(st) as client:
        assert client.connected
        assert client.state.session == OWN_SESSION
        assert client.state.users[5].name == "alice"
        assert isinstance(client.get_event(timeout=2), events.Connected)

        client.send_text("hi", channel_ids=[1])
        client.move(1)
        client.set_self_state(comment="now playing")
        wait_until(lambda: st.received(pb.TextMessage))
        assert st.received(pb.TextMessage)[0].message == "hi"
        wait_until(lambda: len(st.received(pb.UserState)) == 2)

        st.call(st.server.send(pb.TextMessage(actor=5, channel_id=[0], message="!skip")))
        for event in client.events():
            if isinstance(event, events.TextMessage):
                assert event.message == "!skip"
                break


def test_events_end_and_commands_fail_after_server_drop() -> None:
    with fake_server() as st, client_for(st) as client:
        st.call(st.server.drop())
        seen = list(client.events())  # 阻塞到连接结束
        assert isinstance(seen[-1], events.ConnectionClosed)
        assert client.get_event(timeout=0) is None
        assert client.wait_closed(timeout=2) is not None
        with pytest.raises(ConnectionClosedError):
            client.send_text("too late", channel_ids=[0])


def test_rejected_connect_raises_and_cleans_up() -> None:
    with fake_server(reject=("ServerFull", "full")) as st:
        client = client_for(st)
        with pytest.raises(ConnectionRejected):
            client.connect()
        assert client._thread is not None
        client._thread.join(2)
        assert not client._thread.is_alive()
        assert client.get_event(timeout=1) is None  # 没连上也能让消费者退出
        with pytest.raises(ConnectionClosedError):
            client.move(1)


def test_connects_only_once_and_close_is_idempotent() -> None:
    with fake_server() as st:
        client = client_for(st)
        client.connect()
        with pytest.raises(MumbleError):
            client.connect()
        client.close()
        client.close()
        assert client._thread is not None and not client._thread.is_alive()


def test_get_event_timeout() -> None:
    with fake_server() as st, client_for(st) as client:
        client.get_event(timeout=1)  # Connected
        client.get_event(timeout=1)  # 同步后的 ServerInfoUpdated
        with pytest.raises(TimeoutError):
            client.get_event(timeout=0.05)


def test_blocking_call_from_loop_thread_is_refused() -> None:
    with fake_server() as st, client_for(st) as client:
        assert client._loop is not None
        result: list[BaseException] = []

        def inside_loop() -> None:
            try:
                client.move(1)
            except BaseException as exc:
                result.append(exc)

        client._loop.call_soon_threadsafe(inside_loop)
        wait_until(lambda: result)
        assert isinstance(result[0], RuntimeError)


def test_stream_from_a_producer_thread() -> None:
    with fake_server() as st, client_for(st, channels=2) as client:
        buf, done = client.open_stream(capacity=0.1)
        client.volume = 0.5

        def producer() -> None:
            pcm = tone(300, 2)
            for i in range(0, len(pcm), 3840):
                buf.write(pcm[i : i + 3840])  # 缓冲只有 0.1s,这里会被播放速度节流
            buf.close()

        started = time.monotonic()
        t = threading.Thread(target=producer)
        t.start()
        done.result(timeout=5)
        t.join()
        assert time.monotonic() - started > 0.25  # 实时播放,不是瞬间写完
        assert client.volume == 0.5

        def tunnel() -> list[bytes]:
            return [m for m in st.received(bytes) if m[:1] == bytes([UdpType.Audio])]

        wait_until(lambda: len(tunnel()) == 16)  # 15 帧 + 终止帧
        assert client.audio is not None and client.audio.stats["underrun_frames"] == 0


def test_stop_audio_resolves_the_stream() -> None:
    with fake_server() as st, client_for(st) as client:
        buf, done = client.open_stream()
        buf.write(tone(100, 1))
        client.stop_audio()
        done.result(timeout=2)
        assert st.server.port  # 只为让 st 在作用域内


def test_playback_future_fails_on_disconnect() -> None:
    with fake_server() as st, client_for(st) as client:
        _, done = client.open_stream()
        st.call(st.server.drop())
        with pytest.raises(ConnectionClosedError):
            done.result(timeout=5)


def test_volume_can_be_set_before_connecting() -> None:
    with fake_server() as st:
        client = client_for(st)
        client.volume = 0.25
        assert client.volume == 0.25
        with client:
            client.volume = 0.5
            wait_until(lambda: client.volume == 0.5)
        client.volume = 0.75  # 关闭后也不抛
        assert client.volume == 0.75


def test_open_stream_with_a_voice_target() -> None:
    from pymumble_rev import VoiceTarget

    with fake_server() as st, client_for(st) as client:
        client.set_voice_target(1, [VoiceTarget(sessions=[5])])
        wait_until(lambda: st.received(pb.VoiceTarget))
        buf, done = client.open_stream(target=1)
        buf.write(tone(40, 1))
        buf.close()
        done.result(timeout=2)
        wait_until(lambda: len(st.received(bytes)) == 3)
        assert st.received(pb.VoiceTarget)[0].id == 1


def test_tokens_and_receive_audio_toggle() -> None:
    with fake_server() as st, client_for(st) as client:
        client.set_tokens(["vip"])
        wait_until(lambda: len(st.received(pb.Authenticate)) == 2)
        assert list(st.received(pb.Authenticate)[-1].tokens) == ["vip"]
        assert client.receive_audio is False
        client.receive_audio = True
        assert client.receive_audio is True
