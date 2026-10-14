from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

from pymumble_rev import ClientConfig, events
from pymumble_rev.client import MumbleClient, default_ssl_context
from pymumble_rev.errors import ConnectionClosedError, ConnectionRejected, MumbleError
from pymumble_rev.messages import TcpType, UdpType, pb, udp_pb

from .fake_server import DATA, FakeServer
from .helpers import OWN_SESSION

# 不引入 pytest-asyncio:每个测试自己 asyncio.run,且整体有超时兜底


def run(
    fn: Callable[[], Coroutine[Any, Any, None]],
) -> Callable[[], None]:
    def wrapper() -> None:
        async def guarded() -> None:
            async with asyncio.timeout(10):
                await fn()

        asyncio.run(guarded())

    wrapper.__name__ = fn.__name__
    return wrapper


def client_for(server: FakeServer, **kwargs: Any) -> MumbleClient:
    return MumbleClient("127.0.0.1", ClientConfig(username="dj-bot"), port=server.port, **kwargs)


async def next_event(client: MumbleClient, kind: type[events.Event]) -> events.Event:
    async for event in client.events():
        if isinstance(event, kind):
            return event
    raise AssertionError(f"stream ended before {kind.__name__}")


@run
async def test_connect_syncs_state_and_handshakes() -> None:
    async with FakeServer() as server, client_for(server) as client:
        assert client.connected
        assert client.state.session == OWN_SESSION
        assert client.state.users[5].name == "alice"
        version, auth = server.received[:2]
        assert isinstance(version, pb.Version)
        assert isinstance(auth, pb.Authenticate) and auth.username == "dj-bot"
        first = await next_event(client, events.Connected)
        assert isinstance(first, events.Connected)


@run
async def test_commands_reach_the_server() -> None:
    async with FakeServer() as server, client_for(server) as client:
        await client.send_text("hi", channel_ids=[1])
        await client.move(1)
        await client.send_audio(b"opus", frame_number=2)
        text = await server.wait_for(pb.TextMessage)
        assert isinstance(text, pb.TextMessage) and text.message == "hi"
        move = await server.wait_for(pb.UserState)
        assert isinstance(move, pb.UserState) and move.channel_id == 1
        tunnel = next(m for m in server.received if isinstance(m, bytes))
        assert tunnel[0] == UdpType.Audio


@run
async def test_events_stream_and_end_on_server_close() -> None:
    async with FakeServer() as server, client_for(server) as client:
        await server.send(pb.TextMessage(actor=5, channel_id=[0], message="!play"))
        msg = await next_event(client, events.TextMessage)
        assert isinstance(msg, events.TextMessage) and msg.message == "!play"

        await server.drop()
        seen = [type(e) async for e in client.events()]
        assert seen[-1] is events.ConnectionClosed
        assert seen.count(events.ConnectionClosed) == 1
        assert not client.connected
        assert client.close_reason is not None

        with pytest.raises(ConnectionClosedError):
            await client.send_text("too late", channel_ids=[0])
        # 关闭后再迭代立即结束
        assert [e async for e in client.events()] == []


@run
async def test_rejected_connect_raises() -> None:
    async with FakeServer(reject=("WrongServerPW", "bad password")) as server:
        client = client_for(server)
        with pytest.raises(ConnectionRejected) as info:
            await client.connect()
        assert info.value.kind == "WrongServerPW"
        assert info.value.reason == "bad password"
        assert not client.connected
        assert client.close_reason is not None and "WrongServerPW" in client.close_reason


@run
async def test_connect_times_out_when_server_never_syncs() -> None:
    async with FakeServer(on_auth=False) as server:
        client = client_for(server, connect_timeout=0.3)
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            await client.connect()
        assert time.monotonic() - started < 2
        assert client.close_reason is not None and "TimeoutError" in client.close_reason


@run
async def test_connect_refused_raises_oserror() -> None:
    async with FakeServer() as server:
        port = server.port
    client = MumbleClient("127.0.0.1", ClientConfig(username="u"), port=port)
    with pytest.raises(OSError):
        await client.connect()


@run
async def test_client_connects_only_once() -> None:
    async with FakeServer() as server:
        client = client_for(server)
        await client.connect()
        await client.close()
        with pytest.raises(MumbleError):
            await client.connect()


@run
async def test_pings_measure_rtt() -> None:
    async with FakeServer() as server, client_for(server, ping_interval=0.05) as client:
        await server.wait_for(pb.Ping)
        await next_event(client, events.Pong)
        assert client.rtt is not None and 0 <= client.rtt < 1


@run
async def test_silent_server_is_detected_as_dead() -> None:
    async with FakeServer(echo_pings=False) as server:
        client = client_for(server, ping_interval=0.05, dead_after=0.3)
        await client.connect()
        reason = await client.wait_closed()
        assert reason is not None and "no data" in reason
        assert [type(e) async for e in client.events()][-1] is events.ConnectionClosed


@run
async def test_audio_events_are_opt_in() -> None:
    audio = udp_pb.Audio(sender_session=5, frame_number=1, opus_data=b"x")
    packet = bytes([UdpType.Audio]) + audio.SerializeToString()
    from pymumble_rev.framing import encode_frame

    tunnel = encode_frame(TcpType.UDPTunnel, packet)
    marker = pb.TextMessage(actor=5, channel_id=[0], message="after audio")

    async with FakeServer() as server, client_for(server) as quiet:
        await server.send_raw(tunnel)
        await server.send(marker)
        seen = []
        async for event in quiet.events():
            seen.append(type(event))
            if isinstance(event, events.TextMessage):
                break
        assert events.AudioReceived not in seen

    async with FakeServer() as server, client_for(server, receive_audio=True) as loud:
        await server.send_raw(tunnel)
        got = await next_event(loud, events.AudioReceived)
        assert isinstance(got, events.AudioReceived) and got.opus_data == b"x"


@run
async def test_slow_consumer_drops_oldest_events() -> None:
    async with FakeServer() as server, client_for(server, max_queued_events=5) as client:
        msgs = [pb.TextMessage(actor=5, channel_id=[0], message=str(i)) for i in range(20)]
        await server.send(*msgs, pb.TextMessage(actor=5, channel_id=[0], message="last"))
        # 先不读,等客户端把这批全部收进队列
        await asyncio.sleep(0.2)
        texts: list[str] = []
        async for event in client.events():
            if isinstance(event, events.TextMessage):
                texts.append(event.message)
                if event.message == "last":
                    break
        assert texts[-1] == "last"
        assert len(texts) <= 5
        assert texts == [str(i) for i in range(16, 20)] + ["last"]


@run
async def test_full_socket_applies_backpressure_instead_of_disconnecting() -> None:
    async with FakeServer() as server, client_for(server, dead_after=60) as client:
        server.pause_reading()
        blob = "x" * 60_000
        blocked = False
        # 对端不读时,内核缓冲和 asyncio 写缓冲迟早写满,send 应当挂起而不是报错断线
        for _ in range(2_000):
            try:
                async with asyncio.timeout(0.2):
                    await client.send_text(blob, channel_ids=[0])
            except TimeoutError:
                blocked = True
                break
        assert blocked
        assert client.connected

        server.pause_reading(False)
        await client.send_text("after", channel_ids=[0])
        assert client.connected


@run
async def test_frozen_server_is_detected_while_a_send_is_blocked() -> None:
    # 服务器既不读也不写(进程卡死、网络黑洞):发送方卡在 drain,心跳仍要能判定死亡并解开发送方
    async with FakeServer(echo_pings=False) as server:
        client = client_for(server, ping_interval=0.05, dead_after=0.5)
        await client.connect()
        server.pause_reading()

        async def flood() -> None:
            while True:
                await client.send_text("x" * 60_000, channel_ids=[0])

        sender = asyncio.create_task(flood())
        reason = await asyncio.wait_for(client.wait_closed(), 5)
        assert reason is not None and "no data" in reason
        with pytest.raises(ConnectionClosedError):
            await asyncio.wait_for(sender, 5)


@run
async def test_close_is_idempotent_and_ends_stream() -> None:
    async with FakeServer() as server:
        client = client_for(server)
        await client.connect()
        await client.close()
        await client.close()
        assert client.close_reason == "closed locally"
        assert [type(e) async for e in client.events()][-1] is events.ConnectionClosed


def test_default_ssl_context_loads_client_certificate() -> None:
    ctx = default_ssl_context(str(DATA / "cert.pem"), str(DATA / "key.pem"))
    assert ctx.verify_mode.name == "CERT_NONE"


# —— 音频 ——————————————————————————————————————————————————————————————


def tunnelled_audio(server: FakeServer) -> list[udp_pb.Audio]:
    out = []
    for msg in server.received:
        if isinstance(msg, bytes) and msg[:1] == bytes([UdpType.Audio]):
            audio = udp_pb.Audio()
            audio.ParseFromString(msg[1:])
            out.append(audio)
    return out


@run
async def test_play_streams_paced_opus_to_the_server() -> None:
    from pymumble_rev.audio import BytesSource

    from .test_opus import tone

    async with FakeServer() as server, client_for(server, channels=2) as client:
        assert client.audio is None  # 不放音就不加载 libopus
        await client.play(BytesSource(tone(100, 2)))
        await asyncio.sleep(0.05)
        packets = tunnelled_audio(server)
        assert [p.frame_number for p in packets] == [0, 2, 4, 6, 8, 10]
        assert [p.is_terminator for p in packets] == [False] * 5 + [True]
        assert client.audio is not None
        # 假服务器的 max_bandwidth 是 558000,不会压低默认码率
        assert client.audio.bitrate == 96_000


@run
async def test_play_requires_a_connection_and_fails_on_drop() -> None:
    from pymumble_rev.audio import BytesSource, PcmBuffer

    async with FakeServer() as server:
        client = client_for(server)
        with pytest.raises(ConnectionClosedError):
            client.play(BytesSource(b""))
        await client.connect()
        playing = client.play(PcmBuffer(1))  # 永不结束的源
        await server.drop()
        with pytest.raises(ConnectionClosedError):
            await asyncio.wait_for(playing, 5)


@run
async def test_bitrate_follows_server_bandwidth_limit() -> None:
    from pymumble_rev.audio import PcmBuffer

    async with FakeServer() as server, client_for(server, bitrate=128_000) as client:
        client.play(PcmBuffer(1))
        assert client.audio is not None and client.audio.bitrate == 128_000
        await server.send(pb.ServerConfig(max_bandwidth=72_000))
        # 队列里还有同步阶段的 ServerInfoUpdated,要等到带新限速的那一个
        async for event in client.events():
            if isinstance(event, events.ServerInfoUpdated) and event.server.max_bandwidth == 72_000:
                break
        assert client.audio.bitrate == 72_000 - 57 * 8 * 50
