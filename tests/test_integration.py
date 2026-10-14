"""Tests against a real Mumble server. Skipped unless PYMUMBLE_REV_SERVER=host:port is set.

本地跑法(Debian/Ubuntu):apt install mumble-server,用一个只监听 127.0.0.1、无密码、
autobanAttempts=0 的 ini 启动(否则短时间内的多次连接会触发服务器的自动封禁),
然后 PYMUMBLE_REV_SERVER=127.0.0.1:64738 uv run pytest tests/test_integration.py
"""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

import pytest

from pymumble_rev import ClientConfig, ConnectionRejected, MumbleClient, events

from .test_client import next_event, run

SERVER = os.environ.get("PYMUMBLE_REV_SERVER")
pytestmark = pytest.mark.skipif(not SERVER, reason="PYMUMBLE_REV_SERVER not set")


def make_client(name: str, **kwargs: Any) -> MumbleClient:
    assert SERVER is not None
    host, _, port = SERVER.rpartition(":")
    return MumbleClient(host, ClientConfig(username=name), port=int(port), **kwargs)


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:6]}"


@run
async def test_two_clients_see_each_other_and_talk() -> None:
    a_name, b_name = unique("a"), unique("b")
    async with make_client(a_name, ping_interval=0.5) as a:
        assert a.state.myself is not None and a.state.myself.name == a_name
        assert 0 in a.state.channels
        await next_event(a, events.Connected)

        async with make_client(b_name, receive_audio=True) as b:
            joined = await next_event(a, events.UserJoined)
            assert isinstance(joined, events.UserJoined) and joined.user.name == b_name
            assert b.state.myself is not None
            assert {u.name for u in b.state.users.values()} >= {a_name, b_name}

            # 文字
            await a.send_text("hello", channel_ids=[0])
            text = await next_event(b, events.TextMessage)
            assert isinstance(text, events.TextMessage)
            assert (text.actor, text.message) == (a.state.session, "hello")

            # 语音:服务器不解码 Opus,原样转发给同频道的人
            for n in range(5):
                await a.send_audio(b"\xf8\xff\xfe", frame_number=n * 2, is_terminator=n == 4)
            audio = await next_event(b, events.AudioReceived)
            assert isinstance(audio, events.AudioReceived)
            assert audio.sender_session == a.state.session
            assert audio.opus_data == b"\xf8\xff\xfe"

            # 自身状态
            await a.set_self_state(comment="now playing: nothing", self_mute=False)
            updated = await next_event(b, events.UserUpdated)
            assert isinstance(updated, events.UserUpdated)
            assert "comment" in updated.changed
            assert updated.user.comment == "now playing: nothing"

            # ping 往返
            await next_event(a, events.Pong)
            assert a.rtt is not None and a.rtt < 1

        left = await next_event(a, events.UserLeft)
        assert isinstance(left, events.UserLeft) and left.user.name == b_name


@run
async def test_invalid_username_is_rejected() -> None:
    # 空用户名一定会被拒绝(默认用户名正则其实连空格和感叹号都允许)
    with pytest.raises(ConnectionRejected) as info:
        async with make_client(""):
            pass
    assert info.value.kind == "InvalidUsername"


@run
async def test_same_name_reconnect_kicks_the_old_session() -> None:
    # 没有证书时同名重连会被服务器当成"换设备登录":踢掉旧连接,新连接正常进入
    name = unique("dup")
    async with make_client(name) as old:
        await next_event(old, events.Connected)
        async with make_client(name) as new:
            assert new.state.myself is not None and new.state.myself.name == name
            left = await next_event(old, events.UserLeft)
            assert isinstance(left, events.UserLeft)
            assert left.user.session == old.state.session
            reason = await asyncio.wait_for(old.wait_closed(), 5)
            assert reason is not None
            assert not old.connected
            assert [type(e) async for e in old.events()][-1] is events.ConnectionClosed


@run
async def test_other_user_leaving_is_reported() -> None:
    async with make_client(unique("watcher")) as watcher:
        await next_event(watcher, events.Connected)
        leaver = make_client(unique("leaver"))
        await leaver.connect()
        await next_event(watcher, events.UserJoined)
        await leaver.close()
        left = await asyncio.wait_for(next_event(watcher, events.UserLeft), 5)
        assert isinstance(left, events.UserLeft) and left.actor is None


@run
async def test_played_audio_arrives_decodable() -> None:
    from pymumble_rev import opus
    from pymumble_rev.audio import BytesSource

    from .test_opus import rms, tone

    async with (
        make_client(unique("dj"), channels=1) as dj,
        make_client(unique("ear"), receive_audio=True) as ear,
    ):
        await next_event(ear, events.Connected)
        src = tone(400, 1, amp=10_000)
        await dj.play(BytesSource(src))

        dec = opus.Decoder(1)
        received: list[bytes] = []
        async with asyncio.timeout(5):
            async for event in ear.events():
                if isinstance(event, events.AudioReceived):
                    assert event.sender_session == dj.state.session
                    received.append(dec.decode(event.opus_data))
                    if event.is_terminator:
                        break
        assert len(received) == 21  # 20 帧 + 终止帧
        assert rms(b"".join(received[5:15])) > 0.5 * rms(src)
