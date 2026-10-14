from __future__ import annotations

import pytest

from pymumble_rev import (
    ClientConfig,
    Connection,
    ConnectionClosedError,
    LocalProtocolError,
    Phase,
    ProtocolError,
    events,
)
from pymumble_rev.connection import encode_version
from pymumble_rev.framing import HEADER, encode_frame
from pymumble_rev.messages import TcpType, UdpType, pb, udp_pb

from .helpers import OWN_SESSION, connected, decode_sent, frame, initial_sync

# —— 握手 ——————————————————————————————————————————————————————————————


def test_initiate_sends_version_then_authenticate() -> None:
    conn = Connection(
        ClientConfig(
            username="dj-bot", password="pw", tokens=["a", "b"], client_type="bot", release="t"
        )
    )
    conn.initiate()
    version, auth = decode_sent(conn.data_to_send())

    assert isinstance(version, pb.Version)
    assert (version.version_v1, version.version_v2) == encode_version(1, 5, 939)
    assert version.release == "t"
    assert isinstance(auth, pb.Authenticate)
    assert auth.username == "dj-bot"
    assert auth.password == "pw"
    assert list(auth.tokens) == ["a", "b"]
    assert auth.opus is True
    assert auth.client_type == 1
    assert conn.phase is Phase.AUTHENTICATING
    assert conn.data_to_send() == b""


def test_password_is_omitted_when_not_set() -> None:
    conn = Connection(ClientConfig(username="u"))
    conn.initiate()
    _, auth = decode_sent(conn.data_to_send())
    assert isinstance(auth, pb.Authenticate)
    assert not auth.HasField("password")


def test_tokens_as_plain_string_is_rejected() -> None:
    with pytest.raises(TypeError):
        ClientConfig(username="u", tokens="a,b")


def test_version_v1_clamps_patch() -> None:
    v1, v2 = encode_version(1, 5, 939)
    assert v1 == (1 << 16) | (5 << 8) | 255
    assert v2 == (1 << 48) | (5 << 32) | (939 << 16)


def test_initial_sync_yields_connected_then_server_config() -> None:
    conn, evs = connected()
    connected_ev, config_ev = evs
    assert connected_ev == events.Connected(
        session=OWN_SESSION, welcome_text="welcome", max_bandwidth=558000
    )
    # 服务器在 ServerSync 之后才发 ServerConfig
    assert isinstance(config_ev, events.ServerInfoUpdated)
    assert conn.phase is Phase.CONNECTED

    state = conn.state
    assert state.session == OWN_SESSION
    assert state.myself is not None and state.myself.name == "dj-bot"
    assert set(state.users) == {5, OWN_SESSION}
    assert state.users[5].user_id == 3
    assert state.users[OWN_SESSION].user_id is None
    assert state.channels[0].parent is None
    assert state.channels[2].links == frozenset({1})
    assert state.server.message_length == 5000
    assert state.server.opus is True


def test_sync_split_byte_by_byte_gives_same_result() -> None:
    conn = Connection(ClientConfig(username="dj-bot"))
    conn.initiate()
    data = initial_sync()
    evs: list[events.Event] = []
    for i in range(len(data)):
        evs += conn.receive_data(data[i : i + 1])
    assert [type(e) for e in evs] == [events.Connected, events.ServerInfoUpdated]
    assert conn.state.users[5].name == "alice"


def test_reject_closes_and_ignores_rest_of_batch() -> None:
    conn = Connection(ClientConfig(username="dj-bot"))
    conn.initiate()
    data = frame(
        pb.Reject(type="UsernameInUse", reason="taken"),
        pb.ServerSync(session=1),
    )
    evs = conn.receive_data(data)
    assert evs == [events.Rejected(kind="UsernameInUse", reason="taken")]
    assert conn.phase is Phase.CLOSED
    assert conn.close_reason is not None and "UsernameInUse" in conn.close_reason
    assert conn.receive_data(frame(pb.ServerSync(session=1))) == []


def test_eof_closes() -> None:
    conn, _ = connected()
    assert conn.receive_data(b"") == [events.ConnectionClosed()]
    assert conn.phase is Phase.CLOSED
    with pytest.raises(LocalProtocolError):
        conn.send_text("hi", channel_ids=[0])


def test_local_close_is_idempotent_and_blocks_commands() -> None:
    conn, _ = connected()
    conn.close("transport failed")
    conn.close("again")
    assert conn.phase is Phase.CLOSED
    assert conn.close_reason == "transport failed"
    with pytest.raises(ConnectionClosedError, match="transport failed"):
        conn.move(1)
    assert conn.receive_data(frame(pb.TextMessage(message="late", channel_id=[0]))) == []


# —— 状态与事件 ——————————————————————————————————————————————————————————


def test_user_lifecycle_events() -> None:
    conn, _ = connected()

    (joined,) = conn.receive_data(frame(pb.UserState(session=20, name="bob", channel_id=0)))
    assert isinstance(joined, events.UserJoined)
    assert joined.user.name == "bob"

    (moved,) = conn.receive_data(frame(pb.UserState(session=20, channel_id=1, actor=20)))
    assert isinstance(moved, events.UserUpdated)
    assert moved.changed == frozenset({"channel_id"})
    assert (moved.previous.channel_id, moved.user.channel_id) == (0, 1)
    assert moved.actor == 20

    # 值没变的更新不产生事件
    assert conn.receive_data(frame(pb.UserState(session=20, channel_id=1))) == []

    (left,) = conn.receive_data(frame(pb.UserRemove(session=20, actor=5, reason="bye", ban=True)))
    assert left == events.UserLeft(user=moved.user, actor=5, reason="bye", ban=True)
    assert 20 not in conn.state.users


def test_snapshots_are_immutable_and_detached() -> None:
    conn, _ = connected()
    before = conn.state
    conn.receive_data(frame(pb.UserState(session=5, channel_id=0)))
    after = conn.state

    assert before.users[5].channel_id == 1
    assert after.users[5].channel_id == 0
    assert before is not after
    with pytest.raises(TypeError):
        before.users[99] = before.users[5]  # type: ignore[index]


def test_state_object_is_reused_when_nothing_changes() -> None:
    conn, _ = connected()
    before = conn.state
    conn.receive_data(frame(pb.Ping(timestamp=1), pb.UserState(session=5, channel_id=1)))
    assert conn.state is before


def test_listening_channels_and_flags() -> None:
    conn, _ = connected()
    conn.receive_data(frame(pb.UserState(session=5, self_mute=True, listening_channel_add=[0, 2])))
    alice = conn.state.users[5]
    assert alice.self_mute is True
    assert alice.listening_channels == frozenset({0, 2})
    conn.receive_data(frame(pb.UserState(session=5, listening_channel_remove=[0, 7])))
    assert conn.state.users[5].listening_channels == frozenset({2})


def test_comment_blob_handling() -> None:
    conn, _ = connected()
    conn.receive_data(frame(pb.UserState(session=5, comment="short")))
    assert conn.state.users[5].comment == "short"
    assert conn.state.users[5].comment_hash is None

    # 大评论只来 hash:内容未知
    conn.receive_data(frame(pb.UserState(session=5, comment_hash=b"h1")))
    assert conn.state.users[5].comment is None
    assert conn.state.users[5].comment_hash == b"h1"

    # 取回 blob 后内容和 hash 一起到
    conn.receive_data(frame(pb.UserState(session=5, comment="long text", comment_hash=b"h1")))
    assert conn.state.users[5].comment == "long text"

    # 同一个 hash 再来一次不清空已知内容
    conn.receive_data(frame(pb.UserState(session=5, comment_hash=b"h1")))
    assert conn.state.users[5].comment == "long text"


def test_texture_hash_does_not_touch_comment() -> None:
    conn, _ = connected()
    conn.receive_data(frame(pb.UserState(session=5, comment="c", texture_hash=b"t")))
    alice = conn.state.users[5]
    assert alice.comment == "c"
    assert alice.texture is None and alice.texture_hash == b"t"


def test_channel_events_and_links() -> None:
    conn, _ = connected()

    (created,) = conn.receive_data(frame(pb.ChannelState(channel_id=3, parent=1, name="Jazz")))
    assert isinstance(created, events.ChannelCreated)

    (linked,) = conn.receive_data(frame(pb.ChannelState(channel_id=3, links_add=[0, 2])))
    assert isinstance(linked, events.ChannelUpdated)
    assert linked.channel.links == frozenset({0, 2})
    assert linked.changed == frozenset({"links"})

    conn.receive_data(frame(pb.ChannelState(channel_id=3, links_remove=[0])))
    assert conn.state.channels[3].links == frozenset({2})

    (removed,) = conn.receive_data(frame(pb.ChannelRemove(channel_id=3)))
    assert isinstance(removed, events.ChannelRemoved)
    assert removed.channel.name == "Jazz"
    assert 3 not in conn.state.channels


def test_tree_helpers() -> None:
    conn, _ = connected()
    conn.receive_data(frame(pb.ChannelState(channel_id=3, parent=1, name="Jazz")))
    state = conn.state
    assert [c.name for c in state.path(3)] == ["Root", "Music", "Jazz"]
    assert [c.name for c in state.children(0)] == ["AFK", "Music"]
    assert [u.name for u in state.users_in(1)] == ["alice"]
    assert state.path(99) == []


def test_path_survives_parent_cycle() -> None:
    conn, _ = connected()
    conn.receive_data(
        frame(
            pb.ChannelState(channel_id=3, parent=4, name="a"),
            pb.ChannelState(channel_id=4, parent=3, name="b"),
        )
    )
    # 3 的父是 4,4 的父又是 3:走到重复处停下,不死循环
    assert [c.name for c in conn.state.path(3)] == ["b", "a"]


def test_text_message_event() -> None:
    conn, _ = connected()
    (msg,) = conn.receive_data(
        frame(pb.TextMessage(actor=5, session=[OWN_SESSION], message="!play x"))
    )
    assert msg == events.TextMessage(
        actor=5, message="!play x", sessions=(OWN_SESSION,), channel_ids=(), tree_ids=()
    )


def test_server_text_message_has_no_actor() -> None:
    conn, _ = connected()
    (msg,) = conn.receive_data(frame(pb.TextMessage(channel_id=[0], message="maintenance")))
    assert isinstance(msg, events.TextMessage)
    assert msg.actor is None


def test_permission_denied_event() -> None:
    conn, _ = connected()
    denied = pb.PermissionDenied(type="TextTooLong", session=OWN_SESSION)
    (ev,) = conn.receive_data(frame(denied))
    assert isinstance(ev, events.PermissionDenied)
    assert ev.kind == "TextTooLong"
    assert ev.session == OWN_SESSION
    assert ev.channel_id is None


def test_server_config_after_sync_emits_update() -> None:
    conn, _ = connected()
    (ev,) = conn.receive_data(frame(pb.ServerConfig(message_length=100)))
    assert isinstance(ev, events.ServerInfoUpdated)
    assert ev.server.message_length == 100
    assert ev.server.image_message_length == 131072


# —— 命令 ——————————————————————————————————————————————————————————————


def test_commands_require_connected_phase() -> None:
    conn = Connection(ClientConfig(username="u"))
    with pytest.raises(LocalProtocolError):
        conn.ping(0.0)
    conn.initiate()
    conn.ping(0.0)  # 认证阶段允许 ping
    with pytest.raises(LocalProtocolError):
        conn.move(1)
    with pytest.raises(LocalProtocolError):
        conn.initiate()


def test_ping_round_trip() -> None:
    conn, _ = connected()
    conn.ping(123.456789)
    (ping,) = decode_sent(conn.data_to_send())
    assert isinstance(ping, pb.Ping)
    (pong,) = conn.receive_data(frame(ping))
    assert pong == events.Pong(sent_at=123.456789)


def test_send_text_and_move() -> None:
    conn, _ = connected()
    conn.send_text("hello", channel_ids=[1])
    conn.move(1)
    conn.move(2, session=5)
    text, me, other = decode_sent(conn.data_to_send())
    assert isinstance(text, pb.TextMessage)
    assert (list(text.channel_id), text.message) == ([1], "hello")
    assert isinstance(me, pb.UserState) and (me.session, me.channel_id) == (OWN_SESSION, 1)
    assert isinstance(other, pb.UserState) and (other.session, other.channel_id) == (5, 2)


def test_send_text_needs_recipient() -> None:
    conn, _ = connected()
    with pytest.raises(ValueError):
        conn.send_text("into the void")


def test_set_self_state_only_sends_given_fields() -> None:
    conn, _ = connected()
    conn.set_self_state(self_mute=False, texture=b"")
    (msg,) = decode_sent(conn.data_to_send())
    assert isinstance(msg, pb.UserState)
    assert msg.session == OWN_SESSION
    assert msg.HasField("self_mute") and msg.self_mute is False
    assert msg.HasField("texture") and msg.texture == b""
    assert not msg.HasField("comment")
    assert not msg.HasField("self_deaf")


def test_request_blob_uses_ids_not_hashes() -> None:
    conn, _ = connected()
    conn.request_blob(comments=[5], descriptions=[2])
    (msg,) = decode_sent(conn.data_to_send())
    assert isinstance(msg, pb.RequestBlob)
    assert list(msg.session_comment) == [5]
    assert list(msg.channel_description) == [2]


# —— 音频 ——————————————————————————————————————————————————————————————


def test_send_audio_is_tunnelled_protobuf() -> None:
    conn, _ = connected()
    conn.send_audio(b"\x01\x02", frame_number=4, is_terminator=True)
    (tunnel,) = decode_sent(conn.data_to_send())
    assert isinstance(tunnel, pb.UDPTunnel)
    assert tunnel.packet[0] == UdpType.Audio
    audio = udp_pb.Audio()
    audio.ParseFromString(tunnel.packet[1:])
    assert (audio.opus_data, audio.frame_number, audio.target) == (b"\x01\x02", 4, 0)
    assert audio.is_terminator is True


def test_receive_tunnelled_audio() -> None:
    conn, _ = connected()
    audio = udp_pb.Audio(context=0, sender_session=5, frame_number=8, opus_data=b"op")
    packet = bytes([UdpType.Audio]) + audio.SerializeToString()
    (ev,) = conn.receive_data(encode_frame(TcpType.UDPTunnel, packet))
    assert ev == events.AudioReceived(
        sender_session=5,
        frame_number=8,
        opus_data=b"op",
        context=0,
        is_terminator=False,
        volume_adjustment=0.0,
    )


def test_legacy_audio_format_is_ignored() -> None:
    conn, _ = connected()
    legacy = bytes([0x80, 0x05, 0x01]) + b"opus"  # 旧格式:首字节 4<<5 = Opus
    assert conn.receive_data(encode_frame(TcpType.UDPTunnel, legacy)) == []


# —— 健壮性 ——————————————————————————————————————————————————————————————


def test_unknown_and_undecodable_messages_are_skipped() -> None:
    conn, _ = connected()
    data = (
        encode_frame(99, b"future message")
        + encode_frame(TcpType.UserState, b"\xff\xff\xff")
        + frame(pb.TextMessage(actor=5, channel_id=[0], message="still here"))
    )
    (ev,) = conn.receive_data(data)
    assert isinstance(ev, events.TextMessage)


def test_corrupt_stream_raises() -> None:
    conn, _ = connected()
    with pytest.raises(ProtocolError):
        conn.receive_data(HEADER.pack(TcpType.UserState, 0xFFFFFFFF))


def test_set_tokens_sends_token_only_authenticate() -> None:
    conn, _ = connected()
    conn.set_tokens(["vip", "music"])
    (msg,) = decode_sent(conn.data_to_send())
    assert isinstance(msg, pb.Authenticate)
    assert list(msg.tokens) == ["vip", "music"]
    assert not msg.HasField("username")
    assert not msg.HasField("password")
    with pytest.raises(TypeError):
        conn.set_tokens([b"bytes"])  # type: ignore[list-item]
