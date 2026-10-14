"""Helpers that play the server side of the protocol in tests."""

from __future__ import annotations

from google.protobuf.message import Message

from pymumble_rev import ClientConfig, Connection
from pymumble_rev.events import Event
from pymumble_rev.framing import FrameDecoder, encode_frame
from pymumble_rev.messages import TCP_CLASSES, TcpType, pb, tcp_type_of

OWN_SESSION = 11


def frame(*messages: Message) -> bytes:
    """Encode server→client control messages as one byte string."""
    return b"".join(encode_frame(tcp_type_of(m), m.SerializeToString()) for m in messages)


def decode_sent(data: bytes) -> list[Message]:
    """Decode client→server bytes back into protobuf messages (UDPTunnel stays raw bytes)."""
    out: list[Message] = []
    for msg_type, payload in FrameDecoder().feed(data):
        kind = TcpType(msg_type)
        if kind is TcpType.UDPTunnel:
            raw = pb.UDPTunnel(packet=payload)
            out.append(raw)
            continue
        msg = TCP_CLASSES[kind]()
        msg.ParseFromString(payload)
        out.append(msg)
    return out


def initial_sync() -> bytes:
    """What a Mumble 1.5 server sends between Authenticate and ServerSync, plus ServerConfig."""
    return frame(
        pb.Version(version_v2=(1 << 48) | (5 << 32) | (939 << 16), release="1.5.939"),
        pb.CryptSetup(key=b"k" * 16, client_nonce=b"c" * 16, server_nonce=b"s" * 16),
        pb.CodecVersion(alpha=-2147483637, beta=0, prefer_alpha=True, opus=True),
        pb.ChannelState(channel_id=0, name="Root", position=0),
        pb.ChannelState(channel_id=1, parent=0, name="Music", position=1),
        pb.ChannelState(channel_id=2, parent=0, name="AFK", position=0, links=[1]),
        pb.PermissionQuery(channel_id=0, permissions=134744846),
        pb.UserState(session=5, name="alice", channel_id=1, user_id=3),
        pb.UserState(session=OWN_SESSION, name="dj-bot", channel_id=0),
        pb.ServerSync(
            session=OWN_SESSION,
            max_bandwidth=558000,
            welcome_text="welcome",
            permissions=134744846,
        ),
        pb.ServerConfig(
            message_length=5000,
            image_message_length=131072,
            allow_html=True,
            max_users=100,
            recording_allowed=True,
        ),
    )


def connected(**config: object) -> tuple[Connection, list[Event]]:
    """Return a connection that has completed the handshake, and the sync events."""
    conn = Connection(ClientConfig(username="dj-bot", **config))  # type: ignore[arg-type]
    conn.initiate()
    conn.data_to_send()
    events = conn.receive_data(initial_sync())
    return conn, events
