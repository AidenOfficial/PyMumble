"""The sans-IO Mumble client protocol state machine."""

from __future__ import annotations

import logging
import platform
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field, replace
from enum import Enum
from types import MappingProxyType
from typing import Any, Literal

from google.protobuf.message import DecodeError, Message

from . import events as ev
from .errors import ConnectionClosedError, LocalProtocolError
from .framing import FrameDecoder, encode_frame
from .messages import TCP_CLASSES, TcpType, UdpType, pb, tcp_type_of, udp_pb
from .state import (
    Channel,
    ServerInfo,
    ServerState,
    User,
    apply_channel_state,
    apply_server_config,
    apply_user_state,
)

log = logging.getLogger(__name__)

#: 对服务器声明的协议版本。>= 1.5.0 时服务器才会用 protobuf 格式的语音包。
PROTOCOL_VERSION = (1, 5, 939)

try:
    from importlib.metadata import version as _dist_version

    _RELEASE = f"pymumble-rev {_dist_version('pymumble-rev')}"
except Exception:  # 源码树里直接跑测试、没有安装元数据时
    _RELEASE = "pymumble-rev"


def encode_version(major: int, minor: int, patch: int) -> tuple[int, int]:
    """Return ``(version_v1, version_v2)`` as Mumble expects them."""
    v1 = (major << 16) | (minor << 8) | min(patch, 255)
    v2 = (major << 48) | (minor << 32) | (patch << 16)
    return v1, v2


@dataclass(frozen=True, slots=True, kw_only=True)
class ClientConfig:
    """Everything sent to the server during the handshake."""

    username: str
    password: str | None = None
    #: Channel access tokens.
    tokens: Sequence[str] = ()
    #: ``"bot"`` makes the server hide this client from user counts on public lists.
    client_type: Literal["regular", "bot"] = "regular"
    release: str = _RELEASE
    os: str = field(default_factory=platform.system)
    os_version: str = field(default_factory=platform.release)
    version: tuple[int, int, int] = PROTOCOL_VERSION

    def __post_init__(self) -> None:
        # 常见坑:直接传 "a,b" 字符串会被当成逐字符的 token 列表
        if isinstance(self.tokens, str):
            raise TypeError("tokens must be a sequence of strings, not a single string")


class Phase(Enum):
    IDLE = "idle"
    AUTHENTICATING = "authenticating"
    CONNECTED = "connected"
    CLOSED = "closed"


class Connection:
    """Mumble client protocol without I/O.

    Feed bytes read from the TLS socket to :meth:`receive_data` and write whatever
    :meth:`data_to_send` returns. Commands (:meth:`send_text`, :meth:`move`, ...) only queue
    bytes. Not thread-safe: drive it from one thread or one event loop. :attr:`state` is an
    immutable snapshot and may be read from anywhere.
    """

    def __init__(self, config: ClientConfig) -> None:
        self.config = config
        self.phase = Phase.IDLE
        self._decoder = FrameDecoder()
        self._outbound = bytearray()
        # 处理一批数据时在可变副本上改,批次结束再发布成不可变快照
        self._users: dict[int, User] = {}
        self._channels: dict[int, Channel] = {}
        self._server = ServerInfo()
        self._session: int | None = None
        self._state = ServerState()
        self._dirty = False
        self.close_reason: str | None = None

    # —— I/O 边界 ————————————————————————————————————————————————————————

    @property
    def state(self) -> ServerState:
        return self._state

    def data_to_send(self) -> bytes:
        """Return and clear the bytes queued for the server."""
        data = bytes(self._outbound)
        self._outbound.clear()
        return data

    def receive_data(self, data: bytes) -> list[ev.Event]:
        """Process bytes read from the socket. ``b""`` signals end of stream.

        Raises :class:`ProtocolError` if the stream is corrupt; the caller should then drop
        the connection.
        """
        if self.phase is Phase.CLOSED:
            return []
        if not data:
            self._close("connection closed by peer")
            return [ev.ConnectionClosed()]

        out: list[ev.Event] = []
        for msg_type, payload in self._decoder.feed(data):
            self._dispatch(msg_type, payload, out)
            if self.close_reason is not None:  # 例如收到 Reject:之后的数据没有意义
                break
        self._publish()
        return out

    # —— 命令 ——————————————————————————————————————————————————————————

    def initiate(self) -> None:
        """Queue the Version and Authenticate messages. Call once, right after TLS is up."""
        if self.phase is not Phase.IDLE:
            raise LocalProtocolError(f"initiate() in phase {self.phase.value}")
        cfg = self.config
        v1, v2 = encode_version(*cfg.version)
        self._send(
            pb.Version(
                version_v1=v1,
                version_v2=v2,
                release=cfg.release,
                os=cfg.os,
                os_version=cfg.os_version,
            )
        )
        auth = pb.Authenticate(
            username=cfg.username,
            tokens=list(cfg.tokens),
            opus=True,
            client_type=1 if cfg.client_type == "bot" else 0,
        )
        if cfg.password is not None:
            auth.password = cfg.password
        self._send(auth)
        self.phase = Phase.AUTHENTICATING

    def ping(self, now: float) -> None:
        """Queue a keep-alive. ``now`` comes back in :class:`events.Pong` (any clock, ≥ 0)."""
        self._require(Phase.AUTHENTICATING, Phase.CONNECTED)
        self._send(pb.Ping(timestamp=_encode_timestamp(now)))

    def send_text(
        self,
        message: str,
        *,
        sessions: Iterable[int] = (),
        channel_ids: Iterable[int] = (),
        tree_ids: Iterable[int] = (),
    ) -> None:
        """Send a text (HTML) message to users, channels and/or channel trees."""
        self._require(Phase.CONNECTED)
        msg = pb.TextMessage(
            message=message,
            session=list(sessions),
            channel_id=list(channel_ids),
            tree_id=list(tree_ids),
        )
        if not (msg.session or msg.channel_id or msg.tree_id):
            raise ValueError("send_text() needs at least one recipient")
        self._send(msg)

    def move(self, channel_id: int, session: int | None = None) -> None:
        """Move ``session`` (default: ourselves) to ``channel_id``."""
        self._require(Phase.CONNECTED)
        self._send(pb.UserState(session=self._target(session), channel_id=channel_id))

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
        """Change our own mute/deaf flags, comment, avatar or listened channels.

        Pass ``texture=b""`` to clear the avatar and ``comment=""`` to clear the comment.
        """
        self._require(Phase.CONNECTED)
        msg = pb.UserState(session=self._target(None))
        if self_mute is not None:
            msg.self_mute = self_mute
        if self_deaf is not None:
            msg.self_deaf = self_deaf
        if comment is not None:
            msg.comment = comment
        if texture is not None:
            msg.texture = texture
        msg.listening_channel_add.extend(listen_add)
        msg.listening_channel_remove.extend(listen_remove)
        self._send(msg)

    def request_blob(
        self,
        *,
        comments: Iterable[int] = (),
        textures: Iterable[int] = (),
        descriptions: Iterable[int] = (),
    ) -> None:
        """Ask for full comments/avatars (by user session) or descriptions (by channel ID).

        The answers arrive as ordinary user/channel updates.
        """
        self._require(Phase.CONNECTED)
        self._send(
            pb.RequestBlob(
                session_comment=list(comments),
                session_texture=list(textures),
                channel_description=list(descriptions),
            )
        )

    def send_audio(
        self,
        opus_data: bytes,
        *,
        frame_number: int,
        target: int = 0,
        is_terminator: bool = False,
    ) -> None:
        """Queue one Opus packet, tunnelled over the control channel.

        ``frame_number`` counts 10 ms units since the start of the transmission, so a stream of
        20 ms packets goes 0, 2, 4, ... Set ``is_terminator`` on the last packet.
        """
        self._require(Phase.CONNECTED)
        audio = udp_pb.Audio(
            target=target,
            frame_number=frame_number,
            opus_data=opus_data,
            is_terminator=is_terminator,
        )
        packet = bytes([UdpType.Audio]) + audio.SerializeToString()
        self._outbound += encode_frame(TcpType.UDPTunnel, packet)

    def close(self, reason: str = "closed locally") -> None:
        """Mark the connection closed, e.g. because the transport failed. Idempotent."""
        if self.phase is not Phase.CLOSED:
            self._close(reason)

    # —— 内部 ——————————————————————————————————————————————————————————

    def _require(self, *phases: Phase) -> None:
        if self.phase not in phases:
            if self.phase is Phase.CLOSED:
                raise ConnectionClosedError(f"connection closed ({self.close_reason})")
            allowed = "/".join(p.value for p in phases)
            raise LocalProtocolError(f"not allowed in phase {self.phase.value} (needs {allowed})")

    def _target(self, session: int | None) -> int:
        if session is not None:
            return session
        assert self._session is not None  # CONNECTED 时一定已知
        return self._session

    def _send(self, message: Message) -> None:
        self._outbound += encode_frame(tcp_type_of(message), message.SerializeToString())

    def _close(self, reason: str) -> None:
        self.phase = Phase.CLOSED
        self.close_reason = reason

    def _publish(self) -> None:
        if not self._dirty:
            return
        self._dirty = False
        self._state = ServerState(
            session=self._session,
            users=MappingProxyType(dict(self._users)),
            channels=MappingProxyType(dict(self._channels)),
            server=self._server,
        )

    def _dispatch(self, msg_type: int, payload: bytes, out: list[ev.Event]) -> None:
        try:
            kind = TcpType(msg_type)
        except ValueError:
            log.debug("ignoring control message of unknown type %d", msg_type)
            return

        if kind is TcpType.UDPTunnel:
            self._on_tunnel(payload, out)
            return

        msg = TCP_CLASSES[kind]()
        try:
            msg.ParseFromString(payload)
        except DecodeError:
            # 单条消息坏了不影响分帧,跳过即可;流本身坏了由 FrameDecoder 抛错
            log.warning("could not decode %s message, skipping", kind.name)
            return

        handler = _HANDLERS.get(kind)
        if handler is None:
            log.debug("ignoring %s", kind.name)
            return
        handler(self, msg, out)

    @property
    def _synced(self) -> bool:
        return self.phase is Phase.CONNECTED

    def _on_server_sync(self, msg: pb.ServerSync, out: list[ev.Event]) -> None:
        if self.phase is not Phase.AUTHENTICATING:
            log.warning("unexpected ServerSync in phase %s", self.phase.value)
            return
        self._session = msg.session
        self._dirty = True
        updates: dict[str, object] = {}
        if msg.HasField("max_bandwidth"):
            updates["max_bandwidth"] = msg.max_bandwidth
        if msg.HasField("welcome_text"):
            updates["welcome_text"] = msg.welcome_text
        self._server = replace(self._server, **updates)  # type: ignore[arg-type]
        self._dirty = True
        self.phase = Phase.CONNECTED
        out.append(
            ev.Connected(
                session=msg.session,
                welcome_text=self._server.welcome_text,
                max_bandwidth=self._server.max_bandwidth,
            )
        )

    def _on_reject(self, msg: pb.Reject, out: list[ev.Event]) -> None:
        kind = pb.Reject.RejectType.Name(msg.type) if msg.HasField("type") else "None"
        self._close(f"rejected: {kind}: {msg.reason}")
        out.append(ev.Rejected(kind=kind, reason=msg.reason))

    def _on_user_state(self, msg: pb.UserState, out: list[ev.Event]) -> None:
        if not msg.HasField("session"):
            return
        previous = self._users.get(msg.session)
        user, changed = apply_user_state(previous, msg)
        if previous is None or changed:
            self._users[msg.session] = user
            self._dirty = True
        if not self._synced:
            return
        if previous is None:
            out.append(ev.UserJoined(user=user))
        elif changed:
            actor = msg.actor if msg.HasField("actor") else None
            out.append(ev.UserUpdated(user=user, previous=previous, changed=changed, actor=actor))

    def _on_user_remove(self, msg: pb.UserRemove, out: list[ev.Event]) -> None:
        user = self._users.pop(msg.session, None)
        if user is None:
            return
        self._dirty = True
        if not self._synced:
            return
        out.append(
            ev.UserLeft(
                user=user,
                actor=msg.actor if msg.HasField("actor") else None,
                reason=msg.reason,
                ban=msg.ban,
            )
        )

    def _on_channel_state(self, msg: pb.ChannelState, out: list[ev.Event]) -> None:
        if not msg.HasField("channel_id"):
            return
        previous = self._channels.get(msg.channel_id)
        channel, changed = apply_channel_state(previous, msg)
        if previous is None or changed:
            self._channels[msg.channel_id] = channel
            self._dirty = True
        if not self._synced:
            return
        if previous is None:
            out.append(ev.ChannelCreated(channel=channel))
        elif changed:
            out.append(ev.ChannelUpdated(channel=channel, previous=previous, changed=changed))

    def _on_channel_remove(self, msg: pb.ChannelRemove, out: list[ev.Event]) -> None:
        channel = self._channels.pop(msg.channel_id, None)
        if channel is None:
            return
        self._dirty = True
        if self._synced:
            out.append(ev.ChannelRemoved(channel=channel))

    def _on_text_message(self, msg: pb.TextMessage, out: list[ev.Event]) -> None:
        out.append(
            ev.TextMessage(
                actor=msg.actor if msg.HasField("actor") else None,
                message=msg.message,
                sessions=tuple(msg.session),
                channel_ids=tuple(msg.channel_id),
                tree_ids=tuple(msg.tree_id),
            )
        )

    def _on_permission_denied(self, msg: pb.PermissionDenied, out: list[ev.Event]) -> None:
        kind = pb.PermissionDenied.DenyType.Name(msg.type) if msg.HasField("type") else "Text"
        out.append(
            ev.PermissionDenied(
                kind=kind,
                reason=msg.reason,
                session=msg.session if msg.HasField("session") else None,
                channel_id=msg.channel_id if msg.HasField("channel_id") else None,
                permission=msg.permission if msg.HasField("permission") else None,
                name=msg.name if msg.HasField("name") else None,
            )
        )

    def _on_server_config(self, msg: pb.ServerConfig, out: list[ev.Event]) -> None:
        self._server = apply_server_config(self._server, msg)
        self._dirty = True
        if self._synced:
            out.append(ev.ServerInfoUpdated(server=self._server))

    def _on_codec_version(self, msg: pb.CodecVersion, out: list[ev.Event]) -> None:
        if msg.opus == self._server.opus:
            return
        self._server = replace(self._server, opus=msg.opus)
        self._dirty = True
        if self._synced:
            out.append(ev.ServerInfoUpdated(server=self._server))

    def _on_ping(self, msg: pb.Ping, out: list[ev.Event]) -> None:
        if msg.HasField("timestamp"):
            out.append(ev.Pong(sent_at=_decode_timestamp(msg.timestamp)))

    def _on_tunnel(self, payload: bytes, out: list[ev.Event]) -> None:
        if not payload:
            return
        if payload[0] != UdpType.Audio:
            # 1.5 之前的旧格式语音包(首字节高 3 位是编码类型)或语音 ping:都不处理
            log.debug("ignoring tunnelled packet with header 0x%02x", payload[0])
            return
        audio = udp_pb.Audio()
        try:
            audio.ParseFromString(payload[1:])
        except DecodeError:
            log.warning("could not decode tunnelled audio packet, skipping")
            return
        out.append(
            ev.AudioReceived(
                sender_session=audio.sender_session,
                frame_number=audio.frame_number,
                opus_data=audio.opus_data,
                context=audio.context,
                is_terminator=audio.is_terminator,
                volume_adjustment=audio.volume_adjustment,
            )
        )


def _encode_timestamp(now: float) -> int:
    if now < 0:
        raise ValueError("ping timestamp must be non-negative")
    return round(now * 1_000_000)


def _decode_timestamp(value: int) -> float:
    return value / 1_000_000


_HANDLERS: dict[TcpType, Callable[[Connection, Any, list[ev.Event]], None]] = {
    TcpType.ServerSync: Connection._on_server_sync,
    TcpType.Reject: Connection._on_reject,
    TcpType.UserState: Connection._on_user_state,
    TcpType.UserRemove: Connection._on_user_remove,
    TcpType.ChannelState: Connection._on_channel_state,
    TcpType.ChannelRemove: Connection._on_channel_remove,
    TcpType.TextMessage: Connection._on_text_message,
    TcpType.PermissionDenied: Connection._on_permission_denied,
    TcpType.ServerConfig: Connection._on_server_config,
    TcpType.CodecVersion: Connection._on_codec_version,
    TcpType.Ping: Connection._on_ping,
}
