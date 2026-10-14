"""Immutable snapshots of server state, and the reducers that build them from protobuf messages.

Every object here is frozen. :class:`ServerState` is replaced wholesale whenever something
changes, so a reference obtained on one thread stays internally consistent while the
connection keeps updating on another.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from .messages import pb


@dataclass(frozen=True, slots=True, kw_only=True)
class User:
    """A connected user as last reported by the server."""

    session: int
    name: str = ""
    #: Registered user ID, or ``None`` for unregistered users. SuperUser is ``0``.
    user_id: int | None = None
    channel_id: int = 0
    mute: bool = False
    deaf: bool = False
    suppress: bool = False
    self_mute: bool = False
    self_deaf: bool = False
    priority_speaker: bool = False
    recording: bool = False
    #: ``None`` when the server only sent ``comment_hash`` (fetch it with ``request_blob``).
    comment: str | None = None
    comment_hash: bytes | None = None
    texture: bytes | None = None
    texture_hash: bytes | None = None
    #: Hex SHA-1 of the user's certificate, if they have one.
    cert_hash: str | None = None
    listening_channels: frozenset[int] = frozenset()


@dataclass(frozen=True, slots=True, kw_only=True)
class Channel:
    """A channel as last reported by the server."""

    channel_id: int
    #: ``None`` only for the root channel.
    parent: int | None = None
    name: str = ""
    #: ``None`` when the server only sent ``description_hash``.
    description: str | None = None
    description_hash: bytes | None = None
    temporary: bool = False
    position: int = 0
    max_users: int = 0
    is_enter_restricted: bool = False
    can_enter: bool = True
    links: frozenset[int] = frozenset()


@dataclass(frozen=True, slots=True, kw_only=True)
class ServerInfo:
    """Server-wide settings collected from ServerSync, ServerConfig and CodecVersion."""

    welcome_text: str = ""
    #: Bits per second the server allows each client to send, if announced.
    max_bandwidth: int | None = None
    allow_html: bool = True
    #: ``0`` means unlimited.
    message_length: int = 0
    image_message_length: int = 0
    max_users: int | None = None
    recording_allowed: bool = True
    opus: bool = True


@dataclass(frozen=True, slots=True, kw_only=True)
class ServerState:
    """A consistent snapshot of everything the client knows about the server."""

    #: Our own session ID, known once the server has synced.
    session: int | None = None
    users: Mapping[int, User] = field(default_factory=lambda: MappingProxyType({}))
    channels: Mapping[int, Channel] = field(default_factory=lambda: MappingProxyType({}))
    server: ServerInfo = field(default_factory=ServerInfo)

    @property
    def myself(self) -> User | None:
        """Our own :class:`User`, or ``None`` before the server has synced."""
        return None if self.session is None else self.users.get(self.session)

    def users_in(self, channel_id: int) -> list[User]:
        """Users currently in ``channel_id``, ordered by session ID."""
        return sorted(
            (u for u in self.users.values() if u.channel_id == channel_id),
            key=lambda u: u.session,
        )

    def children(self, channel_id: int) -> list[Channel]:
        """Direct sub-channels of ``channel_id``, in display order."""
        return sorted(
            (c for c in self.channels.values() if c.parent == channel_id),
            key=lambda c: (c.position, c.name),
        )

    def path(self, channel_id: int) -> list[Channel]:
        """Channels from the root down to ``channel_id`` (inclusive)."""
        chain: list[Channel] = []
        seen: set[int] = set()
        current: int | None = channel_id
        # seen 防止服务器数据成环时死循环
        while current is not None and current not in seen:
            seen.add(current)
            channel = self.channels.get(current)
            if channel is None:
                break
            chain.append(channel)
            current = channel.parent
        chain.reverse()
        return chain


# —— reducers ————————————————————————————————————————————————————————————

# 直接照搬的标量字段:proto 字段名 → User 属性名
_USER_SCALARS = {
    "name": "name",
    "user_id": "user_id",
    "channel_id": "channel_id",
    "mute": "mute",
    "deaf": "deaf",
    "suppress": "suppress",
    "self_mute": "self_mute",
    "self_deaf": "self_deaf",
    "priority_speaker": "priority_speaker",
    "recording": "recording",
    "hash": "cert_hash",
}

_CHANNEL_SCALARS = {
    "parent": "parent",
    "name": "name",
    "temporary": "temporary",
    "position": "position",
    "max_users": "max_users",
    "is_enter_restricted": "is_enter_restricted",
    "can_enter": "can_enter",
}


def _blob(
    msg: pb.UserState | pb.ChannelState,
    value_field: str,
    hash_field: str,
    current_value: object,
    current_hash: bytes | None,
) -> tuple[object, bytes | None]:
    """Resolve a (value, hash) pair such as comment/comment_hash.

    服务器对大于 128 字节的内容只发 hash;hash 没变就保留已知内容,变了就清空等客户端去取。
    """
    has_value = msg.HasField(value_field)
    has_hash = msg.HasField(hash_field)
    value = getattr(msg, value_field) if has_value else current_value
    digest = getattr(msg, hash_field) if has_hash else current_hash
    if has_hash and not has_value and digest != current_hash:
        value = None
    if has_value and not has_hash:
        # 内容直接下发说明它小到不需要 hash
        digest = None
    return value, digest


def apply_user_state(current: User | None, msg: pb.UserState) -> tuple[User, frozenset[str]]:
    """Return the updated user and the names of the attributes that changed."""
    user = current or User(session=msg.session)
    updates: dict[str, object] = {}
    for proto_name, attr in _USER_SCALARS.items():
        if msg.HasField(proto_name):
            updates[attr] = getattr(msg, proto_name)

    comment, comment_hash = _blob(msg, "comment", "comment_hash", user.comment, user.comment_hash)
    updates["comment"], updates["comment_hash"] = comment, comment_hash
    texture, texture_hash = _blob(msg, "texture", "texture_hash", user.texture, user.texture_hash)
    updates["texture"], updates["texture_hash"] = texture, texture_hash

    if msg.listening_channel_add or msg.listening_channel_remove:
        updates["listening_channels"] = (
            user.listening_channels | frozenset(msg.listening_channel_add)
        ) - frozenset(msg.listening_channel_remove)

    changed = frozenset(k for k, v in updates.items() if getattr(user, k) != v)
    if current is not None and not changed:
        return current, changed
    return dataclasses.replace(user, **updates), changed  # type: ignore[arg-type]


def apply_channel_state(
    current: Channel | None, msg: pb.ChannelState
) -> tuple[Channel, frozenset[str]]:
    """Return the updated channel and the names of the attributes that changed."""
    channel = current or Channel(channel_id=msg.channel_id)
    updates: dict[str, object] = {}
    for proto_name, attr in _CHANNEL_SCALARS.items():
        if msg.HasField(proto_name):
            updates[attr] = getattr(msg, proto_name)

    description, description_hash = _blob(
        msg, "description", "description_hash", channel.description, channel.description_hash
    )
    updates["description"], updates["description_hash"] = description, description_hash

    links = channel.links
    if msg.links:
        # 完整列表只在初始同步或整体替换时出现
        links = frozenset(msg.links)
    links = (links | frozenset(msg.links_add)) - frozenset(msg.links_remove)
    updates["links"] = links

    changed = frozenset(k for k, v in updates.items() if getattr(channel, k) != v)
    if current is not None and not changed:
        return current, changed
    return dataclasses.replace(channel, **updates), changed  # type: ignore[arg-type]


def apply_server_config(info: ServerInfo, msg: pb.ServerConfig) -> ServerInfo:
    updates: dict[str, object] = {}
    for name in (
        "max_bandwidth",
        "welcome_text",
        "allow_html",
        "message_length",
        "image_message_length",
        "max_users",
        "recording_allowed",
    ):
        if msg.HasField(name):
            updates[name] = getattr(msg, name)
    return dataclasses.replace(info, **updates)  # type: ignore[arg-type]
