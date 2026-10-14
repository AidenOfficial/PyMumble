"""Events returned by :meth:`Connection.receive_data`.

Per-entity events (users, channels) are only emitted after :class:`Connected`. Everything the
server sends during the initial sync is folded into ``Connection.state`` instead, so a
consumer sees one ``Connected`` with a fully populated snapshot rather than a burst of
"joined" events for users who were already there.
"""

from __future__ import annotations

from dataclasses import dataclass

from .state import Channel, ServerInfo, User


@dataclass(frozen=True, slots=True, kw_only=True)
class Event:
    """Base class for all events."""


@dataclass(frozen=True, slots=True, kw_only=True)
class Connected(Event):
    """The server accepted us and finished sending users and channels.

    Mumble servers send ServerConfig (message length limits, HTML policy) right *after* this,
    so expect a :class:`ServerInfoUpdated` to follow immediately.
    """

    session: int
    welcome_text: str
    max_bandwidth: int | None


@dataclass(frozen=True, slots=True, kw_only=True)
class Rejected(Event):
    """The server refused the connection. It will close the socket next."""

    #: Name of ``Reject.RejectType`` (``"WrongUserPW"``, ``"UsernameInUse"``, ...).
    kind: str
    reason: str


@dataclass(frozen=True, slots=True, kw_only=True)
class ConnectionClosed(Event):
    """The peer closed the stream (``receive_data(b"")``)."""


@dataclass(frozen=True, slots=True, kw_only=True)
class UserJoined(Event):
    user: User


@dataclass(frozen=True, slots=True, kw_only=True)
class UserUpdated(Event):
    user: User
    previous: User
    #: Attribute names of :class:`User` whose value changed.
    changed: frozenset[str]
    #: Session of whoever made the change, if the server said so.
    actor: int | None


@dataclass(frozen=True, slots=True, kw_only=True)
class UserLeft(Event):
    user: User
    #: Session of the kicker/banner; ``None`` when the user simply disconnected.
    actor: int | None
    reason: str
    ban: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class ChannelCreated(Event):
    channel: Channel


@dataclass(frozen=True, slots=True, kw_only=True)
class ChannelUpdated(Event):
    channel: Channel
    previous: Channel
    changed: frozenset[str]


@dataclass(frozen=True, slots=True, kw_only=True)
class ChannelRemoved(Event):
    channel: Channel


@dataclass(frozen=True, slots=True, kw_only=True)
class TextMessage(Event):
    #: Sender session; ``None`` for messages from the server itself.
    actor: int | None
    message: str
    #: Recipients as the server reported them. A non-empty ``sessions`` means a private message.
    sessions: tuple[int, ...]
    channel_ids: tuple[int, ...]
    tree_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class PermissionDenied(Event):
    #: Name of ``PermissionDenied.DenyType`` (``"Permission"``, ``"TextTooLong"``, ...).
    kind: str
    reason: str
    session: int | None
    channel_id: int | None
    permission: int | None
    name: str | None


@dataclass(frozen=True, slots=True, kw_only=True)
class ServerInfoUpdated(Event):
    """ServerConfig or CodecVersion arrived after the initial sync."""

    server: ServerInfo


@dataclass(frozen=True, slots=True, kw_only=True)
class Pong(Event):
    """The server echoed one of our pings."""

    #: The ``now`` value that was passed to :meth:`Connection.ping`.
    sent_at: float


@dataclass(frozen=True, slots=True, kw_only=True)
class AudioReceived(Event):
    """One Opus packet from another user, tunnelled over TCP."""

    sender_session: int
    frame_number: int
    opus_data: bytes
    #: 0 = normal talking, 1 = shout/whisper to a channel, 2 = direct whisper, ...
    context: int
    is_terminator: bool
    volume_adjustment: float
