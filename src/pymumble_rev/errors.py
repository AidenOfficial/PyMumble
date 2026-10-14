"""Exceptions raised by pymumble-rev."""

from __future__ import annotations


class MumbleError(Exception):
    """Base class for every exception raised by this library."""


class ProtocolError(MumbleError):
    """The server sent data that cannot be parsed; the connection must be dropped."""


class LocalProtocolError(MumbleError):
    """The caller asked for something the protocol does not allow in the current phase."""


class ConnectionClosedError(LocalProtocolError):
    """A command was issued after the connection closed."""


class ConnectionRejected(MumbleError):
    """The server refused the connection (wrong password, name in use, server full, ...)."""

    def __init__(self, kind: str, reason: str) -> None:
        super().__init__(f"{kind}: {reason}" if reason else kind)
        #: Name of ``Reject.RejectType``.
        self.kind = kind
        self.reason = reason
