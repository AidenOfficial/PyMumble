"""Exceptions raised by pymumble-rev."""

from __future__ import annotations


class MumbleError(Exception):
    """Base class for every exception raised by this library."""


class ProtocolError(MumbleError):
    """The server sent data that cannot be parsed; the connection must be dropped."""


class LocalProtocolError(MumbleError):
    """The caller asked for something the protocol does not allow in the current phase."""
