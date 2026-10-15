"""Asyncio Mumble client library — sans-IO protocol core and pull-based Opus audio."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _dist_version

from . import audio, events, opus
from .client import MumbleClient, default_ssl_context
from .connection import (
    PROTOCOL_VERSION,
    VOICE_TARGET_LOOPBACK,
    ClientConfig,
    Connection,
    Phase,
    VoiceTarget,
)
from .errors import (
    ConnectionClosedError,
    ConnectionRejected,
    LocalProtocolError,
    MumbleError,
    ProtocolError,
)
from .state import Channel, ServerInfo, ServerState, User
from .sync import ThreadedClient

try:
    __version__ = _dist_version("pymumble-rev")
except PackageNotFoundError:  # 源码树里直接 import,没有安装元数据
    __version__ = "0+unknown"

__all__ = [
    "PROTOCOL_VERSION",
    "VOICE_TARGET_LOOPBACK",
    "Channel",
    "ClientConfig",
    "Connection",
    "ConnectionClosedError",
    "ConnectionRejected",
    "LocalProtocolError",
    "MumbleClient",
    "MumbleError",
    "Phase",
    "ProtocolError",
    "ServerInfo",
    "ServerState",
    "ThreadedClient",
    "User",
    "VoiceTarget",
    "__version__",
    "audio",
    "default_ssl_context",
    "events",
    "opus",
]
