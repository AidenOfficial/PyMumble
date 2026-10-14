"""Mapping between Mumble message type numbers and protobuf classes."""

from __future__ import annotations

from enum import IntEnum

from google.protobuf.message import Message

from ._proto import Mumble_pb2 as pb
from ._proto import MumbleUDP_pb2 as udp_pb


class TcpType(IntEnum):
    """Control channel message types. Names match the protobuf message names."""

    Version = 0
    UDPTunnel = 1
    Authenticate = 2
    Ping = 3
    Reject = 4
    ServerSync = 5
    ChannelRemove = 6
    ChannelState = 7
    UserRemove = 8
    UserState = 9
    BanList = 10
    TextMessage = 11
    PermissionDenied = 12
    ACL = 13
    QueryUsers = 14
    CryptSetup = 15
    ContextActionModify = 16
    ContextAction = 17
    UserList = 18
    VoiceTarget = 19
    PermissionQuery = 20
    CodecVersion = 21
    UserStats = 22
    RequestBlob = 23
    ServerConfig = 24
    SuggestConfig = 25
    PluginDataTransmission = 26


class UdpType(IntEnum):
    """Header byte of a protobuf-format voice packet (Mumble 1.5+)."""

    Audio = 0
    Ping = 1


# UDPTunnel 的负载是原始语音包,不是 protobuf,所以不在表里
TCP_CLASSES: dict[TcpType, type[Message]] = {
    t: getattr(pb, t.name) for t in TcpType if t is not TcpType.UDPTunnel
}
_TYPE_OF_CLASS: dict[type[Message], TcpType] = {cls: t for t, cls in TCP_CLASSES.items()}


def tcp_type_of(message: Message) -> TcpType:
    """Return the control message type number for a protobuf message instance."""
    try:
        return _TYPE_OF_CLASS[type(message)]
    except KeyError:
        raise TypeError(f"{type(message).__name__} is not a Mumble control message") from None


__all__ = ["TCP_CLASSES", "TcpType", "UdpType", "pb", "tcp_type_of", "udp_pb"]
