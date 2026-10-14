"""Mumble TCP framing: 6-byte header (type u16, length u32, big-endian), then the payload."""

from __future__ import annotations

import struct

from .errors import ProtocolError

HEADER = struct.Struct("!HI")

#: 单帧负载上限。官方服务器对图片消息、头像等也远小于此;超过就当作流已损坏。
MAX_PAYLOAD = 8 * 1024 * 1024


def encode_frame(msg_type: int, payload: bytes) -> bytes:
    """Prefix ``payload`` with the Mumble TCP header."""
    if len(payload) > MAX_PAYLOAD:
        raise ValueError(f"payload of {len(payload)} bytes exceeds {MAX_PAYLOAD}")
    return HEADER.pack(msg_type, len(payload)) + payload


class FrameDecoder:
    """Incrementally split a byte stream into ``(type, payload)`` frames."""

    def __init__(self) -> None:
        self._buffer = bytearray()

    @property
    def pending(self) -> int:
        """Bytes buffered that do not yet form a complete frame."""
        return len(self._buffer)

    def feed(self, data: bytes) -> list[tuple[int, bytes]]:
        """Append ``data`` and return every frame that is now complete."""
        self._buffer += data
        frames: list[tuple[int, bytes]] = []
        offset = 0
        size = HEADER.size
        # 用偏移量扫描,最后一次性切掉已消费部分,避免每帧都搬动缓冲区
        while len(self._buffer) - offset >= size:
            msg_type, length = HEADER.unpack_from(self._buffer, offset)
            if length > MAX_PAYLOAD:
                raise ProtocolError(f"frame of type {msg_type} announces {length} bytes")
            end = offset + size + length
            if end > len(self._buffer):
                break
            frames.append((msg_type, bytes(self._buffer[offset + size : end])))
            offset = end
        if offset:
            del self._buffer[:offset]
        return frames
