from __future__ import annotations

import pytest

from pymumble_rev import ProtocolError
from pymumble_rev.framing import HEADER, MAX_PAYLOAD, FrameDecoder, encode_frame


def test_round_trip_several_frames_in_one_chunk() -> None:
    data = encode_frame(3, b"ping") + encode_frame(11, b"") + encode_frame(9, b"x" * 1000)
    assert FrameDecoder().feed(data) == [(3, b"ping"), (11, b""), (9, b"x" * 1000)]


def test_byte_by_byte_feeding_yields_each_frame_once() -> None:
    data = encode_frame(7, b"hello") + encode_frame(8, b"world")
    decoder = FrameDecoder()
    frames: list[tuple[int, bytes]] = []
    for i in range(len(data)):
        frames += decoder.feed(data[i : i + 1])
    assert frames == [(7, b"hello"), (8, b"world")]
    assert decoder.pending == 0


def test_partial_frame_stays_buffered() -> None:
    data = encode_frame(1, b"abcdef")
    decoder = FrameDecoder()
    assert decoder.feed(data[:-2]) == []
    assert decoder.pending == len(data) - 2
    assert decoder.feed(data[-2:]) == [(1, b"abcdef")]


def test_oversized_length_is_a_protocol_error() -> None:
    with pytest.raises(ProtocolError):
        FrameDecoder().feed(HEADER.pack(9, MAX_PAYLOAD + 1))


def test_encode_rejects_oversized_payload() -> None:
    with pytest.raises(ValueError):
        encode_frame(1, b"\0" * (MAX_PAYLOAD + 1))
