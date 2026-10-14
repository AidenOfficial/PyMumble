from __future__ import annotations

import math
from array import array

import pytest

from pymumble_rev import opus


def tone(ms: int, channels: int, freq: float = 440.0, amp: int = 8000) -> bytes:
    n = opus.SAMPLE_RATE * ms // 1000
    samples = array("h")
    for i in range(n):
        v = int(amp * math.sin(2 * math.pi * freq * i / opus.SAMPLE_RATE))
        samples.extend([v] * channels)
    return samples.tobytes()


def rms(pcm: bytes) -> float:
    s = array("h", pcm)
    return math.sqrt(sum(x * x for x in s) / len(s)) if s else 0.0


def test_version_string() -> None:
    assert opus.version().startswith("libopus")


@pytest.mark.parametrize("channels", [1, 2])
def test_round_trip_keeps_the_signal(channels: int) -> None:
    enc, dec = opus.Encoder(channels), opus.Decoder(channels)
    frame = tone(20, channels)
    out = b""
    for _ in range(10):  # 编码器有预读延迟,多喂几帧再看输出
        out = dec.decode(enc.encode(frame))
    assert len(out) == len(frame)
    assert rms(out) > 0.5 * rms(frame)


def test_packet_loss_concealment_returns_a_frame() -> None:
    enc, dec = opus.Encoder(1), opus.Decoder(1)
    dec.decode(enc.encode(tone(20, 1)))
    assert len(dec.decode(None)) == 960 * 2


def test_bitrate_is_clamped() -> None:
    enc = opus.Encoder(1)
    enc.bitrate = 10**9
    assert enc.bitrate == 510_000
    enc.bitrate = 1
    assert enc.bitrate == 6_000


def test_invalid_input() -> None:
    enc = opus.Encoder(2)
    with pytest.raises(ValueError):
        enc.encode(b"\0" * 6)  # 不是整数个立体声样本
    with pytest.raises(opus.OpusError):
        enc.encode(b"\0" * 4 * 100)  # 100 个样本不是合法帧长
    with pytest.raises(ValueError):
        opus.Encoder(3)


def test_closed_encoder_raises() -> None:
    enc = opus.Encoder(1)
    enc.close()
    enc.close()
    with pytest.raises(ValueError):
        enc.encode(tone(20, 1))
