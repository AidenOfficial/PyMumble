from __future__ import annotations

import asyncio
import threading
import time
from array import array

import pytest

from pymumble_rev import opus
from pymumble_rev.audio import (
    AudioSender,
    BufferClosed,
    BytesSource,
    PcmBuffer,
    apply_gain,
    frame_bytes,
)
from pymumble_rev.errors import ConnectionClosedError

from .test_client import run
from .test_opus import rms, tone

FRAME = frame_bytes(20, 1)

# —— PcmBuffer ——————————————————————————————————————————————————————————


def test_buffer_reads_whole_frames_and_signals_underrun() -> None:
    buf = PcmBuffer(1)
    buf.write(b"\x01" * (FRAME + 10))
    assert buf.read(FRAME) == b"\x01" * FRAME
    assert buf.read(FRAME) == b""  # 只剩 10 字节,还没关闭:算欠载
    buf.close()
    assert buf.read(FRAME) == b"\x01" * 10  # 关闭后吐出尾巴
    assert buf.read(FRAME) is None


def test_buffer_write_blocks_until_there_is_room() -> None:
    buf = PcmBuffer(1, capacity=0.02)  # 恰好一帧
    buf.write(b"\0" * FRAME)
    done = threading.Event()

    def producer() -> None:
        buf.write(b"\1" * FRAME)
        done.set()

    t = threading.Thread(target=producer)
    t.start()
    assert not done.wait(0.1)
    assert buf.read(FRAME) == b"\0" * FRAME
    assert done.wait(1)
    t.join()
    assert buf.read(FRAME) == b"\1" * FRAME


def test_buffer_write_timeout_and_close() -> None:
    buf = PcmBuffer(1, capacity=0.02)
    assert buf.write(b"\0" * FRAME * 2, timeout=0.05) is False
    assert buf.buffered == pytest.approx(0.02)
    buf.close()
    with pytest.raises(BufferClosed):
        buf.write(b"\0")


def test_buffer_clear_wakes_writers() -> None:
    buf = PcmBuffer(1, capacity=0.02)
    buf.write(b"\0" * FRAME)
    done = threading.Event()

    def producer() -> None:
        buf.write(b"\1" * FRAME)
        done.set()

    t = threading.Thread(target=producer)
    t.start()
    assert not done.wait(0.05)
    buf.clear()
    assert done.wait(1)
    t.join()
    assert buf.read(FRAME) == b"\1" * FRAME


def test_close_wakes_blocked_writer_with_error() -> None:
    buf = PcmBuffer(1, capacity=0.02)
    buf.write(b"\0" * FRAME)
    errors: list[BaseException] = []

    def producer() -> None:
        try:
            buf.write(b"\1" * FRAME)
        except BufferClosed as exc:
            errors.append(exc)

    t = threading.Thread(target=producer)
    t.start()
    time.sleep(0.05)
    buf.close()
    t.join(1)
    assert len(errors) == 1


# —— 音量 ——————————————————————————————————————————————————————————————


def samples(pcm: bytes) -> list[int]:
    return list(array("h", pcm))


def test_gain_identity_is_untouched() -> None:
    pcm = array("h", [1000, -1000]).tobytes()
    assert apply_gain(pcm, 1.0, 1.0, 1) is pcm


def test_gain_scales_ramps_and_clips() -> None:
    assert samples(apply_gain(array("h", [1000, -1000]).tobytes(), 0.5, 0.5, 1)) == [500, -500]
    ramp = samples(apply_gain(array("h", [1000] * 4).tobytes(), 0.0, 1.0, 1))
    assert ramp == [0, 250, 500, 750]
    stereo = samples(apply_gain(array("h", [1000, 1000, 1000, 1000]).tobytes(), 0.0, 1.0, 2))
    assert stereo == [0, 0, 500, 500]  # 同一帧的左右声道用同一个增益
    assert samples(apply_gain(array("h", [30000, -30000]).tobytes(), 2.0, 2.0, 1)) == [
        32767,
        -32768,
    ]


# —— AudioSender ——————————————————————————————————————————————————————————


class Recorder:
    def __init__(self, delay: float = 0.0) -> None:
        self.packets: list[tuple[bytes, int, bool, float]] = []
        self.delay = delay
        self.stall_once: float = 0.0

    async def __call__(self, packet: bytes, frame_number: int, terminator: bool) -> None:
        if self.stall_once:
            stall, self.stall_once = self.stall_once, 0.0
            await asyncio.sleep(stall)
        self.packets.append((packet, frame_number, terminator, time.monotonic()))

    @property
    def numbers(self) -> list[int]:
        return [n for _, n, _, _ in self.packets]

    @property
    def terminators(self) -> list[bool]:
        return [t for _, _, t, _ in self.packets]


async def start(sender: AudioSender) -> asyncio.Task[None]:
    task = asyncio.create_task(sender.run())
    await asyncio.sleep(0)
    return task


async def stop(task: asyncio.Task[None]) -> None:
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@run
async def test_source_is_sent_in_real_time_and_terminated() -> None:
    rec = Recorder()
    sender = AudioSender(rec)
    task = await start(sender)
    started = time.monotonic()
    await sender.play(BytesSource(tone(200, 1)))  # 10 帧
    elapsed = time.monotonic() - started
    await asyncio.sleep(0.05)
    await stop(task)

    assert rec.numbers == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
    assert rec.terminators == [False] * 10 + [True]
    assert 0.17 < elapsed < 0.4  # 实时节奏,不是一口气发完
    assert sender.stats["frames"] == 11


@run
async def test_short_underrun_is_bridged_with_silence() -> None:
    rec = Recorder()
    sender = AudioSender(rec, underrun_grace=0.2)
    buf = PcmBuffer(1, capacity=2)
    task = await start(sender)
    buf.write(tone(60, 1))
    sender.play(buf)
    await asyncio.sleep(0.15)  # 数据 60ms 后就断了,欠载约 90ms < 200ms
    buf.write(tone(40, 1))
    buf.close()
    await asyncio.sleep(0.15)
    await stop(task)

    assert True not in rec.terminators[:-1]  # 中途没有结束发言
    assert rec.terminators[-1] is True
    assert rec.numbers == list(range(0, 2 * len(rec.numbers), 2))  # 编号连续
    assert sender.stats["underrun_frames"] > 0


@run
async def test_long_underrun_ends_the_transmission_and_restarts_numbering() -> None:
    rec = Recorder()
    sender = AudioSender(rec, underrun_grace=0.04)
    buf = PcmBuffer(1, capacity=2)
    task = await start(sender)
    buf.write(tone(40, 1))
    sender.play(buf)
    await asyncio.sleep(0.2)
    buf.write(tone(40, 1))
    buf.close()
    await asyncio.sleep(0.15)
    await stop(task)

    first_end = rec.terminators.index(True)
    assert rec.numbers[: first_end + 1] == [0, 2, 4, 6, 8]  # 2 帧数据 + 2 帧静音 + 终止
    assert rec.numbers[first_end + 1] == 0  # 新的一次发言从 0 开始
    assert rec.terminators[-1] is True


@run
async def test_play_replaces_and_stop_terminates() -> None:
    rec = Recorder()
    sender = AudioSender(rec)
    task = await start(sender)
    first = sender.play(PcmBuffer(1))  # 永远欠载的源
    second = sender.play(BytesSource(tone(1000, 1)))
    assert first.done()
    await asyncio.sleep(0.1)
    sender.stop()
    await asyncio.sleep(0.05)
    assert second.done()
    assert rec.terminators[-1] is True
    count = len(rec.packets)
    await asyncio.sleep(0.1)
    assert len(rec.packets) == count  # 停了就不再发
    await stop(task)


@run
async def test_stall_resyncs_instead_of_bursting() -> None:
    rec = Recorder()
    sender = AudioSender(rec, max_lag=3)
    task = await start(sender)
    sender.play(BytesSource(tone(400, 1)))
    await asyncio.sleep(0.06)
    rec.stall_once = 0.3  # 一次 300ms 的发送阻塞
    await asyncio.sleep(0.6)
    await stop(task)

    assert sender.stats["resyncs"] >= 1
    times = [t for _, _, _, t in rec.packets]
    # 阻塞之后不会在一瞬间补发十几帧
    burst = max(sum(1 for t in times if t0 <= t < t0 + 0.01) for t0 in times)
    assert burst <= 2


@run
async def test_bandwidth_cap() -> None:
    sender = AudioSender(Recorder(), bitrate=96_000)
    sender.limit_bandwidth(558_000)
    assert sender.bitrate == 96_000
    sender.limit_bandwidth(40_000)
    assert sender.bitrate == 40_000 - 57 * 8 * 50
    sender.limit_bandwidth(None)
    assert sender.bitrate == 96_000


@run
async def test_volume_reaches_the_encoded_audio() -> None:
    async def loudness(volume: float) -> float:
        rec = Recorder()
        sender = AudioSender(rec)
        sender.volume = volume
        sender._applied_volume = volume  # 跳过首帧的渐变,直接比较稳态
        task = await start(sender)
        await sender.play(BytesSource(tone(200, 1)))
        await stop(task)
        dec = opus.Decoder(1)
        return max(rms(dec.decode(p)) for p, _, _, _ in rec.packets)

    loud, quiet = await loudness(1.0), await loudness(0.1)
    assert quiet < loud * 0.2


@run
async def test_pending_future_fails_when_sender_stops() -> None:
    sender = AudioSender(Recorder())
    task = await start(sender)
    fut = sender.play(PcmBuffer(1))
    await stop(task)
    with pytest.raises(ConnectionClosedError):
        await fut
