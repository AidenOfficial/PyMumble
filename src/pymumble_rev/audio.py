"""Pull-based audio: sources, a thread-safe PCM buffer, and the paced Opus sender.

PCM everywhere in this module is signed 16-bit little-endian, 48 kHz, interleaved when stereo.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import threading
from array import array
from collections.abc import Awaitable, Callable
from typing import Protocol

from .errors import ConnectionClosedError
from .opus import SAMPLE_RATE, Application, Encoder

log = logging.getLogger(__name__)

#: Opus 允许、Mumble 也接受的帧长
FRAME_MS_CHOICES = (10, 20, 40, 60)


def frame_bytes(frame_ms: int, channels: int) -> int:
    return SAMPLE_RATE * frame_ms // 1000 * 2 * channels


class AudioSource(Protocol):
    """Something the sender can pull PCM from, one frame at a time.

    ``read`` is called on the event loop every frame and must not block. Return:

    - ``size`` bytes: the next frame;
    - fewer bytes: the final, partial frame (padded with silence);
    - ``b""``: nothing available right now (an underrun; the sender will ask again next frame);
    - ``None``: the source is finished.
    """

    def read(self, size: int) -> bytes | None: ...


class BufferClosed(Exception):
    """Raised by :meth:`PcmBuffer.write` after :meth:`PcmBuffer.close`."""


class PcmBuffer:
    """Thread-safe PCM queue: producers ``write`` from any thread, the sender pulls frames.

    ``write`` blocks while more than ``capacity`` seconds are queued, so a producer thread
    that reads from ffmpeg is paced by playback without polling. :meth:`clear` drops queued
    audio immediately (skip), :meth:`close` ends the stream after the queued audio plays.
    """

    def __init__(self, channels: int = 1, capacity: float = 0.5) -> None:
        if channels not in (1, 2):
            raise ValueError("channels must be 1 or 2")
        self.channels = channels
        self._capacity = max(1, int(capacity * SAMPLE_RATE)) * 2 * channels
        self._data = bytearray()
        self._closed = False
        self._cond = threading.Condition()

    @property
    def buffered(self) -> float:
        """Seconds of audio waiting to be sent."""
        with self._cond:
            return len(self._data) / (2 * self.channels * SAMPLE_RATE)

    @property
    def closed(self) -> bool:
        return self._closed

    def write(self, pcm: bytes, timeout: float | None = None) -> bool:
        """Queue ``pcm``, blocking while the buffer is full.

        Returns ``False`` if ``timeout`` expired before everything was queued (whatever fit is
        queued). Raises :class:`BufferClosed` if the buffer is or becomes closed.
        """
        view = memoryview(pcm)
        with self._cond:
            while view:
                if self._closed:
                    raise BufferClosed("write() after close()")
                room = self._capacity - len(self._data)
                if room <= 0:
                    if not self._cond.wait(timeout):
                        return False
                    continue
                self._data += view[:room]
                view = view[room:]
                self._cond.notify_all()
        return True

    def read(self, size: int) -> bytes | None:
        with self._cond:
            if len(self._data) >= size:
                chunk = bytes(self._data[:size])
            elif self._closed:
                if not self._data:
                    return None
                chunk = bytes(self._data)
            else:
                return b""
            del self._data[: len(chunk)]
            self._cond.notify_all()
            return chunk

    def clear(self) -> None:
        """Drop everything queued. Blocked writers wake up and continue."""
        with self._cond:
            self._data.clear()
            self._cond.notify_all()

    def close(self) -> None:
        """No more writes; the sender plays what is queued and then ends the stream."""
        with self._cond:
            self._closed = True
            self._cond.notify_all()


class BytesSource:
    """A source over PCM already in memory (tests, sound effects)."""

    def __init__(self, pcm: bytes) -> None:
        self._pcm = memoryview(pcm)

    def read(self, size: int) -> bytes | None:
        if not self._pcm:
            return None
        chunk, self._pcm = self._pcm[:size], self._pcm[size:]
        return bytes(chunk)


def apply_gain(pcm: bytes, start: float, end: float, channels: int) -> bytes:
    """Scale ``pcm`` by a gain ramping linearly from ``start`` to ``end`` (avoids clicks)."""
    if start == end == 1.0:
        return pcm
    samples = array("h", pcm)
    if sys.byteorder == "big":
        samples.byteswap()
    frames = len(samples) // channels
    step = (end - start) / frames if frames else 0.0
    out = array(
        "h",
        [
            max(-32768, min(32767, int(s * (start + step * (i // channels)))))
            for i, s in enumerate(samples)
        ],
    )
    if sys.byteorder == "big":
        out.byteswap()
    return out.tobytes()


#: ``(packet, frame_number, is_terminator, target)`` — what :class:`AudioSender` calls to send.
SendFn = Callable[[bytes, int, bool, int], Awaitable[None]]


class AudioSender:
    """Pulls one frame per ``frame_ms`` from the current source, encodes it, and sends it.

    Pacing uses absolute deadlines, so it does not drift. If sending stalls (a congested
    socket) the sender falls behind; past ``max_lag`` frames it resynchronises to "now"
    instead of bursting the backlog, so the listener hears a delay, not a fast-forward.
    """

    def __init__(
        self,
        send: SendFn,
        *,
        channels: int = 1,
        frame_ms: int = 20,
        bitrate: int = 96_000,
        application: Application = Application.AUDIO,
        underrun_grace: float = 0.2,
        max_lag: int = 5,
    ) -> None:
        if frame_ms not in FRAME_MS_CHOICES:
            raise ValueError(f"frame_ms must be one of {FRAME_MS_CHOICES}")
        self._send = send
        self.channels = channels
        self.frame_ms = frame_ms
        self.frame_size = frame_bytes(frame_ms, channels)
        self._encoder = Encoder(channels, application, bitrate=bitrate)
        self._wanted_bitrate = bitrate
        self._bitrate_cap: int | None = None
        self._grace_frames = max(0, round(underrun_grace * 1000 / frame_ms))
        self._max_lag = max_lag
        self._silence = bytes(self.frame_size)

        self._source: AudioSource | None = None
        self._done: asyncio.Future[None] | None = None
        self._wake = asyncio.Event()
        self._volume = 1.0
        self._applied_volume = 1.0
        # 想要的目标,以及当前这次发言实际用的目标(发言中途换目标要先结束再重开)
        self._target = 0
        self._active_target = 0

        self._talking = False
        self._frame_number = 0
        self._underrun = 0
        #: Frames sent, frames padded with silence because of underruns, resyncs after stalls.
        self.stats = {"frames": 0, "underrun_frames": 0, "resyncs": 0}

    # —— 控制 ——————————————————————————————————————————————————————————

    @property
    def source(self) -> AudioSource | None:
        return self._source

    @property
    def target(self) -> int:
        """Voice target of the current source (0 = our channel)."""
        return self._target

    @property
    def volume(self) -> float:
        return self._volume

    @volume.setter
    def volume(self, value: float) -> None:
        # 只记目标值;下一帧在帧内线性过渡过去,避免爆音
        self._volume = max(0.0, float(value))

    @property
    def bitrate(self) -> int:
        return self._encoder.bitrate

    def set_bitrate(self, bitrate: int) -> None:
        self._wanted_bitrate = bitrate
        self._apply_bitrate()

    def limit_bandwidth(self, max_bandwidth: int | None) -> None:
        """Cap the bitrate so packets plus TCP overhead stay under the server's limit."""
        if max_bandwidth is None or max_bandwidth <= 0:
            self._bitrate_cap = None
        else:
            # 服务器按"语音包长度 + 40 字节"计 TCP 隧道带宽;再留出 UDPTunnel 头和 protobuf 字段
            per_packet_overhead = 40 + 1 + 16
            packets_per_second = 1000 / self.frame_ms
            self._bitrate_cap = int(max_bandwidth - per_packet_overhead * 8 * packets_per_second)
        self._apply_bitrate()

    def _apply_bitrate(self) -> None:
        bitrate = self._wanted_bitrate
        if self._bitrate_cap is not None:
            bitrate = min(bitrate, self._bitrate_cap)
        self._encoder.bitrate = bitrate

    def play(self, source: AudioSource, *, target: int = 0) -> asyncio.Future[None]:
        """Switch to ``source`` immediately. The future resolves when it ends or is replaced.

        ``target`` is 0 for our channel, a registered voice target ID (1–30) or 31 (loopback).
        """
        if not 0 <= target <= 31:
            raise ValueError("target must be 0 (channel), 1-30 (voice target) or 31 (loopback)")
        self._finish_current()
        loop = asyncio.get_running_loop()
        self._source = source
        self._target = target
        self._done = loop.create_future()
        self._wake.set()
        return self._done

    def stop(self) -> None:
        """Stop the current source; the transmission ends with a terminator packet."""
        self._finish_current()
        self._wake.set()

    def _finish_current(self) -> None:
        self._source = None
        if self._done is not None and not self._done.done():
            self._done.set_result(None)
        self._done = None

    # —— 发送循环 ——————————————————————————————————————————————————————

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        period = self.frame_ms / 1000
        deadline = loop.time()
        try:
            while True:
                if self._source is None and not self._talking:
                    self._wake.clear()
                    await self._wake.wait()
                    deadline = loop.time()
                    continue

                now = loop.time()
                if now - deadline > self._max_lag * period:
                    self.stats["resyncs"] += 1
                    deadline = now
                elif deadline > now:
                    await asyncio.sleep(deadline - now)
                deadline += period
                await self._tick()
        finally:
            done, self._done = self._done, None
            self._source = None
            if done is not None and not done.done():
                # 连接断了不等于歌放完:让等待方能分辨
                done.set_exception(ConnectionClosedError("connection closed during playback"))
                done.exception()  # 没人 await 时不报 "exception was never retrieved"
            self._encoder.close()

    async def _tick(self) -> None:
        if self._talking and self._target != self._active_target:
            # 新音源的目标不同:先用旧目标结束上一段发言,下一帧再向新目标重新开始
            await self._emit(self._silence, terminator=True)
            return
        source = self._source
        pcm = source.read(self.frame_size) if source is not None else None

        if pcm is None:
            # 音源结束(或被 stop):用一个带终止标记的静音帧结束这次发言
            if source is not None and source is self._source:
                self._finish_current()
            if self._talking:
                await self._emit(self._silence, terminator=True)
            return

        if not pcm:
            # 欠载:短时间内补静音保持发言状态,太久就结束发言,等数据回来再重新开始
            if not self._talking:
                return
            if self._underrun < self._grace_frames:
                self._underrun += 1
                self.stats["underrun_frames"] += 1
                await self._emit(self._silence, terminator=False)
            else:
                await self._emit(self._silence, terminator=True)
            return

        self._underrun = 0
        if len(pcm) < self.frame_size:
            pcm = pcm + bytes(self.frame_size - len(pcm))
        target = self._volume
        pcm = apply_gain(pcm, self._applied_volume, target, self.channels)
        self._applied_volume = target
        await self._emit(pcm, terminator=False)

    async def _emit(self, pcm: bytes, *, terminator: bool) -> None:
        if not self._talking:
            self._active_target = self._target
        packet = self._encoder.encode(pcm)
        await self._send(packet, self._frame_number, terminator, self._active_target)
        self.stats["frames"] += 1
        if terminator:
            # 下一次发言从 0 重新计数,编码器状态也清掉,避免把上一段的尾巴带过去
            self._talking = False
            self._frame_number = 0
            self._underrun = 0
            self._encoder.reset()
        else:
            self._talking = True
            self._frame_number += self.frame_ms // 10
