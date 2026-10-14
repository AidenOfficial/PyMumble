"""Minimal ctypes binding to the system libopus (encoder and decoder only).

Written against the stable libopus C API (opus.h / opus_defines.h). Requires libopus 1.x to be
installed (``apt install libopus0``, ``brew install opus``, ...).
"""

from __future__ import annotations

import ctypes
import ctypes.util
import os
from enum import IntEnum
from typing import Final

SAMPLE_RATE: Final = 48_000
#: libopus 允许的最长一帧(120 ms)的单声道样本数,解码缓冲按这个分配
_MAX_FRAME_SAMPLES: Final = SAMPLE_RATE * 120 // 1000
#: 单个 Opus 包的上限;官方建议 4000 字节足够任何码率
_MAX_PACKET: Final = 4000

_OPUS_OK: Final = 0
# opus_defines.h 里的 CTL 请求号
_SET_BITRATE: Final = 4002
_SET_VBR: Final = 4006
_SET_COMPLEXITY: Final = 4010
_SET_INBAND_FEC: Final = 4012
_SET_PACKET_LOSS_PERC: Final = 4014
_SET_SIGNAL: Final = 4024
_RESET_STATE: Final = 4028

_SIGNAL_MUSIC: Final = 3002


class Application(IntEnum):
    """``OPUS_APPLICATION_*``: what the encoder optimises for."""

    VOIP = 2048
    AUDIO = 2049
    RESTRICTED_LOWDELAY = 2051


class OpusError(RuntimeError):
    def __init__(self, code: int, where: str) -> None:
        super().__init__(f"{where}: {_lib().opus_strerror(code).decode()} ({code})")
        self.code = code


_LIB: ctypes.CDLL | None = None


def _lib() -> ctypes.CDLL:
    global _LIB
    if _LIB is not None:
        return _LIB
    # 允许用环境变量指定库路径(Windows、自带 libopus 的发行包等)
    path = os.environ.get("PYMUMBLE_REV_LIBOPUS") or ctypes.util.find_library("opus")
    if not path:
        raise OSError(
            "libopus not found; install it (e.g. `apt install libopus0`) "
            "or set PYMUMBLE_REV_LIBOPUS to its path"
        )
    lib = ctypes.CDLL(path)

    p = ctypes.c_void_p
    i32 = ctypes.c_int32
    int_p = ctypes.POINTER(ctypes.c_int)
    lib.opus_strerror.argtypes = [ctypes.c_int]
    lib.opus_strerror.restype = ctypes.c_char_p
    lib.opus_get_version_string.restype = ctypes.c_char_p

    lib.opus_encoder_create.argtypes = [i32, ctypes.c_int, ctypes.c_int, int_p]
    lib.opus_encoder_create.restype = p
    lib.opus_encoder_destroy.argtypes = [p]
    lib.opus_encoder_destroy.restype = None
    lib.opus_encode.argtypes = [p, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, i32]
    lib.opus_encode.restype = i32
    # opus_encoder_ctl 是可变参数函数:只声明固定参数,其余按 c_int32 传
    lib.opus_encoder_ctl.restype = ctypes.c_int

    lib.opus_decoder_create.argtypes = [i32, ctypes.c_int, int_p]
    lib.opus_decoder_create.restype = p
    lib.opus_decoder_destroy.argtypes = [p]
    lib.opus_decoder_destroy.restype = None
    lib.opus_decode.argtypes = [
        p,
        ctypes.c_char_p,
        i32,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_int,
    ]
    lib.opus_decode.restype = ctypes.c_int
    _LIB = lib
    return lib


def version() -> str:
    """The libopus version string, e.g. ``"libopus 1.4"``."""
    return str(_lib().opus_get_version_string().decode())


def _check(code: int, where: str) -> int:
    if code < _OPUS_OK:
        raise OpusError(code, where)
    return code


class Encoder:
    """Encodes 16-bit little-endian interleaved PCM at 48 kHz into Opus packets."""

    # 类级默认值:构造函数中途抛错时 __del__ 也能安全执行
    _handle: int | None = None

    def __init__(
        self,
        channels: int = 1,
        application: Application = Application.AUDIO,
        *,
        bitrate: int = 96_000,
        complexity: int = 10,
        music: bool = True,
    ) -> None:
        if channels not in (1, 2):
            raise ValueError("channels must be 1 or 2")
        lib = _lib()
        err = ctypes.c_int()
        handle = lib.opus_encoder_create(SAMPLE_RATE, channels, int(application), ctypes.byref(err))
        _check(err.value, "opus_encoder_create")
        self._handle = handle
        self.channels = channels
        self._out = ctypes.create_string_buffer(_MAX_PACKET)
        self.bitrate = bitrate
        self._ctl(_SET_COMPLEXITY, complexity)
        self._ctl(_SET_VBR, 1)
        if music:
            self._ctl(_SET_SIGNAL, _SIGNAL_MUSIC)

    def _ctl(self, request: int, value: int | None = None) -> None:
        args: list[ctypes.c_int32] = [] if value is None else [ctypes.c_int32(value)]
        # 可变参数函数没有 argtypes:句柄必须显式包成 c_void_p,否则 ctypes 按 32 位 int 传,
        # 高地址(线程的 malloc arena 通常在 0x7f...)会被截断成野指针
        handle = ctypes.c_void_p(self._require())
        _check(_lib().opus_encoder_ctl(handle, ctypes.c_int(request), *args), "ctl")

    def _require(self) -> int:
        if self._handle is None:
            raise ValueError("encoder is closed")
        return self._handle

    @property
    def bitrate(self) -> int:
        return self._bitrate

    @bitrate.setter
    def bitrate(self, value: int) -> None:
        # libopus 接受 500 ~ 512000;超出范围直接夹住,不让调用方因为服务器限速值奇怪而崩
        value = max(6_000, min(int(value), 510_000))
        self._ctl(_SET_BITRATE, value)
        self._bitrate = value

    def reset(self) -> None:
        """Forget the encoder's history, e.g. before an unrelated new stream."""
        self._ctl(_RESET_STATE)

    def encode(self, pcm: bytes) -> bytes:
        """Encode one frame. ``len(pcm)`` must be a valid Opus frame (2.5–60 ms)."""
        samples = len(pcm) // (2 * self.channels)
        if samples * 2 * self.channels != len(pcm):
            raise ValueError("pcm length is not a whole number of samples")
        n = _lib().opus_encode(self._require(), pcm, samples, self._out, _MAX_PACKET)
        _check(n, "opus_encode")
        return self._out.raw[:n]

    def close(self) -> None:
        if self._handle is not None:
            _lib().opus_encoder_destroy(self._handle)
            self._handle = None

    def __del__(self) -> None:
        self.close()


class Decoder:
    """Decodes Opus packets into 16-bit little-endian interleaved PCM at 48 kHz."""

    _handle: int | None = None

    def __init__(self, channels: int = 1) -> None:
        if channels not in (1, 2):
            raise ValueError("channels must be 1 or 2")
        lib = _lib()
        err = ctypes.c_int()
        handle = lib.opus_decoder_create(SAMPLE_RATE, channels, ctypes.byref(err))
        _check(err.value, "opus_decoder_create")
        self._handle = handle
        self.channels = channels
        self._pcm = ctypes.create_string_buffer(_MAX_FRAME_SAMPLES * 2 * channels)

    def decode(self, packet: bytes | None) -> bytes:
        """Decode one packet. ``None`` asks libopus to conceal one lost 20 ms frame."""
        if self._handle is None:
            raise ValueError("decoder is closed")
        if packet is None:
            frame = SAMPLE_RATE // 50
            n = _lib().opus_decode(self._handle, None, 0, self._pcm, frame, 0)
        else:
            n = _lib().opus_decode(
                self._handle, packet, len(packet), self._pcm, _MAX_FRAME_SAMPLES, 0
            )
        _check(n, "opus_decode")
        return self._pcm.raw[: n * 2 * self.channels]

    def close(self) -> None:
        if self._handle is not None:
            _lib().opus_decoder_destroy(self._handle)
            self._handle = None

    def __del__(self) -> None:
        self.close()
