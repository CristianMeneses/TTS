"""Conversión de audio a μ-law (G.711) para telefonía.

Salida objetivo:
    Stream #0:0: Audio: pcm_mulaw ([7][0][0][0] / 0x0007), 8000 Hz, mono, s16 (8 bit), 64 kb/s

Es un WAV con wFormatTag=0x0007 (WAVE_FORMAT_MULAW), 8000 Hz, mono, 8 bits/muestra.

El encode G.711 lo hace `audioop.lin2ulaw` (implementación C exacta y probada). Python 3.13+
removió `audioop` de la stdlib, por eso se usa el backport `audioop-lts` (provee el mismo
módulo `audioop`). El escritor de cabecera μ-law se hace a mano porque el módulo `wave` no
escribe formatos no-PCM.
"""

import io
import struct
import wave

import audioop  # provisto por audioop-lts en Python 3.13+

WAVE_FORMAT_MULAW = 0x0007


def _read_wav_mono16(wav_bytes: bytes) -> tuple[bytes, int]:
    """Extrae frames PCM int16 mono y la frecuencia de un WAV PCM."""
    with wave.open(io.BytesIO(wav_bytes), "rb") as wf:
        n_channels = wf.getnchannels()
        rate = wf.getframerate()
        width = wf.getsampwidth()
        raw = wf.readframes(wf.getnframes())
    if width != 2:
        raw = audioop.lin2lin(raw, width, 2)
        width = 2
    if n_channels > 1:
        raw = audioop.tomono(raw, 2, 0.5, 0.5)
    return raw, rate


def _build_mulaw_wav(ulaw_bytes: bytes, sample_rate: int) -> bytes:
    """Construye un WAV con cabecera μ-law (formato 0x0007) + chunk `fact`."""
    n_samples = len(ulaw_bytes)
    channels = 1
    bits = 8
    byte_rate = sample_rate * channels * bits // 8   # 8000 * 1 * 1 = 8000 B/s (64 kb/s)
    block_align = channels * bits // 8               # 1

    # fmt chunk para formatos no-PCM: 18 bytes (incluye cbSize=0)
    fmt = struct.pack(
        "<HHIIHHH",
        WAVE_FORMAT_MULAW,  # wFormatTag
        channels,           # nChannels
        sample_rate,        # nSamplesPerSec
        byte_rate,          # nAvgBytesPerSec
        block_align,        # nBlockAlign
        bits,               # wBitsPerSample
        0,                  # cbSize
    )
    fact = struct.pack("<I", n_samples)  # número de muestras

    pad = b"\x00" if (n_samples & 1) else b""
    body = (
        b"WAVE"
        + b"fmt " + struct.pack("<I", len(fmt)) + fmt
        + b"fact" + struct.pack("<I", len(fact)) + fact
        + b"data" + struct.pack("<I", n_samples) + ulaw_bytes + pad
    )
    return b"RIFF" + struct.pack("<I", len(body)) + body


def to_mulaw_wav(pcm16_wav_bytes: bytes, target_rate: int = 8000) -> bytes:
    """Convierte un WAV PCM 16-bit (cualquier frecuencia/canales) a WAV μ-law 8 kHz mono.

    Si la entrada no está a `target_rate`, se remuestrea (audioop.ratecv, calidad telefonía).
    """
    raw, rate = _read_wav_mono16(pcm16_wav_bytes)
    if rate != target_rate:
        raw, _ = audioop.ratecv(raw, 2, 1, rate, target_rate, None)
    ulaw = audioop.lin2ulaw(raw, 2)
    return _build_mulaw_wav(ulaw, target_rate)
