import io
import re
import struct
import time
import wave
from pathlib import Path
from typing import Optional

import numpy as np
from piper.config import SynthesisConfig
import cache as cache_module

_voice_cache: dict = {}


def _trim_silence(wav_bytes: bytes, threshold: int = 300, frame_ms: int = 10) -> bytes:
    buf = io.BytesIO(wav_bytes)
    with wave.open(buf, "rb") as wf:
        params = wf.getparams()
        raw = wf.readframes(wf.getnframes())

    n_samples = len(raw) // 2
    if n_samples == 0:
        return wav_bytes

    frame_size = max(1, int(params.framerate * frame_ms / 1000))
    samples = struct.unpack(f"<{n_samples}h", raw)

    start = 0
    for i in range(0, n_samples - frame_size, frame_size):
        if max(abs(s) for s in samples[i : i + frame_size]) > threshold:
            start = max(0, i - frame_size)
            break

    end = n_samples
    for i in range(n_samples - frame_size, frame_size, -frame_size):
        if max(abs(s) for s in samples[i : i + frame_size]) > threshold:
            end = min(n_samples, i + frame_size * 2)
            break

    if start >= end:
        return wav_bytes

    trimmed = struct.pack(f"<{end - start}h", *samples[start:end])
    out = io.BytesIO()
    with wave.open(out, "wb") as wf:
        wf.setparams(params)
        wf.writeframes(trimmed)
    return out.getvalue()


def _resample(wav_bytes: bytes, target_rate: int) -> bytes:
    buf = io.BytesIO(wav_bytes)
    with wave.open(buf, "rb") as wf:
        params = wf.getparams()
        raw = wf.readframes(wf.getnframes())
    if params.framerate == target_rate:
        return wav_bytes
    samples = np.frombuffer(raw, dtype=np.int16).astype(np.float32)
    new_length = int(len(samples) * target_rate / params.framerate)
    resampled = np.interp(
        np.linspace(0, len(samples) - 1, new_length),
        np.arange(len(samples)),
        samples,
    ).astype(np.int16)
    out = io.BytesIO()
    with wave.open(out, "wb") as wf:
        wf.setnchannels(params.nchannels)
        wf.setsampwidth(params.sampwidth)
        wf.setframerate(target_rate)
        wf.writeframes(resampled.tobytes())
    return out.getvalue()


def load_voice(model_path: str, config_path: Optional[str] = None):
    if model_path in _voice_cache:
        return _voice_cache[model_path]
    from piper import PiperVoice
    voice = PiperVoice.load(model_path, config_path=config_path)
    _voice_cache[model_path] = voice
    return voice


def _synthesize_text(voice, text: str, syn_config: SynthesisConfig) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(voice.config.sample_rate)
        for chunk in voice.synthesize(text, syn_config):
            wf.writeframes(chunk.audio_int16_bytes)
    return _trim_silence(buf.getvalue())


def _synthesize_with_segment_cache(
    voice,
    voice_name: str,
    segments: list[str],
    syn_config: SynthesisConfig,
    length_scale: float,
    noise_scale: float,
    noise_w: float,
    namespace: str = "",
) -> tuple[list[bytes], int, int]:
    wav_blobs = []
    hits = 0
    misses = 0

    for seg in segments:
        key = cache_module.segment_key(seg, voice_name, length_scale, noise_scale, noise_w, namespace=namespace)
        cached = cache_module.get_segment(key, namespace=namespace)
        if cached:
            wav_blobs.append(cached)
            hits += 1
        else:
            wav = _synthesize_text(voice, seg, syn_config)
            cache_module.put_segment(key, wav, namespace=namespace)
            wav_blobs.append(wav)
            misses += 1

    return wav_blobs, hits, misses


def _concat_wavs(wav_blobs: list[bytes], pause_ms: int = 0) -> bytes:
    if len(wav_blobs) == 1:
        return wav_blobs[0]

    frames_list = []
    params = None

    for blob in wav_blobs:
        buf = io.BytesIO(blob)
        with wave.open(buf, "rb") as wf:
            if params is None:
                params = wf.getparams()
            frames_list.append(wf.readframes(wf.getnframes()))
            if pause_ms > 0:
                n_samples = int(params.framerate * pause_ms / 1000) * params.nchannels
                frames_list.append(b"\x00\x00" * n_samples)

    out = io.BytesIO()
    with wave.open(out, "wb") as wf:
        wf.setparams(params)
        for f in frames_list:
            wf.writeframes(f)
    return out.getvalue()


def _split_segments(text: str) -> list[str]:
    parts = re.split(r"(?<=[.!?,;:])\s+", text.strip())
    return [p.strip() for p in parts if p.strip()]


def generate_audio(
    model_path: str,
    text: str,
    voice_name: str,
    config_path: Optional[str] = None,
    length_scale: float = 0.95,
    noise_scale: float = 0.85,
    noise_w: float = 0.9,
    pause_ms: int = 150,
    output_sample_rate: Optional[int] = None,
    namespace: str = "",
) -> tuple[bytes, dict]:
    voice = load_voice(model_path, config_path)
    syn_config = SynthesisConfig(
        length_scale=length_scale,
        noise_scale=noise_scale,
        noise_w_scale=noise_w,
    )

    t_start = time.perf_counter()
    segments = _split_segments(text)

    t_inference_start = time.perf_counter()
    wav_blobs, cache_hits, cache_misses = _synthesize_with_segment_cache(
        voice, voice_name, segments, syn_config, length_scale, noise_scale, noise_w, namespace=namespace
    )
    t_inference_end = time.perf_counter()

    wav_bytes = _concat_wavs(wav_blobs, pause_ms)
    if output_sample_rate and output_sample_rate != voice.config.sample_rate:
        wav_bytes = _resample(wav_bytes, output_sample_rate)
    t_end = time.perf_counter()

    return wav_bytes, {
        "model_inference_ms": round((t_inference_end - t_inference_start) * 1000, 2),
        "file_write_ms": round((t_end - t_inference_end) * 1000, 2),
        "total_ms": round((t_end - t_start) * 1000, 2),
        "segments": len(segments),
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
    }


def parse_template(template: str) -> list[tuple[str, str]]:
    """
    Descompone un template en partes fijas y variables.
    Ejemplo: "Hola {nombre}. Su saldo es de {saldo} pesos." →
    [('fixed','Hola '), ('var','nombre'), ('fixed','. Su saldo es de '), ('var','saldo'), ('fixed',' pesos.')]
    """
    parts = []
    last = 0
    for m in re.finditer(r"\{(\w+)\}", template):
        if m.start() > last:
            parts.append(("fixed", template[last:m.start()]))
        parts.append(("var", m.group(1)))
        last = m.end()
    if last < len(template):
        parts.append(("fixed", template[last:]))
    return parts


def generate_audio_from_template(
    model_path: str,
    template_parts: list[tuple[str, str]],
    row: dict,
    voice_name: str,
    config_path: Optional[str] = None,
    length_scale: float = 0.95,
    noise_scale: float = 0.85,
    noise_w: float = 0.9,
    pause_ms: int = 150,
    output_sample_rate: Optional[int] = None,
    namespace: str = "",
) -> tuple[bytes, dict]:
    voice = load_voice(model_path, config_path)
    syn_config = SynthesisConfig(
        length_scale=length_scale,
        noise_scale=noise_scale,
        noise_w_scale=noise_w,
    )
    t_start = time.perf_counter()

    wav_blobs = []
    cache_hits = 0
    cache_misses = 0

    for part_type, content in template_parts:
        if part_type == "fixed":
            text = content.strip()
            if not text:
                continue
            key = cache_module.segment_key(text, voice_name, length_scale, noise_scale, noise_w, namespace=namespace)
            cached = cache_module.get_segment(key, namespace=namespace)
            if cached:
                wav_blobs.append(cached)
                cache_hits += 1
            else:
                wav = _synthesize_text(voice, text, syn_config)
                cache_module.put_segment(key, wav, namespace=namespace)
                wav_blobs.append(wav)
                cache_misses += 1
        else:
            value = str(row.get(content, f"{{{content}}}")).strip()
            if value:
                wav = _synthesize_text(voice, value, syn_config)
                wav_blobs.append(wav)
                cache_misses += 1

    t_inference_end = time.perf_counter()
    # Pausa mínima entre partes del template (son trozos de la misma frase, no oraciones separadas)
    wav_bytes = _concat_wavs(wav_blobs, pause_ms=20)
    if output_sample_rate:
        wav_bytes = _resample(wav_bytes, output_sample_rate)
    t_end = time.perf_counter()

    return wav_bytes, {
        "model_inference_ms": round((t_inference_end - t_start) * 1000, 2),
        "file_write_ms": round((t_end - t_inference_end) * 1000, 2),
        "total_ms": round((t_end - t_start) * 1000, 2),
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
    }


def list_available_voices(models_dir: str) -> list[dict]:
    base = Path(models_dir)
    voices = []
    for onnx in sorted(base.glob("**/*.onnx")):
        json_path = onnx.with_suffix(".onnx.json")
        voices.append({
            "name": onnx.stem,
            "path": str(onnx),
            "config": str(json_path) if json_path.exists() else None,
            "ready": json_path.exists(),
        })
    return voices
