import io
import re
import time
import wave
from pathlib import Path
from typing import Optional

import numpy as np

import cache as cache_module

KOKORO_MODEL = Path("models/kokoro/kokoro-v1.0.onnx")
KOKORO_VOICES = Path("models/kokoro/voices-v1.0.bin")
KOKORO_SAMPLE_RATE = 24000

_LANG_MAP: dict[str, tuple[str, str]] = {
    "af_": ("en-us", "Ingles US F"),
    "am_": ("en-us", "Ingles US M"),
    "bf_": ("en-gb", "Ingles UK F"),
    "bm_": ("en-gb", "Ingles UK M"),
    "ef_": ("es",    "Espanol F"),
    "em_": ("es",    "Espanol M"),
    "ff_": ("fr-fr", "Frances F"),
    "hf_": ("hi",    "Hindi F"),
    "hm_": ("hi",    "Hindi M"),
    "if_": ("it",    "Italiano F"),
    "im_": ("it",    "Italiano M"),
    "jf_": ("ja",    "Japones F"),
    "jm_": ("ja",    "Japones M"),
    "pf_": ("pt-br", "Portugues F"),
    "pm_": ("pt-br", "Portugues M"),
    "zf_": ("zh",    "Chino F"),
    "zm_": ("zh",    "Chino M"),
}

_instance = None


def is_available() -> bool:
    return KOKORO_MODEL.exists() and KOKORO_VOICES.exists()


def load_kokoro():
    global _instance
    if _instance is None:
        from kokoro_onnx import Kokoro
        _instance = Kokoro(str(KOKORO_MODEL), str(KOKORO_VOICES))
    return _instance


def voice_lang(voice_id: str) -> str:
    return _LANG_MAP.get(voice_id[:3], ("en-us", "?"))[0]


def list_voices() -> list[dict]:
    if not is_available():
        return []
    try:
        raw = np.load(str(KOKORO_VOICES), allow_pickle=True)
        names = sorted(raw.files if hasattr(raw, "files") else raw.keys())
        result = []
        for name in names:
            lang_code, lang_label = _LANG_MAP.get(name[:3], ("en-us", "Ingles"))
            result.append({
                "name": f"kokoro:{name}",
                "engine": "kokoro",
                "voice_id": name,
                "language": lang_label,
                "lang_code": lang_code,
                "ready": True,
            })
        return result
    except Exception:
        return []


# ── Utilidades WAV ────────────────────────────────────────────────────────────

def _samples_to_wav(samples: np.ndarray, sample_rate: int) -> bytes:
    pcm = (samples * 32767).astype(np.int16)
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(pcm.tobytes())
    return buf.getvalue()


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
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(target_rate)
        wf.writeframes(resampled.tobytes())
    return out.getvalue()


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


# ── Síntesis con caché de segmentos ─────────────────────────────────────────

def _synthesize_segment(kokoro, text: str, voice_id: str, speed: float, lang: str) -> bytes:
    """Sintetiza un segmento a 24kHz nativo, sin resamplear."""
    samples, sample_rate = kokoro.create(text=text, voice=voice_id, speed=speed, lang=lang)
    return _samples_to_wav(samples, sample_rate)


def _synthesize_with_segment_cache(
    voice_name: str,
    segments: list[str],
    voice_id: str,
    speed: float,
    lang: str,
) -> tuple[list[bytes], int, int]:
    kokoro = load_kokoro()
    wav_blobs = []
    hits = 0
    misses = 0
    for seg in segments:
        # speed como equivalente a length_scale; noise_scale/noise_w no aplican → 0
        key = cache_module.segment_key(seg, voice_name, speed, 0, 0)
        cached = cache_module.get_segment(key)
        if cached:
            wav_blobs.append(cached)
            hits += 1
        else:
            wav = _synthesize_segment(kokoro, seg, voice_id, speed, lang)
            cache_module.put_segment(key, wav)
            wav_blobs.append(wav)
            misses += 1
    return wav_blobs, hits, misses


# ── API pública ───────────────────────────────────────────────────────────────

def generate_audio(
    text: str,
    voice_id: str,
    speed: float = 1.0,
    lang: Optional[str] = None,
    pause_ms: int = 150,
    output_sample_rate: Optional[int] = None,
) -> tuple[bytes, dict]:
    if lang is None:
        lang = voice_lang(voice_id)
    voice_name = f"kokoro:{voice_id}"

    t_start = time.perf_counter()
    segments = _split_segments(text)

    t_inf_start = time.perf_counter()
    wav_blobs, cache_hits, cache_misses = _synthesize_with_segment_cache(
        voice_name, segments, voice_id, speed, lang
    )
    t_inf_end = time.perf_counter()

    wav_bytes = _concat_wavs(wav_blobs, pause_ms)
    if output_sample_rate and output_sample_rate != KOKORO_SAMPLE_RATE:
        wav_bytes = _resample(wav_bytes, output_sample_rate)
    t_end = time.perf_counter()

    return wav_bytes, {
        "model_inference_ms": round((t_inf_end - t_inf_start) * 1000, 2),
        "file_write_ms": round((t_end - t_inf_end) * 1000, 2),
        "total_ms": round((t_end - t_start) * 1000, 2),
        "segments": len(segments),
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
    }


def generate_audio_from_template(
    template_parts: list[tuple[str, str]],
    row: dict,
    voice_name: str,
    speed: float = 1.0,
    lang: Optional[str] = None,
    output_sample_rate: Optional[int] = None,
) -> tuple[bytes, dict]:
    voice_id = voice_name[7:] if voice_name.startswith("kokoro:") else voice_name
    if lang is None:
        lang = voice_lang(voice_id)

    kokoro = load_kokoro()
    t_start = time.perf_counter()
    wav_blobs = []
    cache_hits = 0
    cache_misses = 0

    for part_type, content in template_parts:
        if part_type == "fixed":
            text = content.strip()
            if not text:
                continue
            key = cache_module.segment_key(text, voice_name, speed, 0, 0)
            cached = cache_module.get_segment(key)
            if cached:
                wav_blobs.append(cached)
                cache_hits += 1
            else:
                wav = _synthesize_segment(kokoro, text, voice_id, speed, lang)
                cache_module.put_segment(key, wav)
                wav_blobs.append(wav)
                cache_misses += 1
        else:
            value = str(row.get(content, f"{{{content}}}")).strip()
            if value:
                wav = _synthesize_segment(kokoro, value, voice_id, speed, lang)
                wav_blobs.append(wav)
                cache_misses += 1

    t_inf_end = time.perf_counter()
    wav_bytes = _concat_wavs(wav_blobs, pause_ms=20)
    if output_sample_rate and output_sample_rate != KOKORO_SAMPLE_RATE:
        wav_bytes = _resample(wav_bytes, output_sample_rate)
    t_end = time.perf_counter()

    return wav_bytes, {
        "model_inference_ms": round((t_inf_end - t_start) * 1000, 2),
        "file_write_ms": round((t_end - t_inf_end) * 1000, 2),
        "total_ms": round((t_end - t_start) * 1000, 2),
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
    }
