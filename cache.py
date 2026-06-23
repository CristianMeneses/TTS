import hashlib
import re
import threading
from pathlib import Path
from typing import Optional

CACHE_DIR = Path("output/cache")
SEGMENT_DIR = Path("output/cache/segments")
_MEM_LIMIT = 200

# Caché en memoria: key → wav_bytes
_mem_cache: dict[str, bytes] = {}

# Crear directorios una sola vez al importar
CACHE_DIR.mkdir(parents=True, exist_ok=True)
SEGMENT_DIR.mkdir(parents=True, exist_ok=True)


def _normalize(text: str) -> str:
    return re.sub(r"^[\s.,;:!?¡¿]+|[\s.,;:!?¡¿]+$", "", text.strip().lower())


def _mem_put(key: str, data: bytes) -> None:
    if len(_mem_cache) >= _MEM_LIMIT:
        _mem_cache.pop(next(iter(_mem_cache)))
    _mem_cache[key] = data


# ── Claves ───────────────────────────────────────────────────────────────────

def cache_key(text: str, voice_name: str, length_scale: float, noise_scale: float, noise_w: float, pause_ms: int, output_sample_rate: int = 22050) -> str:
    raw = f"{text}|{voice_name}|{length_scale}|{noise_scale}|{noise_w}|{pause_ms}|{output_sample_rate}"
    return hashlib.sha256(raw.encode()).hexdigest()


def segment_key(text: str, voice_name: str, length_scale: float, noise_scale: float, noise_w: float) -> str:
    raw = f"seg|{_normalize(text)}|{voice_name}|{length_scale}|{noise_scale}|{noise_w}"
    return hashlib.sha256(raw.encode()).hexdigest()


# ── Caché de audio completo ──────────────────────────────────────────────────

def get(key: str) -> Optional[bytes]:
    if key in _mem_cache:
        return _mem_cache[key]
    path = CACHE_DIR / f"{key}.wav"
    if path.exists():
        data = path.read_bytes()
        _mem_put(key, data)
        return data
    return None


def put(key: str, wav_bytes: bytes) -> None:
    _mem_put(key, wav_bytes)
    threading.Thread(target=(CACHE_DIR / f"{key}.wav").write_bytes, args=(wav_bytes,), daemon=True).start()


# ── Caché de segmentos ───────────────────────────────────────────────────────

def get_segment(key: str) -> Optional[bytes]:
    if key in _mem_cache:
        return _mem_cache[key]
    path = SEGMENT_DIR / f"{key}.wav"
    if path.exists():
        data = path.read_bytes()
        _mem_put(key, data)
        return data
    return None


def put_segment(key: str, wav_bytes: bytes) -> None:
    _mem_put(key, wav_bytes)
    threading.Thread(target=(SEGMENT_DIR / f"{key}.wav").write_bytes, args=(wav_bytes,), daemon=True).start()


# ── Stats y limpieza ─────────────────────────────────────────────────────────

def stats() -> dict:
    full_files = list(CACHE_DIR.glob("*.wav"))
    seg_files = list(SEGMENT_DIR.glob("*.wav"))
    total_bytes = sum(f.stat().st_size for f in full_files + seg_files)
    return {
        "entries": len(full_files) + len(seg_files),
        "full_audio_entries": len(full_files),
        "segment_entries": len(seg_files),
        "total_size_mb": round(total_bytes / 1024 / 1024, 3),
    }


def clear() -> int:
    files = list(CACHE_DIR.glob("*.wav")) + list(SEGMENT_DIR.glob("*.wav"))
    for f in files:
        f.unlink()
    for f in list(CACHE_DIR.glob("*.json")) + list(SEGMENT_DIR.glob("*.json")):
        f.unlink()
    _mem_cache.clear()
    return len(files)
