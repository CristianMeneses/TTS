import hashlib
import os
import re
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Optional

# ── Ubicación del caché ───────────────────────────────────────────────────────
# IMPORTANTE: el caché NO debe vivir en una carpeta sincronizada (OneDrive/Dropbox).
# La sincronización vuelve lentas las lecturas/escrituras y bloquea archivos
# (rompe el borrado). Usamos almacenamiento local. Se puede override con TTS_CACHE_DIR.
_base = os.environ.get("TTS_CACHE_DIR") or os.environ.get("LOCALAPPDATA") or os.environ.get("TEMP") or "."
CACHE_DIR = Path(_base) / "tts_cache"
SEGMENT_DIR = CACHE_DIR / "segments"

# Cachés en memoria SEPARADOS — los audios completos (grandes) no deben expulsar
# a los segmentos (pequeños y muy reutilizados).
_FULL_MEM_LIMIT = 256        # audios completos: ~190KB c/u
_SEG_MEM_LIMIT = 8192        # segmentos: pequeños, muchos hits → conservar muchos

_full_mem: "OrderedDict[str, bytes]" = OrderedDict()
_seg_mem: "OrderedDict[str, bytes]" = OrderedDict()
_lock = threading.Lock()

CACHE_DIR.mkdir(parents=True, exist_ok=True)
SEGMENT_DIR.mkdir(parents=True, exist_ok=True)


def _normalize(text: str) -> str:
    return re.sub(r"^[\s.,;:!?¡¿]+|[\s.,;:!?¡¿]+$", "", text.strip().lower())


def _mem_get(store: "OrderedDict[str, bytes]", key: str) -> Optional[bytes]:
    """Lee de RAM y marca como usado recientemente (LRU)."""
    with _lock:
        data = store.get(key)
        if data is not None:
            store.move_to_end(key)
        return data


def _mem_put(store: "OrderedDict[str, bytes]", key: str, data: bytes, limit: int) -> None:
    """Guarda en RAM y expulsa el menos usado recientemente si se pasa del límite."""
    with _lock:
        store[key] = data
        store.move_to_end(key)
        while len(store) > limit:
            store.popitem(last=False)  # expulsa el LRU (frente), no el más reciente


# ── Claves ───────────────────────────────────────────────────────────────────

def cache_key(text: str, voice_name: str, length_scale: float, noise_scale: float, noise_w: float, pause_ms: int, output_sample_rate: int = 22050) -> str:
    raw = f"{text}|{voice_name}|{length_scale}|{noise_scale}|{noise_w}|{pause_ms}|{output_sample_rate}"
    return hashlib.sha256(raw.encode()).hexdigest()


def segment_key(text: str, voice_name: str, length_scale: float, noise_scale: float, noise_w: float) -> str:
    raw = f"seg|{_normalize(text)}|{voice_name}|{length_scale}|{noise_scale}|{noise_w}"
    return hashlib.sha256(raw.encode()).hexdigest()


# ── Caché de audio completo ──────────────────────────────────────────────────

def get(key: str) -> Optional[bytes]:
    cached = _mem_get(_full_mem, key)
    if cached is not None:
        return cached
    path = CACHE_DIR / f"{key}.wav"
    if path.exists():
        data = path.read_bytes()
        _mem_put(_full_mem, key, data, _FULL_MEM_LIMIT)
        return data
    return None


def put(key: str, wav_bytes: bytes) -> None:
    _mem_put(_full_mem, key, wav_bytes, _FULL_MEM_LIMIT)
    threading.Thread(target=_write_safe, args=(CACHE_DIR / f"{key}.wav", wav_bytes), daemon=True).start()


# ── Caché de segmentos ───────────────────────────────────────────────────────

def get_segment(key: str) -> Optional[bytes]:
    cached = _mem_get(_seg_mem, key)
    if cached is not None:
        return cached
    path = SEGMENT_DIR / f"{key}.wav"
    if path.exists():
        data = path.read_bytes()
        _mem_put(_seg_mem, key, data, _SEG_MEM_LIMIT)
        return data
    return None


def put_segment(key: str, wav_bytes: bytes) -> None:
    _mem_put(_seg_mem, key, wav_bytes, _SEG_MEM_LIMIT)
    threading.Thread(target=_write_safe, args=(SEGMENT_DIR / f"{key}.wav", wav_bytes), daemon=True).start()


def _write_safe(path: Path, data: bytes) -> None:
    """Escritura a disco que no revienta si el archivo está bloqueado."""
    try:
        path.write_bytes(data)
    except OSError:
        pass


# ── Stats y limpieza ─────────────────────────────────────────────────────────

def stats() -> dict:
    full_files = list(CACHE_DIR.glob("*.wav"))
    seg_files = list(SEGMENT_DIR.glob("*.wav"))
    total_bytes = sum(f.stat().st_size for f in full_files + seg_files)
    return {
        "entries": len(full_files) + len(seg_files),
        "full_audio_entries": len(full_files),
        "segment_entries": len(seg_files),
        "mem_full": len(_full_mem),
        "mem_segments": len(_seg_mem),
        "total_size_mb": round(total_bytes / 1024 / 1024, 3),
        "cache_dir": str(CACHE_DIR),
    }


def clear() -> int:
    """Borra todo el caché en disco y RAM. Resiliente a archivos bloqueados."""
    deleted = 0
    targets = (
        list(CACHE_DIR.glob("*.wav")) + list(SEGMENT_DIR.glob("*.wav"))
        + list(CACHE_DIR.glob("*.json")) + list(SEGMENT_DIR.glob("*.json"))
    )
    for f in targets:
        try:
            f.unlink()
            deleted += 1
        except OSError:
            pass  # archivo bloqueado/sincronizando — se ignora
    with _lock:
        _full_mem.clear()
        _seg_mem.clear()
    return deleted
