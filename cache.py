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
_FULL_MEM_LIMIT  = 256        # audios completos PCM: ~190KB c/u
_SEG_MEM_LIMIT   = 8192       # segmentos: pequeños, muchos hits → conservar muchos
_MULAW_MEM_LIMIT = 512        # μ-law: ~30KB c/u → podemos guardar más

_full_mem:  "OrderedDict[str, bytes]" = OrderedDict()
_seg_mem:   "OrderedDict[str, bytes]" = OrderedDict()
_mulaw_mem: "OrderedDict[str, bytes]" = OrderedDict()
_lock = threading.Lock()

CACHE_DIR.mkdir(parents=True, exist_ok=True)
SEGMENT_DIR.mkdir(parents=True, exist_ok=True)


def _normalize(text: str) -> str:
    return re.sub(r"^[\s.,;:!?¡¿]+|[\s.,;:!?¡¿]+$", "", text.strip().lower())


# ── Namespacing por cliente+campaña ──────────────────────────────────────────
# namespace="" mantiene el layout plano original (lab / audio individual).
# namespace!="" enruta a un subdirectorio propio y entra en el hash de la clave,
# garantizando aislamiento total (un audio de campaña A nunca pega en campaña B).

def _ns_dir(namespace: str) -> Path:
    return CACHE_DIR / f"ns_{hashlib.sha256(namespace.encode()).hexdigest()[:16]}"


def _full_dir(namespace: str) -> Path:
    return CACHE_DIR if not namespace else _ns_dir(namespace)


def _seg_dir(namespace: str) -> Path:
    return SEGMENT_DIR if not namespace else _ns_dir(namespace) / "segments"


def _mulaw_dir(namespace: str) -> Path:
    return CACHE_DIR / "mulaw" if not namespace else _ns_dir(namespace) / "mulaw"


# ── Caché en memoria (LRU) ────────────────────────────────────────────────────

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

def cache_key(text: str, voice_name: str, length_scale: float, noise_scale: float, noise_w: float, pause_ms: int, output_sample_rate: int = 22050, namespace: str = "") -> str:
    prefix = f"ns:{namespace}|" if namespace else ""
    raw = f"{prefix}{text}|{voice_name}|{length_scale}|{noise_scale}|{noise_w}|{pause_ms}|{output_sample_rate}"
    return hashlib.sha256(raw.encode()).hexdigest()


def segment_key(text: str, voice_name: str, length_scale: float, noise_scale: float, noise_w: float, namespace: str = "") -> str:
    prefix = f"ns:{namespace}|" if namespace else ""
    raw = f"{prefix}seg|{_normalize(text)}|{voice_name}|{length_scale}|{noise_scale}|{noise_w}"
    return hashlib.sha256(raw.encode()).hexdigest()


# ── Caché de audio completo ──────────────────────────────────────────────────

def get(key: str, namespace: str = "") -> Optional[bytes]:
    mem_key = f"{namespace}|{key}" if namespace else key
    cached = _mem_get(_full_mem, mem_key)
    if cached is not None:
        return cached
    path = _full_dir(namespace) / f"{key}.wav"
    if path.exists():
        data = path.read_bytes()
        _mem_put(_full_mem, mem_key, data, _FULL_MEM_LIMIT)
        return data
    return None


def put(key: str, wav_bytes: bytes, namespace: str = "") -> None:
    mem_key = f"{namespace}|{key}" if namespace else key
    _mem_put(_full_mem, mem_key, wav_bytes, _FULL_MEM_LIMIT)
    d = _full_dir(namespace)
    d.mkdir(parents=True, exist_ok=True)
    threading.Thread(target=_write_safe, args=(d / f"{key}.wav", wav_bytes), daemon=True).start()


# ── Caché μ-law ──────────────────────────────────────────────────────────────
# Mismo key que el PCM; se guarda en subdir mulaw/ para no confundir con PCM.

def get_mulaw(key: str, namespace: str = "") -> Optional[bytes]:
    mem_key = f"{namespace}|mu|{key}" if namespace else f"mu|{key}"
    cached = _mem_get(_mulaw_mem, mem_key)
    if cached is not None:
        return cached
    path = _mulaw_dir(namespace) / f"{key}.wav"
    if path.exists():
        data = path.read_bytes()
        _mem_put(_mulaw_mem, mem_key, data, _MULAW_MEM_LIMIT)
        return data
    return None


def put_mulaw(key: str, wav_bytes: bytes, namespace: str = "") -> None:
    mem_key = f"{namespace}|mu|{key}" if namespace else f"mu|{key}"
    _mem_put(_mulaw_mem, mem_key, wav_bytes, _MULAW_MEM_LIMIT)
    d = _mulaw_dir(namespace)
    d.mkdir(parents=True, exist_ok=True)
    threading.Thread(target=_write_safe, args=(d / f"{key}.wav", wav_bytes), daemon=True).start()


# ── Caché de segmentos ───────────────────────────────────────────────────────

def get_segment(key: str, namespace: str = "") -> Optional[bytes]:
    mem_key = f"{namespace}|{key}" if namespace else key
    cached = _mem_get(_seg_mem, mem_key)
    if cached is not None:
        return cached
    path = _seg_dir(namespace) / f"{key}.wav"
    if path.exists():
        data = path.read_bytes()
        _mem_put(_seg_mem, mem_key, data, _SEG_MEM_LIMIT)
        return data
    return None


def put_segment(key: str, wav_bytes: bytes, namespace: str = "") -> None:
    mem_key = f"{namespace}|{key}" if namespace else key
    _mem_put(_seg_mem, mem_key, wav_bytes, _SEG_MEM_LIMIT)
    d = _seg_dir(namespace)
    d.mkdir(parents=True, exist_ok=True)
    threading.Thread(target=_write_safe, args=(d / f"{key}.wav", wav_bytes), daemon=True).start()


def _write_safe(path: Path, data: bytes) -> None:
    """Escritura a disco que no revienta si el archivo está bloqueado."""
    try:
        path.write_bytes(data)
    except OSError:
        pass


# ── Stats y limpieza ─────────────────────────────────────────────────────────

def stats() -> dict:
    """Stats globales: recorre todo el árbol (incluye subdirectorios de campaña)."""
    all_wav = list(CACHE_DIR.rglob("*.wav"))
    seg_files   = [f for f in all_wav if f.parent.name == "segments"]
    mulaw_files = [f for f in all_wav if f.parent.name == "mulaw"]
    full_files  = [f for f in all_wav if f.parent.name not in ("segments", "mulaw")]
    total_bytes = sum(f.stat().st_size for f in all_wav)
    return {
        "entries": len(all_wav),
        "full_audio_entries": len(full_files),
        "segment_entries": len(seg_files),
        "mulaw_entries": len(mulaw_files),
        "mem_full": len(_full_mem),
        "mem_segments": len(_seg_mem),
        "mem_mulaw": len(_mulaw_mem),
        "total_size_mb": round(total_bytes / 1024 / 1024, 3),
        "cache_dir": str(CACHE_DIR),
    }


def clear(namespace: Optional[str] = None) -> int:
    """Borra el caché en disco y RAM. Resiliente a archivos bloqueados.

    namespace=None  → borra TODO (todas las campañas + layout del lab).
    namespace="..." → borra solo esa campaña.
    """
    if namespace:
        base = _ns_dir(namespace)
        targets = list(base.rglob("*.wav")) + list(base.rglob("*.json"))
    else:
        targets = list(CACHE_DIR.rglob("*.wav")) + list(CACHE_DIR.rglob("*.json"))

    deleted = 0
    for f in targets:
        try:
            f.unlink()
            deleted += 1
        except OSError:
            pass  # archivo bloqueado/sincronizando — se ignora
    # La RAM se vacía completa: las claves son hashes y no se pueden filtrar por
    # namespace; es un caché, se reconstruye on-demand.
    with _lock:
        _full_mem.clear()
        _seg_mem.clear()
        _mulaw_mem.clear()
    return deleted
