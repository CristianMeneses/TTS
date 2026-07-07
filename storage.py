"""Rutas de almacenamiento de audios y validación de componentes de ruta.

Fuente única de verdad compartida por el API (`main.py`) y los consumidores de
síntesis (`tts_consumer.py`). Centralizar aquí la validación evita que la lógica
de seguridad de paths se desincronice entre procesos.
"""

import os
import re
from pathlib import Path

# IMPORTANTE: mismo cálculo en todos los procesos para que el consumidor escriba
# donde el API sirve los audios (GET /v1/audio). Override con TTS_STORAGE_DIR.
_storage_base = os.environ.get("TTS_STORAGE_DIR") or os.environ.get("LOCALAPPDATA") or os.environ.get("TEMP") or "."
STORAGE_DIR = Path(_storage_base) / "tts_output"
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

ID_RE   = re.compile(r"[A-Za-z0-9._\-]{1,128}")
UUID_RE = re.compile(r"[0-9a-fA-F]{32}")


def safe_component(value: str, label: str) -> str:
    """Valida un componente de ruta (client_id / campaign_id).

    Lanza ValueError si contiene algo distinto de letras, números, '.', '_' o '-',
    o si es '.'/'..'. El caller HTTP lo traduce a 400; el consumidor lo maneja como
    fallo de tarea.
    """
    value = (value or "").strip()
    if value in (".", "..") or not ID_RE.fullmatch(value):
        raise ValueError(f"{label} inválido: use solo letras, números, '.', '_' o '-'")
    return value


def campaign_dir(client_id: str, campaign_id: str) -> Path:
    """Directorio raíz de una campaña, validado contra path traversal."""
    c  = safe_component(client_id,  "client_id")
    ca = safe_component(campaign_id, "campaign_id")
    d  = (STORAGE_DIR / c / ca).resolve()
    root = STORAGE_DIR.resolve()
    if root != d and root not in d.parents:
        raise ValueError("Ruta de campaña inválida")
    return d


def audios_dir(client_id: str, campaign_id: str) -> Path:
    """Directorio donde viven los .wav de una campaña."""
    return campaign_dir(client_id, campaign_id) / "audios"
