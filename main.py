import asyncio
import hashlib
import io
import json
import os
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Optional

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

import audio_format
import cache
import tts_engine
import kokoro_engine

MODELS_DIR = Path("models")
MODELS_DIR.mkdir(parents=True, exist_ok=True)

_storage_base = os.environ.get("TTS_STORAGE_DIR") or os.environ.get("LOCALAPPDATA") or os.environ.get("TEMP") or "."
STORAGE_DIR = Path(_storage_base) / "tts_output"
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

BATCH_CONCURRENCY = int(os.environ.get("TTS_BATCH_CONCURRENCY", "8"))
executor = ThreadPoolExecutor(max_workers=max(12, BATCH_CONCURRENCY + 4))

_voice_map: dict[str, tuple[str, Optional[str]]] = {}
_play_store: dict[str, bytes] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    voices = tts_engine.list_available_voices(str(MODELS_DIR))
    for v in voices:
        if v["ready"]:
            try:
                tts_engine.load_voice(v["path"], v["config"])
                _voice_map[v["name"]] = (v["path"], v["config"])
                print(f"[warmup] Piper: {v['name']}")
            except Exception as e:
                print(f"[warmup] Error cargando {v['name']}: {e}")
    if kokoro_engine.is_available():
        try:
            kokoro_engine.load_kokoro()
            print("[warmup] Kokoro: modelo cargado")
        except Exception as e:
            print(f"[warmup] Kokoro error: {e}")
    yield


app = FastAPI(title="Piper TTS API", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


def _resolve_voice(voice_name: str) -> tuple[str, Optional[str]]:
    if voice_name.startswith("kokoro:"):
        return "", None
    if voice_name in _voice_map:
        return _voice_map[voice_name]
    raise HTTPException(status_code=404, detail=f"Voz '{voice_name}' no encontrada")


def _generate_cached(
    text: str,
    voice_name: str,
    model_path: str,
    config_path: Optional[str],
    length_scale: float,
    noise_scale: float,
    noise_w: float,
    pause_ms: int,
    output_sample_rate: int = 8000,
    namespace: str = "",
) -> tuple[bytes, dict, bool]:
    key = cache.cache_key(text, voice_name, length_scale, noise_scale, noise_w, pause_ms, output_sample_rate, namespace=namespace)
    cached = cache.get(key, namespace=namespace)
    if cached:
        return cached, {"total_ms": 0, "model_inference_ms": 0, "file_write_ms": 0, "segments": 0, "cache_hits": 0, "cache_misses": 0}, True

    if voice_name.startswith("kokoro:"):
        voice_id = voice_name[7:]
        speed = round(1.0 / max(length_scale, 0.5), 3)
        wav, timing = kokoro_engine.generate_audio(
            text=text,
            voice_id=voice_id,
            speed=speed,
            pause_ms=pause_ms,
            output_sample_rate=output_sample_rate,
            namespace=namespace,
        )
    else:
        wav, timing = tts_engine.generate_audio(
            model_path=model_path,
            text=text,
            voice_name=voice_name,
            config_path=config_path,
            length_scale=length_scale,
            noise_scale=noise_scale,
            noise_w=noise_w,
            pause_ms=pause_ms,
            output_sample_rate=output_sample_rate,
            namespace=namespace,
        )
    cache.put(key, wav, namespace=namespace)
    return wav, timing, False


def _generate_batch_row_cached(
    template_parts: list,
    row: dict,
    voice_name: str,
    model_path: str,
    config_path: Optional[str],
    length_scale: float,
    noise_scale: float,
    noise_w: float,
    pause_ms: int,
    output_sample_rate: int = 8000,
    namespace: str = "",
) -> tuple[bytes, dict, bool]:
    full_text = "".join(
        content if ptype == "fixed" else str(row.get(content, f"{{{content}}}"))
        for ptype, content in template_parts
    )

    if len(template_parts) == 1 and template_parts[0][0] == "var":
        return _generate_cached(
            full_text, voice_name, model_path, config_path,
            length_scale, noise_scale, noise_w, pause_ms, output_sample_rate, namespace=namespace,
        )

    key = cache.cache_key(full_text, voice_name, length_scale, noise_scale, noise_w, pause_ms, output_sample_rate, namespace=namespace)
    cached = cache.get(key, namespace=namespace)
    if cached:
        return cached, {"total_ms": 0, "model_inference_ms": 0, "segments": 0, "cache_hits": 0, "cache_misses": 0}, True

    if voice_name.startswith("kokoro:"):
        speed = round(1.0 / max(length_scale, 0.5), 3)
        wav, timing = kokoro_engine.generate_audio_from_template(
            template_parts, row, voice_name, speed=speed, output_sample_rate=output_sample_rate,
            namespace=namespace,
        )
    else:
        wav, timing = tts_engine.generate_audio_from_template(
            model_path, template_parts, row, voice_name,
            config_path, length_scale, noise_scale, noise_w, pause_ms, output_sample_rate,
            namespace=namespace,
        )
    cache.put(key, wav, namespace=namespace)
    return wav, timing, False


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return FileResponse("static/index.html")


@app.get("/voices")
async def get_voices():
    piper = [v for v in tts_engine.list_available_voices(str(MODELS_DIR))
             if "kokoro" not in Path(v["path"]).parts]
    for v in piper:
        v.setdefault("engine", "piper")
    kokoro = kokoro_engine.list_voices()
    return piper + kokoro


@app.post("/tts/single")
async def tts_single(
    text: Annotated[str, Form()],
    voice: Annotated[str, Form()],
    length_scale: Annotated[float, Form()] = 0.95,
    noise_scale: Annotated[float, Form()] = 0.85,
    noise_w: Annotated[float, Form()] = 0.9,
    pause_ms: Annotated[int, Form()] = 150,
    output_sample_rate: Annotated[int, Form()] = 8000,
):
    t_req_start = time.perf_counter()
    model_path, config_path = _resolve_voice(voice)

    loop = asyncio.get_event_loop()
    wav_bytes, timing, from_cache = await loop.run_in_executor(
        executor,
        _generate_cached,
        text, voice, model_path, config_path,
        length_scale, noise_scale, noise_w, pause_ms, output_sample_rate,
    )

    play_key = uuid.uuid4().hex[:16]
    _play_store[play_key] = wav_bytes
    t_response_ready = time.perf_counter()

    return JSONResponse({
        "play_key": play_key,
        "from_cache": from_cache,
        "timing": {
            "total_ms": timing["total_ms"],
            "inference_ms": timing["model_inference_ms"],
            "segments": timing.get("segments", 1),
            "cache_hits": timing.get("cache_hits", 0),
            "cache_misses": timing.get("cache_misses", 0),
            "server_total_ms": round((t_response_ready - t_req_start) * 1000, 2),
        },
    })


@app.get("/tts/play/{key}")
async def tts_play(key: str):
    wav = _play_store.get(key)
    if wav is None:
        raise HTTPException(status_code=404, detail="Audio no encontrado")
    return StreamingResponse(io.BytesIO(wav), media_type="audio/wav",
                             headers={"Accept-Ranges": "bytes"})


# ── API v1 — producción ───────────────────────────────────────────────────────

_ID_RE   = re.compile(r"[A-Za-z0-9._\-]{1,128}")
_UUID_RE = re.compile(r"[0-9a-fA-F]{32}")


def _safe_component(value: str, label: str) -> str:
    value = (value or "").strip()
    if value in (".", "..") or not _ID_RE.fullmatch(value):
        raise HTTPException(status_code=400, detail=f"{label} inválido: use solo letras, números, '.', '_' o '-'")
    return value


def _campaign_dir(client_id: str, campaign_id: str) -> Path:
    c  = _safe_component(client_id,  "client_id")
    ca = _safe_component(campaign_id, "campaign_id")
    d  = (STORAGE_DIR / c / ca).resolve()
    root = STORAGE_DIR.resolve()
    if root != d and root not in d.parents:
        raise HTTPException(status_code=400, detail="Ruta de campaña inválida")
    return d


def _generate_row_to_disk(
    i: int, row: dict, template_parts: list, voice: str,
    model_path: str, config_path: Optional[str],
    length_scale: float, noise_scale: float, noise_w: float, pause_ms: int,
    namespace: str, audios_dir: Path, phone_col: Optional[str], include_text: bool,
) -> dict:
    full_text = "".join(
        content if ptype == "fixed" else str(row.get(content, ""))
        for ptype, content in template_parts
    )
    phone = str(row.get(phone_col, "")).strip() if phone_col else ""

    pcm_key = cache.cache_key(full_text, voice, length_scale, noise_scale, noise_w, pause_ms, 8000, namespace=namespace)
    mulaw = cache.get_mulaw(pcm_key, namespace=namespace)
    if mulaw is not None:
        from_cache = True
        timing_ms  = 0
    else:
        wav_pcm, timing, from_cache = _generate_batch_row_cached(
            template_parts, row, voice, model_path, config_path,
            length_scale, noise_scale, noise_w, pause_ms, 8000, namespace=namespace,
        )
        mulaw = audio_format.to_mulaw_wav(wav_pcm)
        cache.put_mulaw(pcm_key, mulaw, namespace=namespace)
        timing_ms = timing.get("total_ms", 0)

    audio_id = uuid.uuid4().hex
    out_path  = audios_dir / f"{audio_id}.wav"
    threading.Thread(target=out_path.write_bytes, args=(mulaw,), daemon=True).start()

    # μ-law a 8000 Hz: 1 byte = 1 muestra = 0.125 ms; header WAV = 44 bytes
    duration_ms = (len(mulaw) - 44) * 1000 // 8000

    entry = {
        "audio_id":    audio_id,
        "row_index":   i + 1,
        "phone":       phone,
        "status":      "ok",
        "duration_ms": duration_ms,
        "size_bytes":  len(mulaw),
        "from_cache":  from_cache,
        "timing_ms":   timing_ms,
        "text_hash":   hashlib.sha256(full_text.encode()).hexdigest()[:16],
    }
    if include_text:
        entry["text"] = full_text
    return entry


@app.get("/v1/audio/{client_id}/{campaign_id}/{audio_id}")
async def v1_get_audio(client_id: str, campaign_id: str, audio_id: str):
    if not _UUID_RE.fullmatch(audio_id):
        raise HTTPException(status_code=400, detail="audio_id inválido")
    campaign_dir = _campaign_dir(client_id, campaign_id)
    path = (campaign_dir / "audios" / f"{audio_id}.wav").resolve()
    if STORAGE_DIR.resolve() not in path.parents:
        raise HTTPException(status_code=400, detail="Ruta inválida")
    if not path.exists():
        raise HTTPException(status_code=404, detail="Audio no encontrado")
    return FileResponse(str(path), media_type="audio/wav", filename=f"{audio_id}.wav")


@app.post("/v1/batch/run")
async def v1_batch_run(request: Request):
    """Endpoint principal para el worker .NET.

    Body: { jobId, clientId, voiceName, pendingAudio: [{recipient, text}, ...],
            lengthScale?, noiseScale?, noiseW?, pauseMs?, includeText? }
    """
    body = await request.json()

    client_id = _safe_component(body.get("clientId", ""), "clientId")
    job_id    = body.get("jobId", "").strip() or uuid.uuid4().hex[:12]
    voice     = body.get("voiceName", "")
    pending   = body.get("pendingAudio", [])

    if not client_id:
        raise HTTPException(status_code=400, detail="clientId requerido")
    if not voice:
        raise HTTPException(status_code=400, detail="voiceName requerido")
    if not pending:
        raise HTTPException(status_code=400, detail="pendingAudio vacío")

    model_path, config_path = _resolve_voice(voice)

    length_scale = float(body.get("lengthScale", 0.95))
    noise_scale  = float(body.get("noiseScale",  0.85))
    noise_w      = float(body.get("noiseW",      0.9))
    pause_ms     = int(body.get("pauseMs",        150))
    include_text = bool(body.get("includeText",   False))

    campaign_id = (body.get("campaignId") or "").strip()

    rows           = [{"_text_": str(item.get("text", "")), "_phone_": str(item.get("recipient", ""))} for item in pending]
    template_parts = [("var", "_text_")]
    namespace      = _safe_component(client_id, "clientId")
    if campaign_id:
        namespace = f"{namespace}|{_safe_component(campaign_id, 'campaignId')}"

    audios_dir = _campaign_dir(client_id, job_id) / "audios"
    audios_dir.mkdir(parents=True, exist_ok=True)

    loop = asyncio.get_event_loop()
    sem  = asyncio.Semaphore(BATCH_CONCURRENCY)
    t0   = time.perf_counter()

    async def process(i: int, row: dict):
        async with sem:
            try:
                return await loop.run_in_executor(
                    executor, _generate_row_to_disk,
                    i, row, template_parts, voice, model_path, config_path,
                    length_scale, noise_scale, noise_w, pause_ms,
                    namespace, audios_dir, "_phone_", include_text,
                )
            except Exception as e:
                return {"row_index": i + 1, "phone": row.get("_phone_", ""), "status": "error", "error": str(e)}

    audios   = await asyncio.gather(*[process(i, row) for i, row in enumerate(rows)])
    total_ms = round((time.perf_counter() - t0) * 1000, 2)

    ok       = [a for a in audios if a.get("status") == "ok"]
    base_url = f"/v1/audio/{client_id}/{job_id}"
    for a in ok:
        a["retrieval_url"] = f"{base_url}/{a['audio_id']}"
        a["recipient"]     = a.pop("phone", "")

    return JSONResponse({
        "jobId":    job_id,
        "clientId": client_id,
        "summary": {
            "total":      len(rows),
            "successful": len(ok),
            "errors":     len(rows) - len(ok),
            "from_cache": sum(1 for a in ok if a.get("from_cache")),
            "total_ms":   total_ms,
            "avg_ms":     round(total_ms / len(rows), 2) if rows else 0,
        },
        "audios": audios,
    })


@app.get("/cache/stats")
async def cache_stats():
    return cache.stats()


@app.delete("/cache/clear")
async def cache_clear():
    n = cache.clear()
    return {"deleted": n}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
