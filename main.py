import asyncio
import csv
import io
import json
import statistics
import threading
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Optional

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

import cache
import tts_engine

MODELS_DIR = Path("models")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
MODELS_DIR.mkdir(parents=True, exist_ok=True)

executor = ThreadPoolExecutor(max_workers=8)

# Mapa en memoria: nombre_voz → (model_path, config_path)
_voice_map: dict[str, tuple[str, Optional[str]]] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    voices = tts_engine.list_available_voices(str(MODELS_DIR))
    for v in voices:
        if v["ready"]:
            try:
                tts_engine.load_voice(v["path"], v["config"])
                _voice_map[v["name"]] = (v["path"], v["config"])
                print(f"[warmup] Voz cargada: {v['name']}")
            except Exception as e:
                print(f"[warmup] Error cargando {v['name']}: {e}")
    yield


app = FastAPI(title="Piper TTS API", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


def _resolve_voice(voice_name: str) -> tuple[str, Optional[str]]:
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
) -> tuple[bytes, dict, bool]:
    key = cache.cache_key(text, voice_name, length_scale, noise_scale, noise_w, pause_ms, output_sample_rate)
    cached = cache.get(key)
    if cached:
        return cached, {"total_ms": 0, "model_inference_ms": 0, "file_write_ms": 0, "segments": 0, "cache_hits": 0, "cache_misses": 0}, True

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
    )
    cache.put(key, wav)
    return wav, timing, False


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return FileResponse("static/index.html")


@app.get("/voices")
async def get_voices():
    return tts_engine.list_available_voices(str(MODELS_DIR))


@app.post("/tts/single")
async def tts_single(
    text: Annotated[str, Form()],
    voice: Annotated[str, Form()],
    length_scale: Annotated[float, Form()] = 0.95,
    noise_scale: Annotated[float, Form()] = 0.85,
    noise_w: Annotated[float, Form()] = 0.9,
    pause_ms: Annotated[int, Form()] = 150,
    output_sample_rate: Annotated[int, Form()] = 8000,
    download: Annotated[bool, Form()] = False,
):
    model_path, config_path = _resolve_voice(voice)

    loop = asyncio.get_event_loop()
    wav_bytes, timing, from_cache = await loop.run_in_executor(
        executor,
        _generate_cached,
        text, voice, model_path, config_path,
        length_scale, noise_scale, noise_w, pause_ms, output_sample_rate,
    )

    filename = f"{voice}_{uuid.uuid4().hex[:8]}.wav"
    out_path = OUTPUT_DIR / filename

    if download:
        await loop.run_in_executor(executor, out_path.write_bytes, wav_bytes)
        return FileResponse(str(out_path), media_type="audio/wav", filename=filename)

    # Escribir a disco en thread separado — nunca bloquea el event loop
    threading.Thread(target=out_path.write_bytes, args=(wav_bytes,), daemon=True).start()

    headers = {
        "X-Timing-Total-Ms": str(timing["total_ms"]),
        "X-Timing-Inference-Ms": str(timing["model_inference_ms"]),
        "X-From-Cache": str(from_cache).lower(),
        "X-Cache-Hits": str(timing.get("cache_hits", 0)),
        "X-Cache-Misses": str(timing.get("cache_misses", 0)),
        "X-Segments": str(timing.get("segments", 1)),
        "X-Filename": filename,
    }
    return StreamingResponse(io.BytesIO(wav_bytes), media_type="audio/wav", headers=headers)


@app.get("/tts/download/{filename}")
async def tts_download(filename: str):
    path = OUTPUT_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="Archivo no encontrado")
    return FileResponse(str(path), media_type="audio/wav", filename=filename)


@app.post("/tts/batch")
async def tts_batch(
    template: Annotated[str, Form()],
    csv_file: Annotated[UploadFile, File()],
    voice: Annotated[str, Form()],
    length_scale: Annotated[float, Form()] = 0.95,
    noise_scale: Annotated[float, Form()] = 0.85,
    noise_w: Annotated[float, Form()] = 0.9,
    pause_ms: Annotated[int, Form()] = 150,
    output_sample_rate: Annotated[int, Form()] = 8000,
):
    model_path, config_path = _resolve_voice(voice)

    content = await csv_file.read()
    for encoding in ("utf-8-sig", "latin-1", "cp1252"):
        try:
            text = content.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise HTTPException(status_code=400, detail="No se pudo leer el CSV")
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        raise HTTPException(status_code=400, detail="CSV vacío")

    template_parts = tts_engine.parse_template(template)
    job_id = uuid.uuid4().hex[:10]

    t_batch_start = time.perf_counter()
    loop = asyncio.get_event_loop()

    async def process_row(i: int, row: dict):
        try:
            wav_bytes, timing = await loop.run_in_executor(
                executor,
                tts_engine.generate_audio_from_template,
                model_path, template_parts, row, voice,
                config_path, length_scale, noise_scale, noise_w, pause_ms, output_sample_rate,
            )
        except Exception as e:
            return {"row": i + 1, "error": str(e)}

        first_val = list(row.values())[0] if row else str(i + 1)
        safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in first_val)
        filename = f"{i+1:03d}_{safe_name}.wav"
        return {"row": i + 1, "file": filename, "timing_ms": timing["total_ms"], "_wav": wav_bytes}

    results = await asyncio.gather(*[process_row(i, row) for i, row in enumerate(rows)])

    successful = [r for r in results if "error" not in r]
    avg_ms = round(statistics.mean(r["timing_ms"] for r in successful), 2) if successful else 0

    # Separar bytes del resumen antes de serializar
    rows_summary = [{k: v for k, v in r.items() if k != "_wav"} for r in results]

    t_batch_end = time.perf_counter()
    total_ms = round((t_batch_end - t_batch_start) * 1000, 2)

    summary_data = {
        "job_id": job_id,
        "total_rows": len(rows),
        "successful": len(successful),
        "errors": len(results) - len(successful),
        "total_wall_ms": total_ms,
        "avg_per_audio_ms": round(total_ms / len(successful), 2) if successful else 0,
        "avg_inference_ms": avg_ms,
        "rows": rows_summary,
    }

    # Construir ZIP en executor — la compresión DEFLATED es CPU-intensiva y bloquearía el event loop
    summary_json = json.dumps(summary_data, ensure_ascii=False, indent=2)

    def _build_zip():
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for r in results:
                if "_wav" in r:
                    zf.writestr(r["file"], r["_wav"])
            zf.writestr("summary.json", summary_json)
        buf.seek(0)
        return buf

    zip_buf = await loop.run_in_executor(executor, _build_zip)

    return StreamingResponse(
        zip_buf,
        media_type="application/zip",
        headers={
            "Content-Disposition": f"attachment; filename=batch_{job_id}.zip",
            "X-Job-Id": job_id,
            "X-Total-Ms": str(total_ms),
            "X-Audio-Count": str(len(successful)),
        },
    )


def _warmup_segment(
    phrase: str,
    voice: str,
    model_path: str,
    config_path: Optional[str],
    length_scale: float,
    noise_scale: float,
    noise_w: float,
) -> tuple[bool, float]:
    """Sintetiza una frase y la guarda como segmento. Devuelve (from_cache, ms)."""
    key = cache.segment_key(phrase, voice, length_scale, noise_scale, noise_w)
    if cache.get_segment(key):
        return True, 0.0
    from piper.config import SynthesisConfig
    voice_obj = tts_engine.load_voice(model_path, config_path)
    syn_config = SynthesisConfig(length_scale=length_scale, noise_scale=noise_scale, noise_w_scale=noise_w)
    t0 = time.perf_counter()
    import io, wave
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(voice_obj.config.sample_rate)
        for chunk in voice_obj.synthesize(phrase, syn_config):
            wf.writeframes(chunk.audio_int16_bytes)
    wav = buf.getvalue()
    ms = round((time.perf_counter() - t0) * 1000, 2)
    cache.put_segment(key, wav)
    return False, ms


@app.post("/warmup")
async def warmup(
    phrases: Annotated[str, Form()],
    voice: Annotated[str, Form()],
    length_scale: Annotated[float, Form()] = 0.95,
    noise_scale: Annotated[float, Form()] = 0.85,
    noise_w: Annotated[float, Form()] = 0.9,
):
    model_path, config_path = _resolve_voice(voice)
    try:
        phrase_list: list[str] = json.loads(phrases)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="'phrases' debe ser un JSON array de strings")

    loop = asyncio.get_event_loop()
    results = []
    for phrase in phrase_list:
        from_cache, ms = await loop.run_in_executor(
            executor, _warmup_segment,
            phrase, voice, model_path, config_path, length_scale, noise_scale, noise_w,
        )
        results.append({
            "phrase": phrase,
            "status": "already_cached" if from_cache else "cached",
            "ms": ms,
        })

    return {"warmed": results}


@app.post("/warmup/csv")
async def warmup_csv(
    csv_file: Annotated[UploadFile, File()],
    column: Annotated[str, Form()],
    voice: Annotated[str, Form()],
    length_scale: Annotated[float, Form()] = 0.95,
    noise_scale: Annotated[float, Form()] = 0.85,
    noise_w: Annotated[float, Form()] = 0.9,
):
    """Precalienta todos los valores de una columna de un CSV como segmentos."""
    model_path, config_path = _resolve_voice(voice)

    content = await csv_file.read()
    for encoding in ("utf-8-sig", "latin-1", "cp1252"):
        try:
            text = content.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise HTTPException(status_code=400, detail="No se pudo leer el CSV, guárdalo en UTF-8 o Latin-1")
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        raise HTTPException(status_code=400, detail="CSV vacío")
    if column not in rows[0]:
        available = list(rows[0].keys())
        raise HTTPException(status_code=400, detail=f"Columna '{column}' no encontrada. Disponibles: {available}")

    phrases = list({row[column].strip() for row in rows if row[column].strip()})

    loop = asyncio.get_event_loop()
    results = []
    for phrase in phrases:
        from_cache, ms = await loop.run_in_executor(
            executor, _warmup_segment,
            phrase, voice, model_path, config_path, length_scale, noise_scale, noise_w,
        )
        results.append({
            "phrase": phrase,
            "status": "already_cached" if from_cache else "cached",
            "ms": ms,
        })

    cached_count = sum(1 for r in results if r["status"] == "cached")
    return {
        "total": len(phrases),
        "newly_cached": cached_count,
        "already_cached": len(phrases) - cached_count,
        "warmed": results,
    }


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
