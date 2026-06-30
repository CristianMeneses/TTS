import asyncio
import csv
import io
import json
import os
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
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

import cache
import tts_engine
import kokoro_engine

MODELS_DIR = Path("models")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
MODELS_DIR.mkdir(parents=True, exist_ok=True)

# Concurrencia del batch. En CPU de 16 núcleos el throughput de Piper sube hasta ~6-8
# inferencias en paralelo (cada inferencia no satura todos los núcleos). Configurable
# con la variable de entorno TTS_BATCH_CONCURRENCY.
BATCH_CONCURRENCY = int(os.environ.get("TTS_BATCH_CONCURRENCY", "6"))
executor = ThreadPoolExecutor(max_workers=max(8, BATCH_CONCURRENCY + 2))

# Mapa en memoria: nombre_voz → (model_path, config_path)
_voice_map: dict[str, tuple[str, Optional[str]]] = {}

# Almacén temporal para audio listo: play_key → wav_bytes
_play_store: dict[str, bytes] = {}

# Almacén temporal para ZIPs generados en batch: zip_key → zip_bytes
_zip_store: dict[str, bytes] = {}


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
        return "", None  # not used by kokoro engine
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

    if voice_name.startswith("kokoro:"):
        voice_id = voice_name[7:]
        speed = round(1.0 / max(length_scale, 0.5), 3)
        wav, timing = kokoro_engine.generate_audio(
            text=text,
            voice_id=voice_id,
            speed=speed,
            pause_ms=pause_ms,
            output_sample_rate=output_sample_rate,
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
        )
    cache.put(key, wav)
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
) -> tuple[bytes, dict, bool]:
    """Versión de _generate_cached para batch: usa el texto ensamblado como clave de caché."""
    full_text = "".join(
        content if ptype == "fixed" else str(row.get(content, f"{{{content}}}"))
        for ptype, content in template_parts
    )

    # Caso "todo el mensaje en una columna" (template = solo {Message}): el texto ensamblado
    # es el valor completo de la columna → tratarlo como audio individual para reutilizar el
    # pipeline probado (caché de audio completo + split por puntuación + caché de segmentos + pausas).
    if len(template_parts) == 1 and template_parts[0][0] == "var":
        return _generate_cached(
            full_text, voice_name, model_path, config_path,
            length_scale, noise_scale, noise_w, pause_ms, output_sample_rate,
        )

    key = cache.cache_key(full_text, voice_name, length_scale, noise_scale, noise_w, pause_ms, output_sample_rate)
    cached = cache.get(key)
    if cached:
        return cached, {"total_ms": 0, "model_inference_ms": 0, "segments": 0, "cache_hits": 0, "cache_misses": 0}, True

    if voice_name.startswith("kokoro:"):
        speed = round(1.0 / max(length_scale, 0.5), 3)
        wav, timing = kokoro_engine.generate_audio_from_template(
            template_parts, row, voice_name, speed=speed, output_sample_rate=output_sample_rate,
        )
    else:
        wav, timing = tts_engine.generate_audio_from_template(
            model_path, template_parts, row, voice_name,
            config_path, length_scale, noise_scale, noise_w, pause_ms, output_sample_rate,
        )
    cache.put(key, wav)
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
    download: Annotated[bool, Form()] = False,
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
    t_executor_done = time.perf_counter()

    safe_voice = voice.replace(":", "_")
    filename = f"{safe_voice}_{uuid.uuid4().hex[:8]}.wav"
    out_path = OUTPUT_DIR / filename

    if download:
        await loop.run_in_executor(executor, out_path.write_bytes, wav_bytes)
        return FileResponse(str(out_path), media_type="audio/wav", filename=filename)

    # Guardar en memoria para servir por GET /tts/play/{key}
    play_key = uuid.uuid4().hex[:16]
    _play_store[play_key] = wav_bytes

    # Escribir a disco en thread separado — nunca bloquea el event loop
    threading.Thread(target=out_path.write_bytes, args=(wav_bytes,), daemon=True).start()

    t_response_ready = time.perf_counter()
    return JSONResponse({
        "play_key": play_key,
        "filename": filename,
        "from_cache": from_cache,
        "timing": {
            "total_ms": timing["total_ms"],
            "inference_ms": timing["model_inference_ms"],
            "segments": timing.get("segments", 1),
            "cache_hits": timing.get("cache_hits", 0),
            "cache_misses": timing.get("cache_misses", 0),
            # tiempo real del lado del servidor: incluye cache.get en disco, overhead, etc.
            "server_total_ms": round((t_response_ready - t_req_start) * 1000, 2),
            "queue_overhead_ms": round((t_executor_done - t_req_start) * 1000 - timing["total_ms"], 2),
        },
    })


@app.get("/tts/play/{key}")
async def tts_play(key: str):
    wav = _play_store.get(key)
    if wav is None:
        raise HTTPException(status_code=404, detail="Audio no encontrado")
    return StreamingResponse(io.BytesIO(wav), media_type="audio/wav",
                             headers={"Accept-Ranges": "bytes"})


@app.get("/tts/download/{filename}")
async def tts_download(filename: str):
    path = OUTPUT_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="Archivo no encontrado")
    return FileResponse(str(path), media_type="audio/wav", filename=filename)


def _read_rows(filename: str, content: bytes) -> list[dict]:
    """Lee un archivo tabular (CSV o XLSX) y devuelve filas como list[dict].
    La primera fila se toma como encabezados."""
    if filename.lower().endswith(".xlsx"):
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
        ws = wb.active
        rows_iter = ws.iter_rows(values_only=True)
        try:
            header_row = next(rows_iter)
        except StopIteration:
            return []
        headers = [str(h).strip() if h is not None else "" for h in header_row]
        result = []
        for r in rows_iter:
            if r is None or all(c is None for c in r):
                continue
            result.append({
                headers[i]: ("" if v is None else str(v))
                for i, v in enumerate(r) if i < len(headers) and headers[i]
            })
        return result

    # CSV: decodificar probando varios encodings comunes
    for encoding in ("utf-8-sig", "latin-1", "cp1252"):
        try:
            text = content.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise HTTPException(status_code=400, detail="No se pudo leer el CSV")
    return list(csv.DictReader(io.StringIO(text)))


def _build_zip_from_dict(wavs: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for filename, wav_bytes in wavs.items():
            zf.writestr(filename, wav_bytes)
    return buf.getvalue()


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
    include_zip: Annotated[bool, Form()] = False,
):
    model_path, config_path = _resolve_voice(voice)

    content = await csv_file.read()
    rows = _read_rows(csv_file.filename or "archivo.csv", content)
    if not rows:
        raise HTTPException(status_code=400, detail="Archivo vacío o sin filas de datos")

    template_parts = tts_engine.parse_template(template)
    job_id = uuid.uuid4().hex[:10]
    loop = asyncio.get_event_loop()
    t_batch_start = time.perf_counter()

    async def generate():
        # Primer evento: total de filas, para que la barra de progreso conozca el
        # denominador sin importar el formato del archivo (csv/xlsx).
        yield f"data: {json.dumps({'start': True, 'total': len(rows)})}\n\n"

        sem = asyncio.Semaphore(BATCH_CONCURRENCY)
        wavs_for_zip: dict[str, bytes] = {}
        completed = 0
        errors = 0
        queue: asyncio.Queue = asyncio.Queue()

        async def process_and_enqueue(i: int, row: dict):
            async with sem:
                try:
                    wav_bytes, timing, from_cache = await loop.run_in_executor(
                        executor,
                        _generate_batch_row_cached,
                        template_parts, row, voice, model_path, config_path,
                        length_scale, noise_scale, noise_w, pause_ms, output_sample_rate,
                    )
                    await queue.put(("ok", i, row, wav_bytes, timing, from_cache))
                except Exception as e:
                    await queue.put(("error", i, row, str(e)))

        tasks = [asyncio.ensure_future(process_and_enqueue(i, row)) for i, row in enumerate(rows)]

        for _ in range(len(rows)):
            item = await queue.get()
            if item[0] == "ok":
                _, i, row, wav_bytes, timing, from_cache = item
                play_key = uuid.uuid4().hex[:16]
                _play_store[play_key] = wav_bytes
                first_val = str(list(row.values())[0]) if row else str(i + 1)
                safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in first_val)[:30]
                filename = f"{i+1:03d}_{safe_name}.wav"
                if include_zip:
                    wavs_for_zip[filename] = wav_bytes
                event = json.dumps({
                    "row": i + 1,
                    "play_key": play_key,
                    "filename": filename,
                    "timing_ms": timing["total_ms"],
                    "from_cache": from_cache,
                })
                completed += 1
            else:
                _, i, row, err = item
                event = json.dumps({"row": i + 1, "error": err})
                errors += 1
            yield f"data: {event}\n\n"

        await asyncio.gather(*tasks, return_exceptions=True)
        t_total = round((time.perf_counter() - t_batch_start) * 1000, 2)

        done: dict = {
            "done": True,
            "job_id": job_id,
            "total": len(rows),
            "successful": completed,
            "errors": errors,
            "total_ms": t_total,
        }
        if include_zip and wavs_for_zip:
            zip_key = uuid.uuid4().hex[:16]
            zip_bytes = await loop.run_in_executor(executor, _build_zip_from_dict, wavs_for_zip)
            _zip_store[zip_key] = zip_bytes
            done["zip_key"] = zip_key
        yield f"data: {json.dumps(done)}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/tts/zip/{key}")
async def tts_zip(key: str):
    zip_bytes = _zip_store.pop(key, None)
    if zip_bytes is None:
        raise HTTPException(status_code=404, detail="ZIP no encontrado o ya descargado")
    return StreamingResponse(
        io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=batch_{key}.zip"},
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
