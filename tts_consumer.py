"""Consumidor de síntesis TTS (worker distribuido).

Cada proceso es un worker independiente que:
  1. Consume tareas de la cola `tts.tasks` (1 tarea = 1 texto único a sintetizar).
  2. Sintetiza el audio (reusando tts_engine/kokoro_engine/cache/audio_format).
  3. Escribe el μ-law en STORAGE_DIR/{clientId}/{jobId}/audios/{audioId}.wav
     (donde el API `main.py` lo sirve vía GET /v1/audio).
  4. Publica el resultado en `tts.results`.

Correr N procesos (ver run_workers.ps1) = N workers compitiendo por la misma cola,
con reparto justo (prefetch=1). En varias máquinas: el mismo código, apuntando al
mismo RabbitMQ y a un STORAGE_DIR compartido.

Contrato de mensajes:
  tts.tasks   (in):  { jobId, clientId, campaignId, audioId, voiceName,
                       text, lengthScale, noiseScale, noiseW, pauseMs }
  tts.results (out): { jobId, audioId, status, durationMs, sizeBytes, fromCache, error? }
"""

import json
import os
import signal
import sys
import threading
from pathlib import Path

import pika

# La consola de Windows (cp1252) revienta con caracteres no-ASCII. Forzamos UTF-8.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import audio_format
import cache
import kokoro_engine
import storage
import tts_engine

MODELS_DIR = Path("models")

# ── Config (env con defaults alineados a worker/appsettings.json) ─────────────
RMQ_HOST  = os.environ.get("TTS_RMQ_HOST", "localhost")
RMQ_PORT  = int(os.environ.get("TTS_RMQ_PORT", "5672"))
RMQ_USER  = os.environ.get("TTS_RMQ_USER", "guest")
RMQ_PASS  = os.environ.get("TTS_RMQ_PASS", "guest")
TASKS_Q   = os.environ.get("TTS_TASKS_QUEUE",   "tts.tasks")
RESULTS_Q = os.environ.get("TTS_RESULTS_QUEUE", "tts.results")

WORKER_ID = f"tts-{os.getpid()}"

_voice_map: dict[str, tuple[str, str | None]] = {}


def _log(msg: str) -> None:
    print(f"[{WORKER_ID}] {msg}", flush=True)


# ── Carga de voces (igual que el warmup de main.py) ──────────────────────────

def load_voices() -> None:
    for v in tts_engine.list_available_voices(str(MODELS_DIR)):
        if v["ready"]:
            try:
                tts_engine.load_voice(v["path"], v["config"])
                _voice_map[v["name"]] = (v["path"], v["config"])
                _log(f"voz Piper cargada: {v['name']}")
            except Exception as e:
                _log(f"error cargando {v['name']}: {e}")
    if kokoro_engine.is_available():
        try:
            kokoro_engine.load_kokoro()
            _log("Kokoro cargado")
        except Exception as e:
            _log(f"Kokoro error: {e}")


def resolve_voice(voice_name: str) -> tuple[str, str | None]:
    if voice_name.startswith("kokoro:"):
        return "", None
    if voice_name in _voice_map:
        return _voice_map[voice_name]
    raise ValueError(f"Voz '{voice_name}' no encontrada")


# ── Síntesis de una tarea (espeja la lógica de main._generate_row_to_disk) ────

def handle_task(task: dict) -> dict:
    text        = str(task.get("text", ""))
    voice       = task.get("voiceName", "")
    client_id   = task.get("clientId", "")
    campaign_id = (task.get("campaignId") or "").strip()
    job_id      = task.get("jobId", "")
    audio_id    = task.get("audioId", "")
    length_scale = float(task.get("lengthScale", 0.95))
    noise_scale  = float(task.get("noiseScale",  0.85))
    noise_w      = float(task.get("noiseW",      0.9))
    pause_ms     = int(task.get("pauseMs",        150))

    # El audioId se usa como nombre de archivo → validar que sea un uuid hex (32
    # chars) evita path traversal si llega un mensaje manipulado/basura a la cola.
    if not storage.UUID_RE.fullmatch(audio_id):
        raise ValueError(f"audioId inválido: {audio_id!r}")

    model_path, config_path = resolve_voice(voice)

    # Namespace del caché: aísla TODO (segmentos, PCM y μ-law) por cliente|campaña.
    # Mismo cálculo/validación que main.py. Si no viene campaignId, se usa job_id como
    # componente para que NUNCA degrade a "por cliente" (campañas de un mismo cliente
    # no comparten caché entre sí).
    namespace = f"{storage.safe_component(client_id, 'clientId')}|{storage.safe_component(campaign_id or job_id, 'campaignId')}"

    key = cache.cache_key(text, voice, length_scale, noise_scale, noise_w, pause_ms, 8000, namespace=namespace)
    mulaw = cache.get_mulaw(key, namespace=namespace)
    from_cache = mulaw is not None

    if mulaw is None:
        pcm = cache.get(key, namespace=namespace)
        if pcm is None:
            if voice.startswith("kokoro:"):
                voice_id = voice[7:]
                speed = round(1.0 / max(length_scale, 0.5), 3)
                pcm, _ = kokoro_engine.generate_audio(
                    text=text, voice_id=voice_id, speed=speed,
                    pause_ms=pause_ms, output_sample_rate=8000, namespace=namespace,
                )
            else:
                pcm, _ = tts_engine.generate_audio(
                    model_path=model_path, text=text, voice_name=voice,
                    config_path=config_path, length_scale=length_scale,
                    noise_scale=noise_scale, noise_w=noise_w, pause_ms=pause_ms,
                    output_sample_rate=8000, namespace=namespace,
                )
            cache.put(key, pcm, namespace=namespace)
        mulaw = audio_format.to_mulaw_wav(pcm)
        cache.put_mulaw(key, mulaw, namespace=namespace)

    # Escribe donde el API sirve el audio: el "bucket" de ruta es el jobId.
    adir = storage.audios_dir(client_id, job_id)
    adir.mkdir(parents=True, exist_ok=True)
    (adir / f"{audio_id}.wav").write_bytes(mulaw)

    # μ-law 8000 Hz: 1 byte = 1 muestra = 0.125 ms; header WAV = 44 bytes.
    duration_ms = (len(mulaw) - 44) * 1000 // 8000

    return {
        "jobId":      job_id,
        "audioId":    audio_id,
        "status":     "ok",
        "durationMs": duration_ms,
        "sizeBytes":  len(mulaw),
        "fromCache":  from_cache,
    }


# ── Loop de consumo ──────────────────────────────────────────────────────────

def _publish_result(ch, result: dict) -> None:
    ch.basic_publish(
        exchange="",
        routing_key=RESULTS_Q,
        body=json.dumps(result).encode("utf-8"),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )


def on_task(ch, method, _props, body):
    task = {}
    try:
        task = json.loads(body)
        result = handle_task(task)
        cache_tag = "cache" if result["fromCache"] else "synth"
        _log(f"ok {result['audioId'][:8]} ({cache_tag}, {result['durationMs']}ms)")
    except Exception as e:  # noqa: BLE001 — no reventamos el worker
        # Reintento único ante fallo transitorio (modelo bloqueado, disco lleno un
        # instante, etc.): si el mensaje aún no fue reentregado, lo devolvemos a la
        # cola una vez. Si ya venía reentregado, nos rendimos: publicamos el error y
        # hacemos ack para no dejar un poison message reciclándose para siempre.
        if not method.redelivered:
            _log(f"fallo tarea {task.get('audioId')}: {e} — reintentando (1)")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return
        result = {
            "jobId":   task.get("jobId"),
            "audioId": task.get("audioId"),
            "status":  "error",
            "error":   str(e),
        }
        _log(f"ERROR tarea {task.get('audioId')} (tras reintento): {e}")
    _publish_result(ch, result)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    _log(f"iniciando; RabbitMQ={RMQ_HOST}:{RMQ_PORT} colas={TASKS_Q}->{RESULTS_Q}")
    load_voices()

    params = pika.ConnectionParameters(
        host=RMQ_HOST, port=RMQ_PORT,
        credentials=pika.PlainCredentials(RMQ_USER, RMQ_PASS),
        heartbeat=600, blocked_connection_timeout=300,
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=TASKS_Q,   durable=True)
    ch.queue_declare(queue=RESULTS_Q, durable=True)
    ch.basic_qos(prefetch_count=1)  # reparto justo entre workers
    ch.basic_consume(queue=TASKS_Q, on_message_callback=on_task, auto_ack=False)

    # Cierre limpio con Ctrl+C.
    def _stop(*_):
        _log("deteniendo…")
        try:
            ch.stop_consuming()
        except Exception:
            pass
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    _log("listo, esperando tareas")
    try:
        ch.start_consuming()
    finally:
        try:
            conn.close()
        except Exception:
            pass
    _log("terminado")


if __name__ == "__main__":
    main()
