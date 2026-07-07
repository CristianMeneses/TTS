"""Marcador emulado (2º worker) — solo para pruebas locales.

En producción, otro worker consume la cola de salida y hace las llamadas reales.
Aquí lo emulamos: consumimos `dialer.output` y "llamamos" (log en consola + calls.log)
apenas llega cada DialerMessage. Como el orquestador publica por lote en streaming,
verás las llamadas del lote 1 mientras los lotes siguientes aún se sintetizan.

Ejecutar desde la raíz del proyecto:  python fake_dialer.py
"""

import json
import os
import signal
import sys
from datetime import datetime, timezone

import pika

# La consola de Windows usa cp1252 y revienta con emojis/flechas. Forzamos UTF-8.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

RMQ_HOST   = os.environ.get("TTS_RMQ_HOST", "localhost")
RMQ_PORT   = int(os.environ.get("TTS_RMQ_PORT", "5672"))
RMQ_USER   = os.environ.get("TTS_RMQ_USER", "guest")
RMQ_PASS   = os.environ.get("TTS_RMQ_PASS", "guest")
DIALER_Q   = os.environ.get("TTS_DIALER_QUEUE", "dialer.output")
CALLS_LOG  = os.environ.get("TTS_CALLS_LOG", "calls.log")

_count = 0


def on_message(ch, method, _props, body):
    global _count
    try:
        m = json.loads(body)
    except Exception:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    _count += 1
    phone    = m.get("phone", "?")
    url      = m.get("audio_url", "?")
    dur      = m.get("duration_ms", 0)
    batch    = m.get("batch_id", "?")
    line = f"📞 [{_count:>5}] llamando {phone:<15} → {url}  ({dur}ms)  lote={batch}"
    print(line, flush=True)

    ts = datetime.now(timezone.utc).isoformat()
    with open(CALLS_LOG, "a", encoding="utf-8") as f:
        f.write(f"{ts}\t{phone}\t{dur}\t{batch}\t{url}\n")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    print(f"[fake_dialer] RabbitMQ={RMQ_HOST}:{RMQ_PORT} cola={DIALER_Q} → {CALLS_LOG}", flush=True)
    params = pika.ConnectionParameters(
        host=RMQ_HOST, port=RMQ_PORT,
        credentials=pika.PlainCredentials(RMQ_USER, RMQ_PASS),
        heartbeat=600,
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=DIALER_Q, durable=True)
    ch.basic_qos(prefetch_count=20)
    ch.basic_consume(queue=DIALER_Q, on_message_callback=on_message, auto_ack=False)

    def _stop(*_):
        print(f"\n[fake_dialer] deteniendo — {_count} llamadas emuladas.", flush=True)
        try:
            ch.stop_consuming()
        except Exception:
            pass
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    print("[fake_dialer] esperando mensajes del marcador…", flush=True)
    try:
        ch.start_consuming()
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
