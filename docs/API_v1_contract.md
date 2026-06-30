# Contrato HTTP `/v1` — servicio TTS para el worker .NET

Servicio de síntesis de voz (FastAPI, Python). El worker .NET es responsable de:
ingestar el Excel, **partirlo en lotes de ~500 filas**, manejar RabbitMQ / reintentos /
progreso, y llamar a estos endpoints. El servicio Python es **stateless respecto a la cola**:
solo sintetiza, guarda en disco y responde.

Base URL: `http://{host}:8000`

---

## 1. Generar un lote — `POST /v1/batch/generate`

`multipart/form-data`:

| Campo | Tipo | Req. | Descripción |
|---|---|---|---|
| `file` | archivo | sí | CSV o XLSX del lote (≤ ~500 filas). 1ª fila = encabezados. |
| `template` | texto | sí | Plantilla. Para el caso actual: `{Message}` (toda la frase en una columna `Message`). |
| `voice` | texto | sí | Nombre de la voz (ver `GET /voices`). Ej. `es_MX-claude-high` o `kokoro:ef_dora`. |
| `client_id` | texto | sí | Identificador del cliente. Namespacing de caché + carpeta. `[A-Za-z0-9._-]`, máx 128. |
| `campaign_id` | texto | sí | Identificador de campaña. Mismo formato. |
| `phone_column` | texto | no | Nombre de la columna de teléfono. Si se omite, se autodetecta (`telefono`, `phone`, `celular`, `msisdn`…). |
| `batch_id` | texto | no | Id del lote. Si se omite, lo genera el servidor. Úsalo para reconciliar reintentos. |
| `length_scale` | float | no | Velocidad (default 0.95). |
| `noise_scale` | float | no | Variación tonal (default 0.85). |
| `noise_w` | float | no | Variación de ritmo (default 0.9). |
| `pause_ms` | int | no | Pausa entre segmentos (default 150). |
| `include_text` | bool | no | Incluir el texto sintetizado en el manifiesto (default true; es PII). |

### Respuesta `200` — manifiesto JSON

```json
{
  "client_id": "clienteA",
  "campaign_id": "camp01",
  "batch_id": "lote1",
  "created_at": "2026-06-30T15:04:05.123456+00:00",
  "voice": { "name": "es_MX-claude-high", "engine": "piper", "lang_code": null },
  "audio_format": { "codec": "pcm_mulaw", "container": "wav", "sample_rate": 8000, "channels": 1, "bits": 8, "bitrate": 64000 },
  "params": { "length_scale": 0.95, "noise_scale": 0.85, "noise_w": 0.9, "pause_ms": 150 },
  "phone_column": "telefono",
  "summary": { "total": 500, "successful": 499, "errors": 1, "from_cache": 120, "total_ms": 84210.5, "avg_ms": 168.42 },
  "retrieval_url_template": "/v1/audio/clienteA/camp01/{audio_id}",
  "audios": [
    {
      "audio_id": "7d62c6c9c23942baa9549fb0c048a921",
      "row_index": 1,
      "phone": "5512345678",
      "status": "ok",
      "filename": "7d62c6c9c23942baa9549fb0c048a921.wav",
      "duration_ms": 12201,
      "size_bytes": 97666,
      "sha256": "…",
      "from_cache": false,
      "timing_ms": 354.07,
      "text_hash": "a1b2c3d4e5f6a7b8",
      "text": "JOSE DEJA DE ESCONDERTE, …",
      "retrieval_url": "/v1/audio/clienteA/camp01/7d62c6c9c23942baa9549fb0c048a921"
    },
    { "row_index": 2, "phone": "5512345679", "status": "error", "error": "…" }
  ]
}
```

- Las filas con `status:"error"` **no tienen** `audio_id` ni archivo — el worker .NET puede reintentar solo esas.
- El audio físico ya está en disco cuando la respuesta llega; se puede recuperar de inmediato.
- Todos los audios de una campaña comparten carpeta y caché: si repites el lote, los audios pegan en caché.

### Errores
- `400` archivo vacío / id inválido (con `/`, vacío, etc.) / voz no encontrada.
- `404` voz inexistente.

---

## 2. Recuperar un audio — `GET /v1/audio/{client_id}/{campaign_id}/{audio_id}`

Devuelve el WAV μ-law (`Content-Type: audio/wav`, `pcm_mulaw 8000 Hz mono 8-bit`).

- `audio_id` debe ser el UUID de 32 hex del manifiesto.
- Validado contra path-traversal (cualquier `/` o `..` en los ids → `400`).
- `404` si no existe.

---

## Formato de audio de salida

WAV con `wFormatTag = 0x0007` (WAVE_FORMAT_MULAW), 8000 Hz, mono, 8 bits/muestra, 64 kb/s.
Equivale a la línea de ffprobe:
`Audio: pcm_mulaw ([7][0][0][0] / 0x0007), 8000 Hz, mono, s16 (8 bit), 64 kb/s`.

---

## Estructura de almacenamiento

```
tts_output/
├── clienteA/
│   ├── campaniaA/
│   │   └── audios/
│   │       ├── audio_id_1.wav
│   │       ├── audio_id_2.wav
│   │       └── ...
│   └── campaniaB/
│       └── audios/
│           ├── audio_id_n.wav
│           └── ...
└── clienteB/
    └── campaniaA/
        └── audios/
            └── ...
```

---

## Notas para el worker .NET

- **Tamaño de lote:** ~500 filas por llamada. El caché de segmentos es por `client_id|campaign_id`
  y persiste entre lotes de la misma campaña: el **lote 2 reutiliza** las cláusulas fijas del lote 1
  (más rápido). Mantener los lotes de una campaña apuntando al mismo servicio maximiza el reuso.
- **Pipeline:** mientras se genera el lote N+1, se pueden recuperar/entregar los audios del lote N
  (ya están en disco y el manifiesto del lote N ya fue devuelto).
- **Aislamiento:** el caché de (clienteA, campA) nunca pega en (clienteA, campB) ni en otro cliente.
- **Reintentos / idempotencia:** reenviar un lote genera nuevos `audio_id` (un manifiesto nuevo).
  Para evitar duplicados, .NET debe trackear qué `batch_id` completaron. Mejora futura: aceptar un
  `external_id` por fila para deduplicar a nivel de servicio.
- **Escalado horizontal:** se pueden correr **N instancias** del servicio Python; comparten el caché
  en disco local por campaña, así que se reusan entre sí. .NET reparte lotes entre instancias.
- **Variables de entorno:** `TTS_STORAGE_DIR` (salida de audios), `TTS_CACHE_DIR` (caché),
  `TTS_BATCH_CONCURRENCY` (inferencias en paralelo, default 6). Ninguna debe apuntar a OneDrive.
