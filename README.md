# Piper TTS API

API de síntesis de voz en español usando [Piper TTS](https://github.com/rhasspy/piper) + FastAPI, orientada a generación masiva para llamadas telefónicas.

## Requisitos

```bash
py -3 -m pip install -r requirements.txt
```

Descargar modelos `.onnx` + `.onnx.json` desde [rhasspy/piper-voices](https://huggingface.co/rhasspy/piper-voices) y colocarlos en `models/`.

## Uso

```bash
py -3 main.py
# → http://localhost:8000
```

## Características

- Audio individual y batch desde CSV con templates `{placeholder}`
- Caché dos niveles: RAM + disco (writes async, nunca bloquea)
- Salida a 8 kHz por defecto (modo telefonía, configurable)
- Paralelismo con 8 workers para batch
- Precalentamiento de segmentos frecuentes
