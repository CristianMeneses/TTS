# Lanza N consumidores de síntesis TTS (workers que compiten por la cola tts.tasks).
#
#   .\run_workers.ps1            # 3 workers (default)
#   .\run_workers.ps1 -N 4       # 4 workers
#
# Cada worker es un proceso Python independiente con su propio modelo Piper cargado.
# Correr desde la raíz del proyecto (necesita ./models y los módulos .py).
# Ctrl+C en cada ventana para detenerlos.

param(
    [int]$N = 3
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot

Write-Host "Lanzando $N workers de síntesis TTS..." -ForegroundColor Cyan

for ($i = 1; $i -le $N; $i++) {
    Start-Process -FilePath "python" `
        -ArgumentList "tts_consumer.py" `
        -WorkingDirectory $root `
        -WindowStyle Normal
    Write-Host "  worker $i lanzado" -ForegroundColor Green
    Start-Sleep -Milliseconds 400   # escalona el arranque (carga de modelo)
}

Write-Host "`n$N workers corriendo en ventanas separadas." -ForegroundColor Cyan
Write-Host "Míralos conectados en la UI de RabbitMQ: http://localhost:15672 (Connections)."
