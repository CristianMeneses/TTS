# Levanta RabbitMQ (con UI de management) en Docker para desarrollo local.
#
#   .\infra\rabbitmq.ps1           # arranca (o reusa) el contenedor
#   .\infra\rabbitmq.ps1 -Stop     # lo detiene y elimina
#
# Requiere Docker Desktop CORRIENDO.
#   - AMQP:       localhost:5672   (lo usan el worker .NET y los consumidores Python)
#   - UI/mgmt:    http://localhost:15672   (usuario: guest / password: guest)
#
# El contenedor NO persiste datos entre recreaciones (las colas se redeclaran solas
# al arrancar cada servicio, así que no hace falta volumen para desarrollo).

param(
    [switch]$Stop
)

$ErrorActionPreference = "Stop"
$name = "tts-rabbitmq"

# ¿Docker está corriendo?
try { docker info | Out-Null } catch {
    Write-Host "Docker no responde. Abre Docker Desktop y espera a que esté 'running'." -ForegroundColor Red
    exit 1
}

if ($Stop) {
    Write-Host "Deteniendo y eliminando '$name'..." -ForegroundColor Yellow
    docker rm -f $name 2>$null | Out-Null
    Write-Host "Listo." -ForegroundColor Green
    exit 0
}

$existing = docker ps -a --filter "name=^/$name$" --format "{{.Names}}"
if ($existing -eq $name) {
    Write-Host "Contenedor '$name' ya existe; arrancándolo..." -ForegroundColor Cyan
    docker start $name | Out-Null
} else {
    Write-Host "Creando '$name' (rabbitmq:3-management)..." -ForegroundColor Cyan
    docker run -d --name $name `
        -p 5672:5672 -p 15672:15672 `
        rabbitmq:3-management | Out-Null
}

Write-Host "`nRabbitMQ arriba." -ForegroundColor Green
Write-Host "  AMQP:  localhost:5672"
Write-Host "  UI:    http://localhost:15672   (guest / guest)"
Write-Host "`nEspera ~10s a que arranque antes de conectar los servicios."
