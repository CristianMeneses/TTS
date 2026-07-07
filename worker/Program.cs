using System.Globalization;
using TtsWorker.Models;
using TtsWorker.Services;

// ContentRootPath explícito: si el .exe se lanza con ruta completa desde otro
// directorio (ej. `& "$env:LOCALAPPDATA\TtsWorker\TtsWorker.exe"`), .NET usa el
// directorio de trabajo actual como content root por defecto — y entonces NO
// encuentra su propio appsettings.json (silenciosamente cae a valores default,
// como el puerto 5000 en vez de los 8001 configurados). Fijarlo a la carpeta del
// ejecutable hace que la config se cargue siempre, sin importar desde dónde se lance.
var builder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory,
});

// CORS permisivo para desarrollo: el panel de campañas del lab se sirve desde
// otro origen (uvicorn en :8000) y hace fetch a este worker (:8001).
builder.Services.AddCors(o => o.AddDefaultPolicy(p =>
    p.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod()));

// Bus de RabbitMQ (conexión única, compartida). Se conecta al arrancar, con retry:
// RabbitMQ debe estar corriendo (ver infra/rabbitmq.ps1).
using (var bootLog = LoggerFactory.Create(b => b.AddConsole()))
{
    var bus = await RabbitBus.CreateAsync(builder.Configuration, bootLog.CreateLogger<RabbitBus>());
    builder.Services.AddSingleton(bus);
}

// Orquestador (productor) + colector de resultados (streaming por lote)
builder.Services.AddHostedService<CampaignProcessor>();
builder.Services.AddHostedService<ResultCollector>();

var app = builder.Build();
app.UseCors();

// ── POST /campaigns ──────────────────────────────────────────────────────────
// Recibe el Excel y los parámetros, encola el trabajo y responde de inmediato.
app.MapPost("/campaigns", async (HttpRequest request) =>
{
    if (!request.HasFormContentType)
        return Results.BadRequest("Se requiere multipart/form-data.");

    var form = await request.ReadFormAsync();

    if (form.Files.GetFile("file") is not IFormFile file)
        return Results.BadRequest("Campo 'file' requerido.");

    var clientId = form["client_id"].ToString();
    var voice    = form["voice"].ToString();

    if (string.IsNullOrWhiteSpace(clientId)) return Results.BadRequest("client_id requerido.");
    if (string.IsNullOrWhiteSpace(voice))    return Results.BadRequest("voice requerido.");

    using var ms = new MemoryStream();
    await file.CopyToAsync(ms);

    var job = new CampaignJob(
        JobId:       Guid.NewGuid().ToString("N")[..12],
        ClientId:    clientId,
        CampaignId:  form["campaign_id"].FirstOrDefault() ?? "",
        Voice:       voice,
        PhoneColumn: form["phone_column"].FirstOrDefault() ?? "telefono",
        TextColumn:  form["text_column"].FirstOrDefault()  ?? "Message",
        FileBytes:   ms.ToArray(),
        FileName:    file.FileName,
        LengthScale: float.TryParse(form["length_scale"], NumberStyles.Float, CultureInfo.InvariantCulture, out var ls) ? ls : 0.95f,
        NoiseScale:  float.TryParse(form["noise_scale"],  NumberStyles.Float, CultureInfo.InvariantCulture, out var ns) ? ns : 0.85f,
        NoiseW:      float.TryParse(form["noise_w"],      NumberStyles.Float, CultureInfo.InvariantCulture, out var nw) ? nw : 0.9f,
        PauseMs:     int.TryParse(form["pause_ms"],       out var pm) ? pm : 150
    );

    // Registrar estado antes de encolar
    CampaignProcessor.Statuses[job.JobId] = new CampaignStatus
    {
        JobId     = job.JobId,
        ClientId  = clientId,
        State     = "queued",
    };

    await CampaignProcessor.Queue.Writer.WriteAsync(job);

    return Results.Accepted($"/campaigns/{job.JobId}", new
    {
        job_id      = job.JobId,
        client_id   = clientId,
        campaign_id = job.CampaignId,
        status      = "queued",
        status_url  = $"/campaigns/{job.JobId}",
    });
});

// ── GET /campaigns/{jobId} ───────────────────────────────────────────────────
app.MapGet("/campaigns/{jobId}", (string jobId) =>
{
    if (!CampaignProcessor.Statuses.TryGetValue(jobId, out var status))
        return Results.NotFound(new { error = $"Job '{jobId}' no encontrado." });

    return Results.Ok(status);
});

// ── GET /campaigns ───────────────────────────────────────────────────────────
// Lista todos los jobs en memoria (útil para monitoreo)
app.MapGet("/campaigns", () =>
    Results.Ok(CampaignProcessor.Statuses.Values
        .OrderByDescending(s => s.StartedAt)
        .Take(100)));

// ── GET /health ──────────────────────────────────────────────────────────────
app.MapGet("/health", () => Results.Ok(new { status = "ok", utc = DateTime.UtcNow }));

app.Run();
