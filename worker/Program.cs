using TtsWorker.Models;
using TtsWorker.Services;

var builder = WebApplication.CreateBuilder(args);

// HTTP client hacia el servicio Python TTS
builder.Services.AddHttpClient<TtsClient>(client =>
{
    var baseUrl = builder.Configuration["Tts:BaseUrl"] ?? "http://localhost:8000";
    client.BaseAddress = new Uri(baseUrl);
    // Timeout generoso: un lote de 500 puede tardar varios minutos en frío
    client.Timeout = TimeSpan.FromMinutes(15);
});

// Worker de procesamiento en background
builder.Services.AddHostedService<CampaignProcessor>();

var app = builder.Build();

// ── POST /campaigns ──────────────────────────────────────────────────────────
// Recibe el Excel y los parámetros, encola el trabajo y responde de inmediato.
app.MapPost("/campaigns", async (HttpRequest request) =>
{
    if (!request.HasFormContentType)
        return Results.BadRequest("Se requiere multipart/form-data.");

    var form = await request.ReadFormAsync();

    if (form.Files.GetFile("file") is not IFormFile file)
        return Results.BadRequest("Campo 'file' requerido.");

    var clientId   = form["client_id"].ToString();
    var campaignId = form["campaign_id"].ToString();
    var voice      = form["voice"].ToString();
    var template   = form["template"].ToString();

    if (string.IsNullOrWhiteSpace(clientId))   return Results.BadRequest("client_id requerido.");
    if (string.IsNullOrWhiteSpace(campaignId)) return Results.BadRequest("campaign_id requerido.");
    if (string.IsNullOrWhiteSpace(voice))      return Results.BadRequest("voice requerido.");
    if (string.IsNullOrWhiteSpace(template))   return Results.BadRequest("template requerido (ej: {Message}).");

    using var ms = new MemoryStream();
    await file.CopyToAsync(ms);

    var job = new CampaignJob(
        JobId:       Guid.NewGuid().ToString("N")[..12],
        ClientId:    clientId,
        CampaignId:  campaignId,
        Voice:       voice,
        Template:    template,
        PhoneColumn: form["phone_column"].ToString(),
        FileBytes:   ms.ToArray(),
        FileName:    file.FileName,
        LengthScale: float.TryParse(form["length_scale"], out var ls) ? ls : 0.95f,
        NoiseScale:  float.TryParse(form["noise_scale"],  out var ns) ? ns : 0.85f,
        NoiseW:      float.TryParse(form["noise_w"],      out var nw) ? nw : 0.9f,
        PauseMs:     int.TryParse(form["pause_ms"],       out var pm) ? pm : 150
    );

    // Registrar estado antes de encolar
    CampaignProcessor.Statuses[job.JobId] = new CampaignStatus
    {
        JobId      = job.JobId,
        ClientId   = clientId,
        CampaignId = campaignId,
        State      = "queued",
    };

    await CampaignProcessor.Queue.Writer.WriteAsync(job);

    return Results.Accepted($"/campaigns/{job.JobId}", new
    {
        job_id     = job.JobId,
        client_id  = clientId,
        campaign_id = campaignId,
        status     = "queued",
        status_url = $"/campaigns/{job.JobId}",
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
