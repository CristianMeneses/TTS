using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;
using TtsWorker.Models;

namespace TtsWorker.Services;

// ORQUESTADOR (productor): recibe el job, lee el Excel, deduplica textos y publica
// 1 tarea de síntesis por texto único a la cola `tts.tasks`. El progreso y el
// streaming por lote los maneja ResultCollector conforme llegan los resultados.
class CampaignProcessor(
    RabbitBus bus,
    IConfiguration config,
    ILogger<CampaignProcessor> logger) : BackgroundService
{
    public static readonly Channel<CampaignJob> Queue =
        Channel.CreateBounded<CampaignJob>(new BoundedChannelOptions(50)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
        });

    public static readonly ConcurrentDictionary<string, CampaignStatus> Statuses = new();
    public static readonly ConcurrentDictionary<string, CampaignState> States = new();

    readonly int _batchSize = config.GetValue("Worker:BatchSize", 250);
    readonly string _ttsBaseUrl = config["Tts:BaseUrl"] ?? "http://localhost:8000";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("CampaignProcessor (orquestador) iniciado. BatchSize={BatchSize}", _batchSize);
        await foreach (var job in Queue.Reader.ReadAllAsync(stoppingToken))
            await ProcessJobAsync(job, stoppingToken);
    }

    async Task ProcessJobAsync(CampaignJob job, CancellationToken ct)
    {
        var status = Statuses[job.JobId];
        status.StartedAt = DateTime.UtcNow;

        try
        {
            // ── Fase 1: leer Excel ────────────────────────────────────────────
            var rows = ExcelReader.ReadRows(job.FileBytes, job.FileName);
            if (rows.Count == 0)
                throw new InvalidOperationException("Archivo sin filas de datos.");

            // (rowIndex 1-based, phone, text) — descarta filas sin texto.
            var items = rows
                .Select((r, i) => (
                    RowIndex: i + 1,
                    Phone:    r.GetValueOrDefault(job.PhoneColumn, "").Trim(),
                    Text:     r.GetValueOrDefault(job.TextColumn, "").Trim()))
                .Where(x => !string.IsNullOrEmpty(x.Text))
                .ToList();

            // ── Fase 2: dedup + armar estado ─────────────────────────────────
            var state = new CampaignState
            {
                JobId       = job.JobId,
                ClientId    = job.ClientId,
                CampaignId  = job.CampaignId,
                Voice       = job.Voice,
                TtsBaseUrl  = _ttsBaseUrl,
                LengthScale = job.LengthScale,
                NoiseScale  = job.NoiseScale,
                NoiseW      = job.NoiseW,
                PauseMs     = job.PauseMs,
            };
            var uniqueTexts = state.Build(items, _batchSize);

            // Registrar ANTES de publicar (evita que un resultado llegue sin estado).
            States[job.JobId] = state;
            status.State        = "synthesizing";
            status.TotalRows    = state.Recipients;
            status.TotalBatches = state.TotalLotes;

            logger.LogInformation(
                "Job {JobId}: {Rows} destinatarios → {Unique} textos únicos → {Lotes} lotes",
                job.JobId, state.Recipients, uniqueTexts.Count, state.TotalLotes);

            // ── Fase 3: publicar 1 tarea por texto único ─────────────────────
            foreach (var e in uniqueTexts)
            {
                var task = new SynthesisTask
                {
                    JobId       = job.JobId,
                    ClientId    = job.ClientId,
                    CampaignId  = job.CampaignId,
                    AudioId     = e.AudioId,
                    VoiceName   = job.Voice,
                    Text        = e.Text,
                    LengthScale = job.LengthScale,
                    NoiseScale  = job.NoiseScale,
                    NoiseW      = job.NoiseW,
                    PauseMs     = job.PauseMs,
                };
                await bus.PublishAsync(bus.TasksQueue, JsonSerializer.SerializeToUtf8Bytes(task), ct);
            }

            logger.LogInformation("Job {JobId}: {N} tareas publicadas a {Queue}",
                job.JobId, uniqueTexts.Count, bus.TasksQueue);
        }
        catch (Exception ex)
        {
            status.State = "error";
            status.Error = ex.Message;
            status.CompletedAt = DateTime.UtcNow;
            logger.LogError(ex, "Job {JobId} falló en la orquestación", job.JobId);
        }
    }
}
