using System.Collections.Concurrent;
using System.Threading.Channels;
using TtsWorker.Models;

namespace TtsWorker.Services;

class CampaignProcessor(
    TtsClient ttsClient,
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

    readonly int _batchSize = config.GetValue("Worker:BatchSize", 500);
    readonly string _ttsBaseUrl = config["Tts:BaseUrl"] ?? "http://localhost:8000";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("CampaignProcessor iniciado. BatchSize={BatchSize}", _batchSize);
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

            var batches = ExcelReader.Chunk(rows, _batchSize).ToList();
            status.State        = "synthesizing";
            status.TotalRows    = rows.Count;
            status.TotalBatches = batches.Count;

            logger.LogInformation("Job {JobId}: {Rows} filas → {Batches} lotes",
                job.JobId, rows.Count, batches.Count);

            // ── Fase 2: síntesis TTS por lote ────────────────────────────────
            var pending = new List<DialerMessage>();

            for (int b = 0; b < batches.Count; b++)
            {
                if (ct.IsCancellationRequested) break;

                var batch   = batches[b];
                var batchId = $"{job.JobId}_b{b + 1:D3}";

                // Armar la lista de audios pendientes para este lote
                var pendingAudio = batch.Select(row => new PendingAudioItem
                {
                    Recipient = row.GetValueOrDefault(job.PhoneColumn, "").Trim(),
                    Text      = row.GetValueOrDefault(job.TextColumn, "").Trim(),
                }).Where(item => !string.IsNullOrEmpty(item.Text)).ToList();

                var request = new BatchRunRequest
                {
                    JobId        = batchId,
                    ClientId     = job.ClientId,
                    VoiceName    = job.Voice,
                    PendingAudio = pendingAudio,
                    LengthScale  = job.LengthScale,
                    NoiseScale   = job.NoiseScale,
                    NoiseW       = job.NoiseW,
                    PauseMs      = job.PauseMs,
                };

                TtsManifest manifest;
                try
                {
                    manifest = await ttsClient.RunBatchAsync(request, ct);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Batch {BatchId} falló, reintentando...", batchId);
                    await Task.Delay(2000, ct);
                    manifest = await ttsClient.RunBatchAsync(request, ct);
                }

                foreach (var audio in manifest.Audios.Where(a => a.Status == "ok" && a.AudioId != null))
                {
                    var phone = !string.IsNullOrEmpty(audio.Recipient) ? audio.Recipient : audio.Phone;
                    pending.Add(new DialerMessage
                    {
                        JobId      = job.JobId,
                        ClientId   = job.ClientId,
                        CampaignId = batchId,
                        BatchId    = batchId,
                        Phone      = phone,
                        AudioId    = audio.AudioId!,
                        AudioUrl   = $"{_ttsBaseUrl}/v1/audio/{job.ClientId}/{batchId}/{audio.AudioId}",
                        DurationMs = audio.DurationMs,
                        RowIndex   = audio.RowIndex,
                        TextHash   = audio.TextHash,
                    });
                }

                status.ProcessedRows    += batch.Count;
                status.SuccessfulRows   += manifest.Summary.Successful;
                status.FailedRows       += manifest.Summary.Errors;
                status.CompletedBatches++;
                status.Batches.Add(new BatchSummary(
                    batchId, b + 1,
                    manifest.Summary.Total,
                    manifest.Summary.Successful,
                    manifest.Summary.Errors,
                    manifest.Summary.TotalMs));

                logger.LogInformation(
                    "Job {JobId}: lote {N}/{Total} — {Ok} ok, {Err} errores, {Ms:F0}ms",
                    job.JobId, b + 1, batches.Count,
                    manifest.Summary.Successful, manifest.Summary.Errors, manifest.Summary.TotalMs);
            }

            // ── Fase 3: publicar a RabbitMQ ───────────────────────────────────
            status.State = "publishing";
            logger.LogInformation("Job {JobId}: publicando {N} mensajes a RabbitMQ",
                job.JobId, pending.Count);

            await using var rabbit = await CreateRabbitAsync(ct);
            foreach (var msg in pending)
                await rabbit.PublishAsync(msg, ct);

            status.State = "done";
            logger.LogInformation("Job {JobId}: completado — {N} audios publicados.",
                job.JobId, pending.Count);
        }
        catch (Exception ex) when (status.State == "publishing")
        {
            status.State = "publishing_failed";
            status.Error = $"TTS completado. Error publicando a RabbitMQ: {ex.Message}";
            logger.LogError(ex, "Job {JobId}: fallo en RabbitMQ", job.JobId);
        }
        catch (Exception ex)
        {
            status.State = "error";
            status.Error = ex.Message;
            logger.LogError(ex, "Job {JobId} terminó con error", job.JobId);
        }
        finally
        {
            status.CompletedAt = DateTime.UtcNow;
        }
    }

    async Task<RabbitPublisher> CreateRabbitAsync(CancellationToken ct) =>
        await RabbitPublisher.CreateAsync(
            host:        config["RabbitMq:Host"]        ?? "localhost",
            port:        config.GetValue("RabbitMq:Port", 5672),
            user:        config["RabbitMq:User"]        ?? "guest",
            password:    config["RabbitMq:Password"]    ?? "guest",
            virtualHost: config["RabbitMq:VirtualHost"] ?? "/",
            exchange:    config["RabbitMq:Exchange"]    ?? "tts",
            routingKey:  config["RabbitMq:RoutingKey"]  ?? "dialer",
            queueName:   config["RabbitMq:Queue"]       ?? "dialer_calls");
}
