using System.Collections.Concurrent;
using System.Text.Json;
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
                    CampaignId   = job.CampaignId,
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

            // ── Fase 3: guardar resultado en JSON (modo prueba, sin RabbitMQ) ──
            status.State = "publishing";

            var outputDir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                "tts_output", job.ClientId, job.JobId);
            Directory.CreateDirectory(outputDir);

            var outputFile = Path.Combine(outputDir, "messages.json");
            await File.WriteAllTextAsync(outputFile,
                JsonSerializer.Serialize(pending, new JsonSerializerOptions { WriteIndented = true }), ct);

            status.OutputFile = outputFile;
            status.State      = "done";
            logger.LogInformation("Job {JobId}: completado — {N} mensajes guardados en {File}",
                job.JobId, pending.Count, outputFile);
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

}
