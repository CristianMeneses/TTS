using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using TtsWorker.Models;

namespace TtsWorker.Services;

// COLECTOR (consumidor): consume `tts.results`, actualiza el progreso del job y,
// APENAS un lote queda completo, hace fan-out y publica sus DialerMessage a
// `dialer.output` (streaming). El marcador empieza a llamar el lote 1 mientras los
// siguientes aún se sintetizan.
class ResultCollector(RabbitBus bus, ILogger<ResultCollector> logger) : BackgroundService
{
    IChannel? _channel;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("ResultCollector iniciado. Consumiendo {Queue}", bus.ResultsQueue);
        _channel = await bus.ConsumeAsync(bus.ResultsQueue, HandleResultAsync, prefetch: 50, ct: stoppingToken);

        // Mantener vivo hasta que se cancele.
        try { await Task.Delay(Timeout.Infinite, stoppingToken); }
        catch (OperationCanceledException) { /* shutdown */ }
    }

    async Task HandleResultAsync(ReadOnlyMemory<byte> body)
    {
        var result = JsonSerializer.Deserialize<SynthesisResult>(body.Span);
        if (result is null || string.IsNullOrEmpty(result.JobId)) return;

        if (!CampaignProcessor.States.TryGetValue(result.JobId, out var state) ||
            !CampaignProcessor.Statuses.TryGetValue(result.JobId, out var status))
        {
            // El estado de los jobs vive en memoria; si el worker reinició a media
            // campaña, los resultados que aún llegan no tienen a dónde ir. Antes se
            // descartaban en silencio; ahora al menos queda registro. (Durabilidad
            // real = persistir el estado; es un cambio aparte, ver nota del review.)
            logger.LogWarning("Resultado para job desconocido {JobId} (¿worker reiniciado?) — descartado", result.JobId);
            return;
        }

        bool isNew = state.MarkDone(result.AudioId, result.Status, result.DurationMs, out var entry, out int releasedLote);
        if (!isNew || entry is null) return;

        // Progreso por destinatarios del texto (protegido contra callbacks concurrentes).
        status.RecordResult(entry.Recipients.Count, entry.Status == "ok");
        if (entry.Status != "ok")
            logger.LogWarning("Job {JobId}: texto {AudioId} falló: {Err}", result.JobId, result.AudioId, result.Error);

        // ¿Se completó un lote? → fan-out streaming hacia el marcador.
        if (releasedLote >= 0)
            await ReleaseLoteAsync(state, status, releasedLote);

        if (state.AllDone && status.State != "done")
        {
            status.State = "done";
            status.CompletedAt = DateTime.UtcNow;
            logger.LogInformation("Job {JobId}: completado — {Ok} ok, {Err} errores",
                result.JobId, status.SuccessfulRows, status.FailedRows);
        }
    }

    async Task ReleaseLoteAsync(CampaignState state, CampaignStatus status, int loteIndex)
    {
        var batchId = $"{state.JobId}_lote{loteIndex + 1:D3}";
        int calls = 0;

        foreach (var e in state.LoteEntries(loteIndex))
        {
            if (e.Status != "ok") continue;
            var audioUrl = $"{state.TtsBaseUrl}/v1/audio/{state.ClientId}/{state.JobId}/{e.AudioId}";
            var textHash = ShortHash(e.Text);

            foreach (var (rowIndex, phone) in e.Recipients)
            {
                var msg = new DialerMessage
                {
                    JobId      = state.JobId,
                    ClientId   = state.ClientId,
                    CampaignId = state.CampaignId,
                    BatchId    = batchId,
                    Phone      = phone,
                    AudioId    = e.AudioId,
                    AudioUrl   = audioUrl,
                    DurationMs = e.DurationMs,
                    RowIndex   = rowIndex,
                    TextHash   = textHash,
                };
                await bus.PublishAsync(bus.DialerQueue, JsonSerializer.SerializeToUtf8Bytes(msg));
                calls++;
            }
        }

        status.RecordBatchReleased(new BatchSummary(batchId, loteIndex + 1, calls));
        logger.LogInformation("Job {JobId}: lote {N}/{Total} liberado → {Calls} llamadas a {Queue}",
            state.JobId, loteIndex + 1, state.TotalLotes, calls, bus.DialerQueue);
    }

    static string ShortHash(string text) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text)))[..16].ToLowerInvariant();

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_channel is not null)
        {
            try { await _channel.CloseAsync(cancellationToken); } catch { /* ignore */ }
            _channel.Dispose();
        }
        await base.StopAsync(cancellationToken);
    }
}
