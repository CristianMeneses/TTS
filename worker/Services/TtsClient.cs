using System.Net.Http.Json;
using TtsWorker.Models;

namespace TtsWorker.Services;

class TtsClient(HttpClient http, ILogger<TtsClient> logger)
{
    public async Task<TtsManifest> RunBatchAsync(
        BatchRunRequest request,
        CancellationToken ct = default)
    {
        logger.LogInformation(
            "Enviando batch {JobId} ({Count} audios) al servicio TTS",
            request.JobId, request.PendingAudio.Count);

        var response = await http.PostAsJsonAsync("/v1/batch/run", request, ct);

        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync(ct);
            logger.LogError("TTS devolvió {Code}: {Body}", (int)response.StatusCode, body);
            response.EnsureSuccessStatusCode();
        }

        var manifest = await response.Content.ReadFromJsonAsync<TtsManifest>(ct)
            ?? throw new InvalidOperationException("Respuesta vacía del servicio TTS");

        logger.LogInformation(
            "Batch {JobId} completado: {Ok} ok, {Err} errores, {Ms}ms",
            request.JobId, manifest.Summary.Successful, manifest.Summary.Errors, manifest.Summary.TotalMs);

        return manifest;
    }
}
