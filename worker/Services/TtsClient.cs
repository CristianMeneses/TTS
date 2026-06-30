using System.Globalization;
using System.Net.Http.Json;
using TtsWorker.Models;

namespace TtsWorker.Services;

class TtsClient(HttpClient http, ILogger<TtsClient> logger)
{
    public async Task<TtsManifest> GenerateBatchAsync(
        CampaignJob job,
        byte[] batchBytes,
        string batchFileName,
        string batchId,
        CancellationToken ct = default)
    {
        using var form = new MultipartFormDataContent();
        form.Add(new ByteArrayContent(batchBytes), "file", batchFileName);
        form.Add(new StringContent(job.Template),   "template");
        form.Add(new StringContent(job.Voice),      "voice");
        form.Add(new StringContent(job.ClientId),   "client_id");
        form.Add(new StringContent(job.CampaignId), "campaign_id");
        form.Add(new StringContent(job.PhoneColumn),"phone_column");
        form.Add(new StringContent(batchId),        "batch_id");
        form.Add(new StringContent(job.LengthScale.ToString("F2", CultureInfo.InvariantCulture)), "length_scale");
        form.Add(new StringContent(job.NoiseScale.ToString("F2",  CultureInfo.InvariantCulture)), "noise_scale");
        form.Add(new StringContent(job.NoiseW.ToString("F2",      CultureInfo.InvariantCulture)), "noise_w");
        form.Add(new StringContent(job.PauseMs.ToString()),                                        "pause_ms");

        logger.LogInformation("Enviando batch {BatchId} ({Rows} filas) al servicio TTS",
            batchId, batchFileName);

        var response = await http.PostAsync("/v1/batch/generate", form, ct);
        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync(ct);
            logger.LogError("TTS devolvió {Code}: {Body}", (int)response.StatusCode, body);
            response.EnsureSuccessStatusCode();
        }

        var manifest = await response.Content.ReadFromJsonAsync<TtsManifest>(ct)
            ?? throw new InvalidOperationException("Respuesta vacía del servicio TTS");

        logger.LogInformation("Batch {BatchId} completado: {Ok} ok, {Err} errores, {Ms}ms total",
            batchId, manifest.Summary.Successful, manifest.Summary.Errors, manifest.Summary.TotalMs);

        return manifest;
    }
}
