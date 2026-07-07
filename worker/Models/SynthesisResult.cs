using System.Text.Json.Serialization;

namespace TtsWorker.Models;

// Resultado de una tarea, publicado por el consumidor Python a la cola `tts.results`.
class SynthesisResult
{
    [JsonPropertyName("jobId")]      public string JobId { get; set; } = "";
    [JsonPropertyName("audioId")]    public string AudioId { get; set; } = "";
    [JsonPropertyName("status")]     public string Status { get; set; } = "";
    [JsonPropertyName("durationMs")] public int DurationMs { get; set; }
    [JsonPropertyName("sizeBytes")]  public int SizeBytes { get; set; }
    [JsonPropertyName("fromCache")]  public bool FromCache { get; set; }
    [JsonPropertyName("error")]      public string? Error { get; set; }
}
