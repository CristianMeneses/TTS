using System.Text.Json.Serialization;

namespace TtsWorker.Models;

// Tarea de síntesis publicada a la cola `tts.tasks`. 1 tarea = 1 texto único.
// El consumidor Python escribe el audio en STORAGE_DIR/{clientId}/{jobId}/audios/{audioId}.wav.
class SynthesisTask
{
    [JsonPropertyName("jobId")]       public string JobId { get; set; } = "";
    [JsonPropertyName("clientId")]    public string ClientId { get; set; } = "";
    [JsonPropertyName("campaignId")]  public string CampaignId { get; set; } = "";
    [JsonPropertyName("audioId")]     public string AudioId { get; set; } = "";
    [JsonPropertyName("voiceName")]   public string VoiceName { get; set; } = "";
    [JsonPropertyName("text")]        public string Text { get; set; } = "";
    [JsonPropertyName("lengthScale")] public float LengthScale { get; set; } = 0.95f;
    [JsonPropertyName("noiseScale")]  public float NoiseScale { get; set; } = 0.85f;
    [JsonPropertyName("noiseW")]      public float NoiseW { get; set; } = 0.9f;
    [JsonPropertyName("pauseMs")]     public int PauseMs { get; set; } = 150;
}
