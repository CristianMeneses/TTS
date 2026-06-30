using System.Text.Json.Serialization;

namespace TtsWorker.Models;

class BatchRunRequest
{
    [JsonPropertyName("jobId")]        public string JobId { get; set; } = "";
    [JsonPropertyName("clientId")]     public string ClientId { get; set; } = "";
    [JsonPropertyName("voiceName")]    public string VoiceName { get; set; } = "";
    [JsonPropertyName("pendingAudio")] public List<PendingAudioItem> PendingAudio { get; set; } = [];
    [JsonPropertyName("lengthScale")]  public float LengthScale { get; set; } = 0.95f;
    [JsonPropertyName("noiseScale")]   public float NoiseScale { get; set; } = 0.85f;
    [JsonPropertyName("noiseW")]       public float NoiseW { get; set; } = 0.9f;
    [JsonPropertyName("pauseMs")]      public int PauseMs { get; set; } = 150;
    [JsonPropertyName("includeText")]  public bool IncludeText { get; set; } = true;
}

class PendingAudioItem
{
    [JsonPropertyName("recipient")] public string Recipient { get; set; } = "";
    [JsonPropertyName("text")]      public string Text { get; set; } = "";
}
