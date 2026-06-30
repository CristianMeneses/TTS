using System.Text.Json.Serialization;

namespace TtsWorker.Models;

// Refleja la respuesta de POST /v1/batch/generate del servicio Python
class TtsManifest
{
    [JsonPropertyName("client_id")]    public string ClientId { get; set; } = "";
    [JsonPropertyName("campaign_id")]  public string CampaignId { get; set; } = "";
    [JsonPropertyName("batch_id")]     public string BatchId { get; set; } = "";
    [JsonPropertyName("created_at")]   public string CreatedAt { get; set; } = "";
    [JsonPropertyName("phone_column")] public string PhoneColumn { get; set; } = "";
    [JsonPropertyName("summary")]      public TtsSummary Summary { get; set; } = new();
    [JsonPropertyName("audios")]       public List<TtsAudio> Audios { get; set; } = [];
}

class TtsSummary
{
    [JsonPropertyName("total")]       public int Total { get; set; }
    [JsonPropertyName("successful")]  public int Successful { get; set; }
    [JsonPropertyName("errors")]      public int Errors { get; set; }
    [JsonPropertyName("from_cache")]  public int FromCache { get; set; }
    [JsonPropertyName("total_ms")]    public double TotalMs { get; set; }
    [JsonPropertyName("avg_ms")]      public double AvgMs { get; set; }
}

class TtsAudio
{
    [JsonPropertyName("audio_id")]      public string? AudioId { get; set; }
    [JsonPropertyName("row_index")]     public int RowIndex { get; set; }
    [JsonPropertyName("phone")]         public string Phone { get; set; } = "";
    [JsonPropertyName("status")]        public string Status { get; set; } = "";
    [JsonPropertyName("error")]         public string? Error { get; set; }
    [JsonPropertyName("retrieval_url")] public string? RetrievalUrl { get; set; }
    [JsonPropertyName("duration_ms")]   public int DurationMs { get; set; }
    [JsonPropertyName("size_bytes")]    public int SizeBytes { get; set; }
    [JsonPropertyName("from_cache")]    public bool FromCache { get; set; }
    [JsonPropertyName("timing_ms")]     public double TimingMs { get; set; }
    [JsonPropertyName("text_hash")]     public string? TextHash { get; set; }
    [JsonPropertyName("text")]          public string? Text { get; set; }
}
