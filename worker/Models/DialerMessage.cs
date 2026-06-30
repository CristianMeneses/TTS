using System.Text.Json.Serialization;

namespace TtsWorker.Models;

// Mensaje publicado a RabbitMQ para el worker del marcador.
// Ajustar los campos según lo que espere el worker existente.
class DialerMessage
{
    [JsonPropertyName("job_id")]      public string JobId { get; set; } = "";
    [JsonPropertyName("client_id")]   public string ClientId { get; set; } = "";
    [JsonPropertyName("campaign_id")] public string CampaignId { get; set; } = "";
    [JsonPropertyName("batch_id")]    public string BatchId { get; set; } = "";
    [JsonPropertyName("phone")]       public string Phone { get; set; } = "";
    [JsonPropertyName("audio_id")]    public string AudioId { get; set; } = "";
    [JsonPropertyName("audio_url")]   public string AudioUrl { get; set; } = "";
    [JsonPropertyName("duration_ms")] public int DurationMs { get; set; }
    [JsonPropertyName("row_index")]   public int RowIndex { get; set; }
    [JsonPropertyName("text_hash")]   public string? TextHash { get; set; }
}
