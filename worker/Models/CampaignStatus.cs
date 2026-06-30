namespace TtsWorker.Models;

class CampaignStatus
{
    public string JobId { get; set; } = "";
    public string ClientId { get; set; } = "";
    public string State { get; set; } = "queued"; // queued | processing | done | error
    public int TotalRows { get; set; }
    public int ProcessedRows { get; set; }
    public int SuccessfulRows { get; set; }
    public int FailedRows { get; set; }
    public int TotalBatches { get; set; }
    public int CompletedBatches { get; set; }
    public DateTime StartedAt { get; set; } = DateTime.UtcNow;
    public DateTime? CompletedAt { get; set; }
    public string? Error { get; set; }
    public List<BatchSummary> Batches { get; set; } = [];
}

record BatchSummary(
    string BatchId,
    int BatchNumber,
    int Total,
    int Successful,
    int Errors,
    double TotalMs
);
