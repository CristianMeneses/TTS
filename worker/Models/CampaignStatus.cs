using System.Text.Json.Serialization;

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

    // Duración transcurrida: hasta CompletedAt si ya terminó, si no hasta "ahora"
    // (así el panel puede mostrar un cronómetro en vivo mientras sigue corriendo).
    public double ElapsedMs => ((CompletedAt ?? DateTime.UtcNow) - StartedAt).TotalMilliseconds;

    public string? Error { get; set; }
    public List<BatchSummary> Batches { get; set; } = [];

    // Los resultados pueden llegar concurrentemente (varios callbacks del consumidor).
    // Estos helpers protegen las actualizaciones compuestas (+=, List.Add) contra
    // lost-updates. Coste: un lock sobre operaciones de microsegundos → despreciable.
    [JsonIgnore] readonly object _sync = new();

    public void RecordResult(int recipients, bool ok)
    {
        lock (_sync)
        {
            ProcessedRows += recipients;
            if (ok) SuccessfulRows += recipients;
            else    FailedRows     += recipients;
        }
    }

    public void RecordBatchReleased(BatchSummary summary)
    {
        lock (_sync)
        {
            CompletedBatches++;
            Batches.Add(summary);
        }
    }
}

record BatchSummary(
    string BatchId,
    int BatchNumber,
    int Calls
);
