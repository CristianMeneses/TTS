namespace TtsWorker.Services;

// Estado por-job para el pipeline por colas: dedup de textos + tracking de lotes.
//
// Dedup: 1 texto único = 1 audioId = 1 tarea. Un texto puede tener varios
// destinatarios (teléfonos). Los textos únicos se agrupan en LOTES de tamaño fijo;
// cuando todas las tareas de un lote terminan, el lote se "libera" (streaming) y se
// hace fan-out: por cada destinatario de cada texto del lote se emite un DialerMessage.
sealed class CampaignState
{
    public required string JobId { get; init; }
    public required string ClientId { get; init; }
    public required string CampaignId { get; init; }
    public required string Voice { get; init; }
    public required string TtsBaseUrl { get; init; }
    public float LengthScale { get; init; }
    public float NoiseScale { get; init; }
    public float NoiseW { get; init; }
    public int PauseMs { get; init; }

    public sealed class TextEntry
    {
        public required string AudioId { get; init; }
        public required string Text { get; init; }
        public int LoteIndex { get; set; }
        public readonly List<(int RowIndex, string Phone)> Recipients = [];
        public string Status = "pending";  // pending | ok | error
        public int DurationMs;
    }

    readonly Dictionary<string, TextEntry> _byText = [];
    readonly Dictionary<string, TextEntry> _byAudioId = [];
    readonly HashSet<string> _doneIds = [];
    int[] _loteSize = [];
    int[] _loteDone = [];
    bool[] _loteReleased = [];
    List<TextEntry>[] _loteEntries = [];
    readonly object _lock = new();

    public int TotalTasks { get; private set; }
    public int TotalLotes { get; private set; }
    public int DoneTasks { get; private set; }
    public int Recipients { get; private set; }

    // Construye el estado a partir de las filas (rowIndex, phone, text) ya filtradas.
    // Devuelve la lista de textos únicos (= tareas a publicar), en orden de aparición.
    public List<TextEntry> Build(IEnumerable<(int RowIndex, string Phone, string Text)> rows, int batchSize)
    {
        lock (_lock)
        {
            var order = new List<TextEntry>();
            foreach (var (rowIndex, phone, text) in rows)
            {
                if (!_byText.TryGetValue(text, out var e))
                {
                    e = new TextEntry { AudioId = Guid.NewGuid().ToString("N"), Text = text };
                    _byText[text] = e;
                    _byAudioId[e.AudioId] = e;
                    order.Add(e);
                }
                e.Recipients.Add((rowIndex, phone));
                Recipients++;
            }

            TotalLotes = Math.Max(1, (order.Count + batchSize - 1) / batchSize);
            _loteSize = new int[TotalLotes];
            _loteDone = new int[TotalLotes];
            _loteReleased = new bool[TotalLotes];
            _loteEntries = new List<TextEntry>[TotalLotes];
            for (int i = 0; i < TotalLotes; i++) _loteEntries[i] = [];
            for (int i = 0; i < order.Count; i++)
            {
                int li = i / batchSize;
                order[i].LoteIndex = li;
                _loteSize[li]++;
                _loteEntries[li].Add(order[i]);
            }
            TotalTasks = order.Count;
            return order;
        }
    }

    // Marca un audioId como terminado (ok o error). Idempotente ante reentregas.
    // Devuelve true si el resultado es nuevo (no duplicado y pertenece al job).
    // releasedLote = índice del lote si ESTE resultado lo completó (listo para liberar), o -1.
    public bool MarkDone(string audioId, string status, int durationMs, out TextEntry? entry, out int releasedLote)
    {
        releasedLote = -1;
        lock (_lock)
        {
            if (!_byAudioId.TryGetValue(audioId, out entry)) return false;  // no es de este job
            if (!_doneIds.Add(audioId)) return false;                        // resultado duplicado

            entry.Status = status == "ok" ? "ok" : "error";
            entry.DurationMs = durationMs;
            DoneTasks++;

            int li = entry.LoteIndex;
            _loteDone[li]++;
            if (_loteDone[li] == _loteSize[li] && !_loteReleased[li])
            {
                _loteReleased[li] = true;
                releasedLote = li;
            }
            return true;
        }
    }

    // Textos de un lote (para el fan-out al liberarlo). Pre-indexado en Build() → O(1).
    // La lista no se muta tras Build (solo cambian campos de cada TextEntry), así que
    // devolver la referencia es seguro para el recorrido de solo lectura del colector.
    public List<TextEntry> LoteEntries(int loteIndex)
    {
        lock (_lock)
            return _loteEntries[loteIndex];
    }

    public bool AllDone
    {
        get { lock (_lock) return DoneTasks >= TotalTasks; }
    }
}
