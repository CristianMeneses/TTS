namespace TtsWorker.Models;

record CampaignJob(
    string JobId,
    string ClientId,
    string CampaignId,
    string Voice,
    string PhoneColumn,
    string TextColumn,
    byte[] FileBytes,
    string FileName,
    float LengthScale,
    float NoiseScale,
    float NoiseW,
    int PauseMs
);
