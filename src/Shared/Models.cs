namespace HeartbeatDemo;

public record JobInput(
    string JobId,
    int ItemCount,
    int PerItemMillis,
    int HeartbeatTimeoutSeconds);

public record Checkpoint(
    int NextIndex,
    int Processed,
    int Failed,
    long Checksum);

public record JobResult(
    string JobId,
    int Processed,
    int Failed,
    int Attempts,
    int ResumedFrom,
    long Checksum);
