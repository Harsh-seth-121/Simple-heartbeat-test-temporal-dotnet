namespace HeartbeatDemo;

/// <summary>
/// Work request for <see cref="ChunkedJobWorkflow"/>.
/// </summary>
/// <param name="JobId">Caller-assigned identifier, echoed into the result.</param>
/// <param name="ItemCount">Number of items to process.</param>
/// <param name="PerItemMillis">Simulated work duration per item.</param>
/// <param name="HeartbeatTimeoutSeconds">Heartbeat timeout applied to the Activity.</param>
public record JobInput(
    string JobId,
    int ItemCount,
    int PerItemMillis,
    int HeartbeatTimeoutSeconds);

/// <summary>
/// Progress checkpoint carried as the Activity's heartbeat detail.
/// </summary>
/// <param name="NextIndex">Index of the first item that has NOT been confirmed complete.</param>
/// <param name="Processed">Items successfully processed so far, across all attempts.</param>
/// <param name="Failed">Items that failed and were skipped.</param>
/// <param name="Checksum">Running fold over processed indices; lets tests assert real work happened.</param>
public record Checkpoint(
    int NextIndex,
    int Processed,
    int Failed,
    long Checksum);

/// <summary>
/// Outcome of a completed job.
/// </summary>
/// <param name="JobId">Echo of <see cref="JobInput.JobId"/>.</param>
/// <param name="Processed">Items confirmed processed.</param>
/// <param name="Failed">Items that failed and were skipped.</param>
/// <param name="Attempts">Activity attempt number that finally succeeded.</param>
/// <param name="ResumedFrom">Index the winning attempt resumed from; 0 means it ran from scratch.</param>
/// <param name="Checksum">Final checksum value.</param>
public record JobResult(
    string JobId,
    int Processed,
    int Failed,
    int Attempts,
    int ResumedFrom,
    long Checksum);
