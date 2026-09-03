using System.Globalization;

namespace HeartbeatDemo;

/// <summary>
/// Read once from environment variables. Only Worker and load generator process code read this; 
/// Workflow code must not, since ENV reads are non-deterministic.
/// </summary>
public sealed record AppConfig
{
    public string TaskQueue { get; init; } = "long-activity-heartbeat";

    public string MetricsBindAddress { get; init; } = "0.0.0.0:9464";

    public string Target { get; init; } = "local";

    public int HeartbeatTimeoutSeconds { get; init; } = 10;

    public int JobItemCount { get; init; } = 200;

    public int JobPerItemMillis { get; init; } = 250;

    public double ChaosFailureRate { get; init; }

    public int MaxConcurrentActivities { get; init; } = 100;

    public double TargetRatePerSecond { get; init; } = 0.5;

    public int MaxInFlight { get; init; } = 50;

    public int DurationSeconds { get; init; }

    public static AppConfig FromEnvironment()
    {
        var defaults = new AppConfig();
        return defaults with
        {
            TaskQueue = Str("TASK_QUEUE", defaults.TaskQueue),
            MetricsBindAddress = Str("METRICS_BIND_ADDRESS", defaults.MetricsBindAddress),
            Target = Str("TEMPORAL_TARGET", defaults.Target).ToLowerInvariant(),
            HeartbeatTimeoutSeconds = Int("HEARTBEAT_TIMEOUT_SECONDS", defaults.HeartbeatTimeoutSeconds),
            JobItemCount = Int("JOB_ITEM_COUNT", defaults.JobItemCount),
            JobPerItemMillis = Int("JOB_PER_ITEM_MILLIS", defaults.JobPerItemMillis),
            ChaosFailureRate = Dbl("CHAOS_FAILURE_RATE", 0.02),
            MaxConcurrentActivities = Int("WORKER_MAX_CONCURRENT_ACTIVITIES", defaults.MaxConcurrentActivities),
            TargetRatePerSecond = Dbl("LOADGEN_TARGET_RATE_PER_SEC", defaults.TargetRatePerSecond),
            MaxInFlight = Int("LOADGEN_MAX_IN_FLIGHT", defaults.MaxInFlight),
            DurationSeconds = Int("LOADGEN_DURATION_SECONDS", defaults.DurationSeconds),
        };
    }

    public JobInput NewJobInput(string jobId) =>
        new(jobId, JobItemCount, JobPerItemMillis, HeartbeatTimeoutSeconds);

    private static string Str(string key, string fallback)
    {
        var raw = Environment.GetEnvironmentVariable(key);
        return string.IsNullOrWhiteSpace(raw) ? fallback : raw.Trim();
    }

    private static int Int(string key, int fallback) =>
        int.TryParse(Str(key, string.Empty), NumberStyles.Integer, CultureInfo.InvariantCulture, out var v)
            ? v
            : fallback;

    private static double Dbl(string key, double fallback) =>
        double.TryParse(Str(key, string.Empty), NumberStyles.Float, CultureInfo.InvariantCulture, out var v)
            ? v
            : fallback;
}
