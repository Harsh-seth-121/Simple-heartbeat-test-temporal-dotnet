using System.Globalization;

namespace HeartbeatDemo;

/// <summary>
/// Process configuration, read once from environment variables.
/// </summary>
/// <remarks>
/// Only Worker and load generator process code reads this. Workflow code must not: see the remarks
/// on <see cref="JobInput"/>.
/// </remarks>
public sealed record AppConfig
{
    /// <summary>Gets the task queue shared by the Worker and the load generator.</summary>
    public string TaskQueue { get; init; } = "long-activity-heartbeat";

    /// <summary>Gets the "ip:port" the SDK binds its Prometheus scrape endpoint to.</summary>
    /// <remarks>Hostnames are rejected by the SDK; this must be an IP literal and its own port.</remarks>
    public string MetricsBindAddress { get; init; } = "0.0.0.0:9464";

    /// <summary>Gets "local" or "cloud". Affects default address only, never client code paths.</summary>
    public string Target { get; init; } = "local";

    /// <summary>Gets the heartbeat timeout handed to the Activity.</summary>
    public int HeartbeatTimeoutSeconds { get; init; } = 10;

    /// <summary>Gets the item count per job.</summary>
    public int JobItemCount { get; init; } = 200;

    /// <summary>Gets the simulated work duration per item.</summary>
    public int JobPerItemMillis { get; init; } = 250;

    /// <summary>Gets the per-item probability that the Activity throws, in [0, 1].</summary>
    public double ChaosFailureRate { get; init; }

    /// <summary>Gets the Worker's concurrent Activity ceiling.</summary>
    public int MaxConcurrentActivities { get; init; } = 100;

    /// <summary>Gets the load generator's Workflow start rate target, per second.</summary>
    public double TargetRatePerSecond { get; init; } = 0.5;

    /// <summary>Gets the load generator's ceiling on simultaneously running Workflows.</summary>
    public int MaxInFlight { get; init; } = 50;

    /// <summary>Gets how long the load generator runs; 0 means until stopped.</summary>
    public int DurationSeconds { get; init; }

    /// <summary>Reads configuration from the process environment, applying defaults.</summary>
    /// <returns>The populated configuration.</returns>
    /// <remarks>
    /// Fallbacks come from the property initializers above so each default is stated once.
    /// <see cref="ChaosFailureRate"/> is the deliberate exception: a deployed process defaults to a
    /// trickle of chaos because that is what the dashboards exist to show, while an in-process
    /// <see cref="AppConfig"/> defaults to none so a caller only gets the failures it asked for.
    /// </remarks>
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

    /// <summary>Builds the job input implied by this configuration.</summary>
    /// <param name="jobId">Identifier for the job.</param>
    /// <returns>Job input for <see cref="ChunkedJobWorkflow"/>.</returns>
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
