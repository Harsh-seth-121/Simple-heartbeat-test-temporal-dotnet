using Temporalio.Common;

namespace HeartbeatDemo;

/// <summary>
/// Custom metric definitions for this demo. SDK built-in metrics cannot carry per-call tags, so
/// anything needing a dimension the SDK does not provide (for example "did this attempt resume?")
/// is emitted here.
/// </summary>
public static class AppMetrics
{
    public const string Prefix = "heartbeat_demo_";
    public const string LoadGenPrefix = "loadgen_";

    /// <summary>Empty tag set, reused to avoid allocating on every metric update.</summary>
    public static readonly IEnumerable<KeyValuePair<string, object>> NoTags =
        Array.Empty<KeyValuePair<string, object>>();

    public static IEnumerable<KeyValuePair<string, object>> Tag(string key, object value) =>
        new[] { new KeyValuePair<string, object>(key, value) };
}

/// <summary>
/// Activity-scoped instruments, created once per Activity invocation.
/// </summary>
/// <param name="meter">The Activity's metric meter.</param>
public sealed class ActivityMetrics(MetricMeter meter)
{
    public MetricCounter<long> AttemptStarted { get; } = meter.CreateCounter<long>(
        AppMetrics.Prefix + "activity_attempt_started",
        "attempts",
        "Activity attempts started, split by whether they resumed from a heartbeat checkpoint");

    public MetricHistogram<long> ResumeOffset { get; } = meter.CreateHistogram<long>(
        AppMetrics.Prefix + "resume_offset",
        "items",
        "Item index a retried Activity attempt resumed from");

    public MetricCounter<long> ItemsProcessed { get; } = meter.CreateCounter<long>(
        AppMetrics.Prefix + "items_processed",
        "items",
        "Items processed by Activity attempts; exceeds items requested when a resume redoes work");

    public MetricCounter<long> HeartbeatsSent { get; } = meter.CreateCounter<long>(
        AppMetrics.Prefix + "heartbeats_sent",
        "heartbeats",
        "Heartbeat calls made by Activity before SDK throttling");

    public MetricCounter<long> AttemptFailed { get; } = meter.CreateCounter<long>(
        AppMetrics.Prefix + "activity_attempt_failed",
        "attempts",
        "Activity attempts that ended in failure or cancellation, by reason");
}

/// <summary>
/// Workflow-scoped instruments. Build these from
/// </summary>
/// <param name="meter">The Workflow's replay-safe metric meter.</param>
public sealed class WorkflowMetrics(MetricMeter meter)
{
    public MetricCounter<long> JobItemsRequested { get; } = meter.CreateCounter<long>(
        AppMetrics.Prefix + "job_items_requested",
        "items",
        "Items requested by started jobs; the denominator for reprocessing overhead");

    public MetricCounter<long> JobCompleted { get; } = meter.CreateCounter<long>(
        AppMetrics.Prefix + "job_completed",
        "jobs",
        "Jobs that finished, by outcome");
}

/// <summary>
/// Load generator instruments, created once per process from the runtime meter.
/// </summary>
/// <param name="meter">The runtime metric meter.</param>
public sealed class LoadGenMetrics(MetricMeter meter)
{
    public MetricCounter<long> WorkflowsStarted { get; } = meter.CreateCounter<long>(
        AppMetrics.LoadGenPrefix + "workflows_started",
        "workflows",
        "Workflows successfully started");

    public MetricCounter<long> StartFailures { get; } = meter.CreateCounter<long>(
        AppMetrics.LoadGenPrefix + "start_failures",
        "errors",
        "Workflow start calls that threw, by error type");

    public MetricHistogram<TimeSpan> StartLatency { get; } = meter.CreateHistogram<TimeSpan>(
        AppMetrics.LoadGenPrefix + "start_latency",
        "duration",
        "Latency of the StartWorkflow call");

    public MetricGauge<long> InFlight { get; } = meter.CreateGauge<long>(
        AppMetrics.LoadGenPrefix + "in_flight",
        "workflows",
        "Workflows started but not yet finished");

    public MetricCounter<long> WorkflowsCompleted { get; } = meter.CreateCounter<long>(
        AppMetrics.LoadGenPrefix + "workflows_completed",
        "workflows",
        "Workflows that reached a terminal state, by outcome");

    public MetricHistogram<TimeSpan> EndToEndDuration { get; } = meter.CreateHistogram<TimeSpan>(
        AppMetrics.LoadGenPrefix + "e2e_duration",
        "duration",
        "Wall time from Workflow start to result");

    public MetricGauge<double> TargetRate { get; } = meter.CreateGauge<double>(
        AppMetrics.LoadGenPrefix + "target_rate",
        "workflows/s",
        "Configured start rate, echoed so dashboards can draw target against achieved");

    public MetricGauge<double> RateDebt { get; } = meter.CreateGauge<double>(
        AppMetrics.LoadGenPrefix + "rate_debt",
        "workflows",
        "Workflows the pacer owes against its target; the saturation signal");
}
