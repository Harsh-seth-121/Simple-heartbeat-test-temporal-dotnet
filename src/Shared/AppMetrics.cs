using Temporalio.Common;

namespace HeartbeatDemo;

/// <summary>
/// Custom metric definitions for this demo.
/// </summary>
/// <remarks>
/// The SDK's built-in metrics cannot carry per-call tags, so anything that needs a dimension the
/// SDK does not already provide (for example "did this attempt resume?") is emitted here instead.
/// Metric instances are cached on the wrapper rather than recreated per call.
/// </remarks>
public static class AppMetrics
{
    /// <summary>Prefix on every Activity/Workflow metric this demo emits.</summary>
    public const string Prefix = "heartbeat_demo_";

    /// <summary>Prefix on every load generator metric.</summary>
    public const string LoadGenPrefix = "loadgen_";

    /// <summary>Empty tag set, reused to avoid allocating on every metric update.</summary>
    public static readonly IEnumerable<KeyValuePair<string, object>> NoTags =
        Array.Empty<KeyValuePair<string, object>>();

    /// <summary>Builds a single-entry tag set.</summary>
    /// <param name="key">Tag name.</param>
    /// <param name="value">Tag value.</param>
    /// <returns>A one-element tag collection.</returns>
    public static IEnumerable<KeyValuePair<string, object>> Tag(string key, object value) =>
        new[] { new KeyValuePair<string, object>(key, value) };
}

/// <summary>
/// Activity-scoped instruments, created once per Activity invocation.
/// </summary>
/// <remarks>
/// <see cref="Temporalio.Activities.ActivityExecutionContext.MetricMeter"/> is created lazily per
/// Activity execution and already carries activity_type, workflow_type, task_queue and namespace,
/// so this type only adds the missing dimensions.
/// </remarks>
/// <param name="meter">The Activity's metric meter.</param>
public sealed class ActivityMetrics(MetricMeter meter)
{
    /// <summary>Gets the attempt-started counter, tagged <c>resumed</c>.</summary>
    public MetricCounter<long> AttemptStarted { get; } = meter.CreateCounter<long>(
        AppMetrics.Prefix + "activity_attempt_started",
        "attempts",
        "Activity attempts started, split by whether they resumed from a heartbeat checkpoint");

    /// <summary>Gets the resume-offset histogram, in items.</summary>
    public MetricHistogram<long> ResumeOffset { get; } = meter.CreateHistogram<long>(
        AppMetrics.Prefix + "resume_offset",
        "items",
        "Item index a retried Activity attempt resumed from");

    /// <summary>Gets the processed-items counter.</summary>
    public MetricCounter<long> ItemsProcessed { get; } = meter.CreateCounter<long>(
        AppMetrics.Prefix + "items_processed",
        "items",
        "Items processed by Activity attempts; exceeds items requested when work is redone after a resume");

    /// <summary>Gets the heartbeat-call counter.</summary>
    public MetricCounter<long> HeartbeatsSent { get; } = meter.CreateCounter<long>(
        AppMetrics.Prefix + "heartbeats_sent",
        "heartbeats",
        "Heartbeat calls made by the Activity before SDK throttling");

    /// <summary>Gets the failed-attempt counter, tagged <c>reason</c>.</summary>
    public MetricCounter<long> AttemptFailed { get; } = meter.CreateCounter<long>(
        AppMetrics.Prefix + "activity_attempt_failed",
        "attempts",
        "Activity attempts that ended in failure or cancellation, by reason");
}

/// <summary>
/// Workflow-scoped instruments.
/// </summary>
/// <remarks>
/// Build these from <see cref="Temporalio.Workflows.Workflow.MetricMeter"/>, which suppresses
/// updates during replay, so these counters are not double-counted when a Workflow is replayed.
/// </remarks>
/// <param name="meter">The Workflow's replay-safe metric meter.</param>
public sealed class WorkflowMetrics(MetricMeter meter)
{
    /// <summary>Gets the requested-items counter.</summary>
    public MetricCounter<long> JobItemsRequested { get; } = meter.CreateCounter<long>(
        AppMetrics.Prefix + "job_items_requested",
        "items",
        "Items requested by started jobs; the denominator for reprocessing overhead");

    /// <summary>Gets the completed-jobs counter, tagged <c>outcome</c>.</summary>
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
    /// <summary>Gets the started-workflows counter.</summary>
    public MetricCounter<long> WorkflowsStarted { get; } = meter.CreateCounter<long>(
        AppMetrics.LoadGenPrefix + "workflows_started",
        "workflows",
        "Workflows successfully started");

    /// <summary>Gets the start-failure counter, tagged <c>error_type</c>.</summary>
    public MetricCounter<long> StartFailures { get; } = meter.CreateCounter<long>(
        AppMetrics.LoadGenPrefix + "start_failures",
        "errors",
        "Workflow start calls that threw, by error type");

    /// <summary>Gets the start-call latency histogram.</summary>
    public MetricHistogram<TimeSpan> StartLatency { get; } = meter.CreateHistogram<TimeSpan>(
        AppMetrics.LoadGenPrefix + "start_latency",
        "duration",
        "Latency of the StartWorkflow call");

    /// <summary>Gets the in-flight gauge.</summary>
    public MetricGauge<long> InFlight { get; } = meter.CreateGauge<long>(
        AppMetrics.LoadGenPrefix + "in_flight",
        "workflows",
        "Workflows started but not yet finished");

    /// <summary>Gets the completed-workflows counter, tagged <c>outcome</c>.</summary>
    public MetricCounter<long> WorkflowsCompleted { get; } = meter.CreateCounter<long>(
        AppMetrics.LoadGenPrefix + "workflows_completed",
        "workflows",
        "Workflows that reached a terminal state, by outcome");

    /// <summary>Gets the end-to-end duration histogram.</summary>
    public MetricHistogram<TimeSpan> EndToEndDuration { get; } = meter.CreateHistogram<TimeSpan>(
        AppMetrics.LoadGenPrefix + "e2e_duration",
        "duration",
        "Wall time from Workflow start to result");

    /// <summary>Gets the configured-target-rate gauge.</summary>
    public MetricGauge<double> TargetRate { get; } = meter.CreateGauge<double>(
        AppMetrics.LoadGenPrefix + "target_rate",
        "workflows/s",
        "Configured start rate, echoed so dashboards can draw target against achieved");

    /// <summary>Gets the pacer debt gauge.</summary>
    public MetricGauge<double> RateDebt { get; } = meter.CreateGauge<double>(
        AppMetrics.LoadGenPrefix + "rate_debt",
        "workflows",
        "Workflows the pacer owes against its target; the stack's saturation signal");
}
