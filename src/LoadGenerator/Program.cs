using System.Diagnostics;
using System.Globalization;
using HeartbeatDemo;
using Microsoft.Extensions.Logging;
using Temporalio.Client;

// Load generator entry point. Starts Workflows at a configured rate, subject to a ceiling on how
// many may be in flight, and reports what it saw on its own Prometheus endpoint.
var config = AppConfig.FromEnvironment();

using var loggerFactory = ProcessHost.CreateLoggerFactory();
var logger = loggerFactory.CreateLogger("LoadGenerator");

var connection = await TemporalStack.ConnectAsync(config, loggerFactory);
var metrics = new LoadGenMetrics(connection.Runtime.MetricMeter);
metrics.TargetRate.Set(config.TargetRatePerSecond, AppMetrics.NoTags);

using var shutdown = new ShutdownSignal();

var runToken = DateTime.UtcNow.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture);
var inFlightGate = new SemaphoreSlim(config.MaxInFlight, config.MaxInFlight);
var tracked = new List<Task>();
var inFlight = 0L;
var issued = 0L;

logger.LogInformation(
    "Driving load: target={Target}/s maxInFlight={MaxInFlight} duration={Duration}s job={Items}x{Millis}ms heartbeatTimeout={Heartbeat}s",
    config.TargetRatePerSecond,
    config.MaxInFlight,
    config.DurationSeconds == 0 ? -1 : config.DurationSeconds,
    config.JobItemCount,
    config.JobPerItemMillis,
    config.HeartbeatTimeoutSeconds);

var clock = Stopwatch.StartNew();
var deadline = config.DurationSeconds > 0
    ? TimeSpan.FromSeconds(config.DurationSeconds)
    : TimeSpan.MaxValue;

try
{
    while (!shutdown.Token.IsCancellationRequested && clock.Elapsed < deadline)
    {
        // Debt is the whole point of this pacer. When the semaphore or the server holds starts
        // back, debt climbs and stays climbing; it is not discarded and re-based, so the graph
        // shows how far behind target the system actually fell.
        var owed = (clock.Elapsed.TotalSeconds * config.TargetRatePerSecond) - issued;
        metrics.RateDebt.Set(Math.Max(0d, owed), AppMetrics.NoTags);

        if (owed < 1d)
        {
            var secondsToNext = (1d - owed) / Math.Max(config.TargetRatePerSecond, 0.0001d);
            var wait = TimeSpan.FromSeconds(Math.Clamp(secondsToNext, 0.005d, 1d));
            await Task.Delay(wait, shutdown.Token);
            continue;
        }

        await inFlightGate.WaitAsync(shutdown.Token);

        var sequence = Interlocked.Increment(ref issued);
        metrics.InFlight.Set(Interlocked.Increment(ref inFlight), AppMetrics.NoTags);
        tracked.Add(RunOneAsync($"job-{runToken}-{sequence}"));
        tracked.RemoveAll(task => task.IsCompleted);
    }
}
catch (OperationCanceledException)
{
    // Ctrl+C or SIGTERM landed on one of the awaits above. Leaving the loop this way is the normal
    // shutdown path, so it falls through to the drain rather than being reported as a failure.
}
finally
{
    // Bounded well under the container's stop_grace_period so shutdown reports what it saw
    // instead of being killed mid-drain.
    var drainBudget = TimeSpan.FromSeconds(20);
    logger.LogInformation(
        "Draining {Count} in-flight workflows (up to {Budget:g})", tracked.Count, drainBudget);
    await Task.WhenAny(Task.WhenAll(tracked), Task.Delay(drainBudget));
    logger.LogInformation("Load generator stopped after issuing {Issued} workflows", issued);
}

async Task RunOneAsync(string workflowId)
{
    try
    {
        var startClock = Stopwatch.StartNew();
        WorkflowHandle<ChunkedJobWorkflow, JobResult> handle;
        try
        {
            handle = await connection.Client.StartWorkflowAsync(
                (ChunkedJobWorkflow workflow) => workflow.RunAsync(config.NewJobInput(workflowId)),
                new(id: workflowId, taskQueue: config.TaskQueue));
        }
        catch (Exception ex)
        {
            metrics.StartFailures.Add(1, AppMetrics.Tag("error_type", ex.GetType().Name));
            logger.LogWarning(ex, "Failed to start workflow {WorkflowId}", workflowId);
            return;
        }

        metrics.StartLatency.Record(startClock.Elapsed, AppMetrics.NoTags);
        metrics.WorkflowsStarted.Add(1, AppMetrics.NoTags);

        var endToEndClock = Stopwatch.StartNew();
        try
        {
            await handle.GetResultAsync();
            metrics.WorkflowsCompleted.Add(1, AppMetrics.Tag("outcome", "completed"));
        }
        catch (Exception ex)
        {
            metrics.WorkflowsCompleted.Add(1, AppMetrics.Tag("outcome", "failed"));
            logger.LogWarning(ex, "Workflow {WorkflowId} did not complete", workflowId);
        }

        metrics.EndToEndDuration.Record(endToEndClock.Elapsed, AppMetrics.NoTags);
    }
    finally
    {
        // Every exit path has to give the slot back, including a start that threw, or the pacer
        // slowly strangles itself against a gate it never reopened.
        metrics.InFlight.Set(Interlocked.Decrement(ref inFlight), AppMetrics.NoTags);
        inFlightGate.Release();
    }
}
