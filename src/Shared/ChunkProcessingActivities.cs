using Microsoft.Extensions.Logging;
using Temporalio.Activities;
using Temporalio.Exceptions;

namespace HeartbeatDemo;

/// <summary>
/// The long-running Activity.
/// </summary>
/// <param name="config">Process configuration, used only for the chaos failure rate.</param>
public sealed class ChunkProcessingActivities(AppConfig config)
{
    /// <summary>
    /// Processes items one at a time, heartbeating a full progress checkpoint after each, and
    /// resuming from the last checkpoint when a previous attempt died.
    /// </summary>
    [Activity]
    public async Task<JobResult> ProcessChunksAsync(JobInput input)
    {
        var ctx = ActivityExecutionContext.Current;
        var metrics = new ActivityMetrics(ctx.MetricMeter);

        // Attempt 1 has no heartbeat details. Later attempts get whatever the server retained
        // from the previous attempt's last flushed heartbeat.
        Checkpoint? resumeFrom = null;
        if (ctx.Info.HeartbeatDetails.Count > 0)
        {
            resumeFrom = await ctx.Info.HeartbeatDetailAtAsync<Checkpoint>(0).ConfigureAwait(false);
        }

        // The checkpoint holds all of this attempt's progress state; startIndex only reports
        // where the attempt began.
        var checkpoint = resumeFrom ?? new Checkpoint(0, 0, 0, 0L);
        var startIndex = checkpoint.NextIndex;

        metrics.AttemptStarted.Add(
            1, AppMetrics.Tag("resumed", resumeFrom is not null ? "true" : "false"));

        if (resumeFrom is not null)
        {
            metrics.ResumeOffset.Record(startIndex, AppMetrics.NoTags);
            ctx.Logger.LogInformation(
                "Resuming job {JobId} on attempt {Attempt} from item {StartIndex} of {ItemCount}",
                input.JobId,
                ctx.Info.Attempt,
                startIndex,
                input.ItemCount);
        }

        try
        {
            for (var i = startIndex; i < input.ItemCount; i++)
            {
                // Heartbeats throttle to roughly 80% of the heartbeat timeout, so items between
                // the last flushed heartbeat and a crash get replayed. Per-item work must be
                // idempotent.
                await Task.Delay(input.PerItemMillis, ctx.CancellationToken).ConfigureAwait(false);

                if (config.ChaosFailureRate > 0 && Random.Shared.NextDouble() < config.ChaosFailureRate)
                {
                    // Thrown after the work and before the heartbeat, where a real crash loses
                    // progress. Throwing after the heartbeat would make the demo lie.
                    metrics.AttemptFailed.Add(1, AppMetrics.Tag("reason", "chaos"));
                    ctx.Logger.LogWarning(
                        "Chaos failure in job {JobId} at item {Index} (attempt {Attempt})",
                        input.JobId,
                        i,
                        ctx.Info.Attempt);
                    throw new ApplicationFailureException(
                        $"Injected chaos failure at item {i}", "ChaosFailure");
                }

                metrics.ItemsProcessed.Add(1, AppMetrics.NoTags);

                checkpoint = checkpoint with
                {
                    NextIndex = i + 1,
                    Processed = checkpoint.Processed + 1,
                    Checksum = unchecked(checkpoint.Checksum + ((long)i * 31) + 7),
                };
                ctx.Heartbeat(checkpoint);
                metrics.HeartbeatsSent.Add(1, AppMetrics.NoTags);
            }
        }
        catch (OperationCanceledException)
        {
            // No final heartbeat on purpose: the loop already heartbeat after the last completed
            // item, and the item in flight when cancellation landed is not complete.
            var reason = ctx.CancelReason;
            metrics.AttemptFailed.Add(1, AppMetrics.Tag("reason", reason.ToString()));
            ctx.Logger.LogInformation(
                "Job {JobId} attempt {Attempt} cancelled at item {NextIndex}, reason {Reason}",
                input.JobId,
                ctx.Info.Attempt,
                checkpoint.NextIndex,
                reason);

            if (reason == ActivityCancelReason.WorkerShutdown)
            {
                // A Worker going away is not a business cancellation. Fail retryably so the next
                // Worker resumes from the checkpoint instead of the Workflow seeing a cancelled
                // Activity.
                throw new ApplicationFailureException(
                    $"Worker shut down at item {checkpoint.NextIndex}", "WorkerShutdown");
            }

            throw;
        }

        ctx.Logger.LogInformation(
            "Completed job {JobId} on attempt {Attempt}: processed={Processed} failed={Failed} resumedFrom={ResumedFrom}",
            input.JobId,
            ctx.Info.Attempt,
            checkpoint.Processed,
            checkpoint.Failed,
            startIndex);

        return new JobResult(
            input.JobId, checkpoint.Processed, checkpoint.Failed,
            ctx.Info.Attempt, startIndex, checkpoint.Checksum);
    }
}
