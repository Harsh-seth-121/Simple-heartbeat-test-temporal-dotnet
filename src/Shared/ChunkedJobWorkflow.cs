using Temporalio.Common;
using Temporalio.Workflows;

namespace HeartbeatDemo;

/// <summary>
/// Runs one long heartbeating Activity and reports its outcome.
/// </summary>
/// <remarks>
/// Deliberately thin. The interesting behaviour lives in the Activity; the Workflow's only job is
/// to set timeouts and a retry policy that let a dead attempt be replaced by one that resumes.
/// </remarks>
[Workflow]
public class ChunkedJobWorkflow
{
    /// <summary>Runs the job.</summary>
    /// <param name="input">The job to run.</param>
    /// <returns>The job outcome.</returns>
    [WorkflowRun]
    public async Task<JobResult> RunAsync(JobInput input)
    {
        var metrics = new WorkflowMetrics(Workflow.MetricMeter);
        metrics.JobItemsRequested.Add(input.ItemCount, AppMetrics.NoTags);

        // Per-attempt budget: enough for a from-scratch run plus headroom. The heartbeat timeout,
        // not this, is what detects a dead Worker quickly; StartToClose is the outer backstop and
        // must stay the larger of the two.
        var perAttemptBudget =
            TimeSpan.FromMilliseconds((double)input.ItemCount * input.PerItemMillis) +
            TimeSpan.FromSeconds(60);

        try
        {
            var result = await Workflow.ExecuteActivityAsync(
                (ChunkProcessingActivities a) => a.ProcessChunksAsync(input),
                new()
                {
                    StartToCloseTimeout = perAttemptBudget,
                    HeartbeatTimeout = TimeSpan.FromSeconds(input.HeartbeatTimeoutSeconds),
                    RetryPolicy = new RetryPolicy
                    {
                        InitialInterval = TimeSpan.FromSeconds(1),
                        BackoffCoefficient = 1.5f,
                        MaximumInterval = TimeSpan.FromSeconds(10),

                        // Unlimited: injected chaos should delay a job, never fail it, so the
                        // dashboards show retry-and-resume rather than a growing failure count.
                        MaximumAttempts = 0,
                    },
                }).ConfigureAwait(true);

            metrics.JobCompleted.Add(1, AppMetrics.Tag("outcome", "completed"));
            return result;
        }
        catch (Exception)
        {
            // Reached only via cancellation or termination, since Activity retries are unlimited.
            metrics.JobCompleted.Add(1, AppMetrics.Tag("outcome", "failed"));
            throw;
        }
    }
}
