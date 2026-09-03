using HeartbeatDemo;
using Temporalio.Client;
using Temporalio.Testing;
using Temporalio.Worker;
using Xunit;

namespace HeartbeatDemo.Tests;

/// <summary>
/// End-to-end coverage against a real local Temporal server. These use
/// <see cref="WorkflowEnvironment.StartLocalAsync"/> rather than the time-skipping environment on
/// purpose: heartbeat timeouts and heartbeat throttling are wall-clock behaviours, and skipping
/// time past them produces retry patterns that do not happen in production. Jobs here are tiny so
/// real time stays cheap.
/// </summary>
public class WorkflowTests
{
    private static async Task<JobResult> RunJobAsync(AppConfig config, JobInput input)
    {
        await using var env = await WorkflowEnvironment.StartLocalAsync();

        using var worker = new TemporalWorker(
            env.Client,
            new TemporalWorkerOptions(config.TaskQueue)
                .AddAllActivities(new ChunkProcessingActivities(config))
                .AddWorkflow<ChunkedJobWorkflow>());

        return await worker.ExecuteAsync(async () =>
            await env.Client.ExecuteWorkflowAsync(
                (ChunkedJobWorkflow workflow) => workflow.RunAsync(input),
                new(id: $"wf-{Guid.NewGuid():N}", taskQueue: config.TaskQueue)));
    }

    [Fact]
    public async Task NoChaos_CompletesOnFirstAttemptWithoutResuming()
    {
        var config = new AppConfig { TaskQueue = "test-no-chaos", ChaosFailureRate = 0 };
        var input = new JobInput("clean", ItemCount: 20, PerItemMillis: 5, HeartbeatTimeoutSeconds: 5);

        var result = await RunJobAsync(config, input);

        Assert.Equal(1, result.Attempts);
        Assert.Equal(0, result.ResumedFrom);
        Assert.Equal(20, result.Processed);
    }

    [Fact]
    public async Task WithChaos_StillCompletesAndNeverLosesOrInventsItems()
    {
        var config = new AppConfig { TaskQueue = "test-chaos", ChaosFailureRate = 0.25 };
        var input = new JobInput("chaotic", ItemCount: 20, PerItemMillis: 5, HeartbeatTimeoutSeconds: 5);

        var result = await RunJobAsync(config, input);

        // Retries change which attempt finishes and where it started, but reported progress
        // still adds up to exactly the job size.
        Assert.Equal(20, result.Processed);
        Assert.Equal(0, result.Failed);
        Assert.True(result.Attempts >= 1);
        Assert.InRange(result.ResumedFrom, 0, 20);
    }
}
