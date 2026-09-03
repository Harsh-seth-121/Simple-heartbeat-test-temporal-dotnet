using HeartbeatDemo;
using Temporalio.Activities;
using Temporalio.Api.Common.V1;
using Temporalio.Converters;
using Temporalio.Exceptions;
using Temporalio.Testing;
using Xunit;

namespace HeartbeatDemo.Tests;

/// <summary>
/// Covers the Activity in isolation: fresh runs, resume from a seeded checkpoint, cancellation and
/// chaos. <see cref="ActivityEnvironment"/> populates the Activity context without a server, so
/// the heartbeat callback and heartbeat details are both controlled directly.
/// </summary>
public class ActivityHeartbeatTests
{
    private static AppConfig Config(double chaosRate = 0d) =>
        new() { ChaosFailureRate = chaosRate };

    private static JobInput Job(int itemCount, int perItemMillis = 1) =>
        new("test-job", itemCount, perItemMillis, HeartbeatTimeoutSeconds: 10);

    /// <summary>Folds item indices the same way the Activity does, for checksum assertions.</summary>
    private static long ChecksumThrough(int exclusiveEnd)
    {
        var checksum = 0L;
        for (var i = 0; i < exclusiveEnd; i++)
        {
            checksum = unchecked(checksum + ((long)i * 31) + 7);
        }

        return checksum;
    }

    [Fact]
    public async Task FreshRun_ProcessesEveryItemOnceAndHeartbeatsEach()
    {
        var heartbeats = new List<Checkpoint>();
        var env = new ActivityEnvironment
        {
            Heartbeater = details => heartbeats.Add((Checkpoint)details[0]!),
        };
        var activities = new ChunkProcessingActivities(Config());

        var result = await env.RunAsync(() => activities.ProcessChunksAsync(Job(10)));

        Assert.Equal(10, result.Processed);
        Assert.Equal(0, result.Failed);
        Assert.Equal(0, result.ResumedFrom);
        Assert.Equal(ChecksumThrough(10), result.Checksum);

        // One heartbeat per item, each reporting absolute progress.
        Assert.Equal(10, heartbeats.Count);
        Assert.Equal(10, heartbeats[^1].NextIndex);
        Assert.Equal(Enumerable.Range(1, 10), heartbeats.Select(h => h.NextIndex));
    }

    [Fact]
    public async Task Resume_StartsFromSeededCheckpointAndFinishesTheJob()
    {
        const int resumeAt = 6;
        const int itemCount = 10;

        var seeded = new Checkpoint(resumeAt, resumeAt, 0, ChecksumThrough(resumeAt));
        var payload = DataConverter.Default.PayloadConverter.ToPayload(seeded);

        var heartbeats = new List<Checkpoint>();
        var env = new ActivityEnvironment
        {
            Heartbeater = details => heartbeats.Add((Checkpoint)details[0]!),
            Info = ActivityEnvironment.DefaultInfo with
            {
                Attempt = 2,
                HeartbeatDetails = new List<Payload> { payload },
            },
        };
        var activities = new ChunkProcessingActivities(Config());

        var result = await env.RunAsync(() => activities.ProcessChunksAsync(Job(itemCount)));

        Assert.Equal(resumeAt, result.ResumedFrom);
        Assert.Equal(2, result.Attempts);

        // Total processed equals the job size, wherever the winning attempt picked up.
        Assert.Equal(itemCount, result.Processed);
        Assert.Equal(ChecksumThrough(itemCount), result.Checksum);

        // Only the remaining items were touched.
        Assert.Equal(itemCount - resumeAt, heartbeats.Count);
        Assert.Equal(resumeAt + 1, heartbeats[0].NextIndex);
    }

    [Fact]
    public async Task Cancellation_StopsPromptlyAndLeavesProgressInTheLastHeartbeat()
    {
        var heartbeats = new List<Checkpoint>();
        var env = new ActivityEnvironment
        {
            Heartbeater = details => heartbeats.Add((Checkpoint)details[0]!),
        };
        var activities = new ChunkProcessingActivities(Config());

        var run = env.RunAsync(() => activities.ProcessChunksAsync(Job(200, perItemMillis: 20)));

        // Let a few items land, then cancel as a Workflow cancellation would.
        await Task.Delay(200);
        env.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);

        Assert.NotEmpty(heartbeats);
        var last = heartbeats[^1];
        Assert.InRange(last.NextIndex, 1, 199);
        Assert.Equal(last.NextIndex, last.Processed);
    }

    [Fact]
    public async Task WorkerShutdown_FailsRetryablyRatherThanCancelling()
    {
        var env = new ActivityEnvironment();
        var activities = new ChunkProcessingActivities(Config());

        var run = env.RunAsync(() => activities.ProcessChunksAsync(Job(200, perItemMillis: 20)));

        await Task.Delay(200);
        env.Cancel(ActivityCancelReason.WorkerShutdown);

        // A Worker going away must not surface as a cancelled Activity, or the Workflow gives up
        // instead of retrying onto another Worker.
        var failure = await Assert.ThrowsAsync<ApplicationFailureException>(() => run);
        Assert.Equal("WorkerShutdown", failure.ErrorType);
    }

    [Fact]
    public async Task Chaos_AtFullRate_FailsTheAttempt()
    {
        var env = new ActivityEnvironment();
        var activities = new ChunkProcessingActivities(Config(chaosRate: 1.0));

        var failure = await Assert.ThrowsAsync<ApplicationFailureException>(
            () => env.RunAsync(() => activities.ProcessChunksAsync(Job(10))));

        Assert.Equal("ChaosFailure", failure.ErrorType);
    }
}
