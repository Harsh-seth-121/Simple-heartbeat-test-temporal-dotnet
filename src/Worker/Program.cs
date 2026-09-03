using HeartbeatDemo;
using Microsoft.Extensions.Logging;
using Temporalio.Worker;

// Worker entry point. Polls the task queue, runs the Workflow and the long heartbeating Activity,
// and exposes SDK and custom metrics on the runtime's Prometheus endpoint.
var config = AppConfig.FromEnvironment();

using var loggerFactory = ProcessHost.CreateLoggerFactory();
var logger = loggerFactory.CreateLogger("Worker");

var connection = await TemporalStack.ConnectAsync(config, loggerFactory);

using var worker = new TemporalWorker(
    connection.Client,
    new TemporalWorkerOptions(config.TaskQueue)
    {
        MaxConcurrentActivities = config.MaxConcurrentActivities,

        // Long enough for in-flight Activities to see cancellation and flush a heartbeat, short
        // enough that `docker compose restart worker` stays a usable drill.
        GracefulShutdownTimeout = TimeSpan.FromSeconds(10),
        LoggerFactory = loggerFactory,
    }
        .AddAllActivities(new ChunkProcessingActivities(config))
        .AddWorkflow<ChunkedJobWorkflow>());

using var shutdown = new ShutdownSignal();

logger.LogInformation(
    "Worker polling task queue {TaskQueue} with maxConcurrentActivities={Max}, chaosRate={Chaos}",
    config.TaskQueue,
    config.MaxConcurrentActivities,
    config.ChaosFailureRate);

try
{
    await worker.ExecuteAsync(shutdown.Token);
}
catch (OperationCanceledException)
{
    logger.LogInformation("Worker shut down");
}
