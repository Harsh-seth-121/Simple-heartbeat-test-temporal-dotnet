using Microsoft.Extensions.Logging;
using Temporalio.Client;
using Temporalio.Common.EnvConfig;
using Temporalio.Runtime;

namespace HeartbeatDemo;

/// <summary>
/// Builds the process-wide <see cref="TemporalRuntime"/> and a connected client.
/// The runtime owns the Prometheus exporter and must exist before any client is created, so both
/// entry points touch this type first. 
/// </summary>
public sealed class TemporalStack
{
    private TemporalStack(TemporalRuntime runtime, ITemporalClient client)
    {
        Runtime = runtime;
        Client = client;
    }

    public TemporalRuntime Runtime { get; }

    public ITemporalClient Client { get; }

    public static async Task<TemporalStack> ConnectAsync(
        AppConfig config, ILoggerFactory loggerFactory)
    {
        var runtime = CreateRuntime(config);
        var options = LoadConnectOptions();

        if (string.IsNullOrWhiteSpace(options.TargetHost))
        {
            options.TargetHost = "localhost:7233";
        }

        if (string.IsNullOrWhiteSpace(options.Namespace))
        {
            options.Namespace = "default";
        }

        options.Runtime = runtime;
        options.LoggerFactory = loggerFactory;

        var logger = loggerFactory.CreateLogger<TemporalStack>();
        logger.LogInformation(
            "Connecting to Temporal target={Target} address={Address} namespace={Namespace} auth={Auth} metrics={Metrics}",
            config.Target,
            options.TargetHost,
            options.Namespace,
            DescribeAuth(options),
            config.MetricsBindAddress);

        var client = await ConnectWithRetryAsync(options, logger).ConfigureAwait(false);
        logger.LogInformation("Connected to Temporal");
        return new TemporalStack(runtime, client);
    }

    /// <summary>
    /// Creates the runtime with the Prometheus exporter enabled.
    /// </summary>
    /// <param name="config">Process configuration.</param>
    /// <returns>The runtime.</returns>
    public static TemporalRuntime CreateRuntime(AppConfig config) => new(new()
    {
        Telemetry = new()
        {
            Metrics = new()
            {
                Prometheus = new(config.MetricsBindAddress)
                {
                    UseSecondsForDuration = true,
                    HistogramBucketOverrides = HistogramBuckets,
                },
            },
        },
    });

    /// <summary>Seconds. A job is item_count * per_item_millis; chaos retries multiply that.</summary>
    private static readonly IReadOnlyCollection<double> JobDurationSeconds =
        new[] { 1d, 5d, 10d, 30d, 60d, 120d, 300d, 600d, 1_800d };

    /// <summary>Seconds. One Activity execution, which is a whole job's worth of items.</summary>
    private static readonly IReadOnlyCollection<double> ActivityLatencySeconds =
        new[] { 0.1d, 0.5d, 1d, 5d, 10d, 30d, 60d, 120d, 300d, 600d };

    /// <summary>
    /// Histogram bucket boundaries for the metrics whose range the SDK defaults do not cover.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, IReadOnlyCollection<double>> HistogramBuckets =
        new Dictionary<string, IReadOnlyCollection<double>>
        {
            // Items, not seconds. Bounded in practice by JOB_ITEM_COUNT.
            ["heartbeat_demo_resume_offset"] =
                new[] { 1d, 5d, 10d, 25d, 50d, 100d, 200d, 500d, 1_000d, 5_000d },

            ["loadgen_e2e_duration"] = JobDurationSeconds,
            ["temporal_workflow_endtoend_latency"] = JobDurationSeconds,
            ["temporal_activity_execution_latency"] = ActivityLatencySeconds,
            ["temporal_activity_succeed_endtoend_latency"] = ActivityLatencySeconds,

            // Seconds, sub-second in the healthy case; finer at the bottom than the default set.
            ["loadgen_start_latency"] =
                new[] { 0.005d, 0.01d, 0.025d, 0.05d, 0.1d, 0.25d, 0.5d, 1d, 2.5d, 5d, 10d },
        };

    private static async Task<ITemporalClient> ConnectWithRetryAsync(
        TemporalClientConnectOptions options, ILogger logger)
    {
        const int maxAttempts = 30;
        var delay = TimeSpan.FromSeconds(2);

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await TemporalClient.ConnectAsync(options).ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt < maxAttempts)
            {
                logger.LogWarning(
                    "Temporal not reachable yet (attempt {Attempt}/{MaxAttempts}): {Message}",
                    attempt,
                    maxAttempts,
                    ex.Message);
                await Task.Delay(delay).ConfigureAwait(false);
            }
        }
    }

    private static TemporalClientConnectOptions LoadConnectOptions()
    {
        // TOML profile (if any) plus TEMPORAL_* environment variables, environment winning.
        // Succeeds with defaults when no config file exists.
        var loaded = ClientEnvConfig.LoadClientConnectOptions(new ClientEnvConfig.ProfileLoadOptions());

        if (string.IsNullOrWhiteSpace(loaded.ApiKey))
        {
            loaded.ApiKey = null;
        }

        var hasClientCert = loaded.Tls?.ClientCert is { Length: > 0 };
        var tlsExplicitlyRequested = string.Equals(
            Environment.GetEnvironmentVariable("TEMPORAL_TLS")?.Trim(),
            "true",
            StringComparison.OrdinalIgnoreCase);

        if (loaded.ApiKey is null && !hasClientCert && !tlsExplicitlyRequested)
        {
            loaded.Tls = null;
        }

        return loaded;
    }

    private static string DescribeAuth(TemporalClientConnectOptions options)
    {
        if (!string.IsNullOrEmpty(options.ApiKey))
        {
            return "api-key";
        }

        if (options.Tls?.ClientCert is { Length: > 0 })
        {
            return "mtls";
        }

        return options.Tls is null ? "none" : "tls";
    }
}
