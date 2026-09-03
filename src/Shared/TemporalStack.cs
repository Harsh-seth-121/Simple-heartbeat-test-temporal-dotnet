using Microsoft.Extensions.Logging;
using Temporalio.Client;
using Temporalio.Common.EnvConfig;
using Temporalio.Runtime;

namespace HeartbeatDemo;

/// <summary>
/// Builds the process-wide <see cref="TemporalRuntime"/> and a connected client.
/// </summary>
/// <remarks>
/// <para>
/// The runtime owns the Prometheus exporter and must exist before any client is created, so this
/// type is the first thing both entry points touch. There is exactly one runtime per process; it
/// carries a native engine and is not cheap to create.
/// </para>
/// <para>
/// Local and Cloud differ only in environment variables. <c>ClientEnvConfig</c> reads
/// <c>TEMPORAL_ADDRESS</c>, <c>TEMPORAL_NAMESPACE</c>, <c>TEMPORAL_API_KEY</c> and the
/// <c>TEMPORAL_TLS_CLIENT_*</c> pair, so there is no Cloud-specific code path to keep in sync.
/// </para>
/// </remarks>
public sealed class TemporalStack
{
    private TemporalStack(TemporalRuntime runtime, ITemporalClient client)
    {
        Runtime = runtime;
        Client = client;
    }

    /// <summary>Gets the runtime that owns telemetry for this process.</summary>
    public TemporalRuntime Runtime { get; }

    /// <summary>Gets the connected client.</summary>
    public ITemporalClient Client { get; }

    /// <summary>
    /// Creates the runtime, resolves connection options from the environment, and connects.
    /// </summary>
    /// <param name="config">Process configuration.</param>
    /// <param name="loggerFactory">Logger factory for client logs and the startup banner.</param>
    /// <returns>The runtime and connected client.</returns>
    public static async Task<TemporalStack> ConnectAsync(
        AppConfig config, ILoggerFactory loggerFactory)
    {
        var runtime = CreateRuntime(config);
        var options = LoadConnectOptions();

        // Local defaults. Cloud sets these via environment, and env always wins over these lines
        // because ClientEnvConfig has already applied them by this point.
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
    /// <param name="config">Process configuration supplying the bind address.</param>
    /// <returns>The new runtime.</returns>
    /// <remarks>
    /// <para>
    /// <c>UseSecondsForDuration</c> is on, so every duration histogram is float seconds rather than
    /// integer milliseconds. The provisioned Grafana dashboards assume that; turning it off means
    /// rewriting their queries. Counters carry no <c>_total</c> suffix — <c>HasCounterTotalSuffix</c>
    /// is deliberately left alone because it has no observable effect in SDK 1.18.0, and setting it
    /// would only suggest the exported names differ from what they are.
    /// </para>
    /// <para>
    /// The bucket overrides are not cosmetic. The SDK's default histogram buckets stop at 10, which
    /// suits sub-second request latencies and nothing else in this demo: a default job runs ~50s and
    /// resume offsets are measured in items, so without these overrides most samples land in
    /// <c>+Inf</c> and every percentile panel reads as a flat line at the top bucket.
    /// </para>
    /// </remarks>
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

    /// <summary>Seconds. A job is item_count * per_item_millis, and chaos retries multiply that.</summary>
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

    /// <summary>
    /// Connects, retrying while the server is still coming up.
    /// </summary>
    /// <param name="options">Resolved connection options.</param>
    /// <param name="logger">Logger for retry notices.</param>
    /// <returns>The connected client.</returns>
    /// <remarks>
    /// In a Compose stack the server is often mid-schema-setup when our container starts. Retrying
    /// here means the stack converges on its own rather than depending on healthcheck ordering.
    /// </remarks>
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

    /// <summary>
    /// Resolves connection options from the TOML profile and TEMPORAL_* environment variables.
    /// </summary>
    /// <returns>Connection options with blank credentials normalised away.</returns>
    /// <remarks>
    /// The blank-credential handling is not defensive padding, it is load-bearing. TLS
    /// auto-enables as soon as an API key or TLS config looks present, and a variable declared
    /// with an empty value still counts as present. An orchestrator that always passes
    /// <c>TEMPORAL_API_KEY</c> — which is exactly what a Compose file with a Cloud-ready
    /// environment block does — therefore turns TLS on against a plaintext local server, and every
    /// connection fails with <c>InvalidMessage(InvalidContentType)</c>: a TLS handshake answered in
    /// cleartext. Blank means absent here.
    /// <para>
    /// Set <c>TEMPORAL_TLS=true</c> for the one case this would otherwise break: a server that
    /// wants TLS but no client credentials.
    /// </para>
    /// </remarks>
    private static TemporalClientConnectOptions LoadConnectOptions()
    {
        // Reads the TOML profile (if any) plus TEMPORAL_* environment variables, environment
        // taking precedence. Succeeds with defaults when no config file exists.
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
