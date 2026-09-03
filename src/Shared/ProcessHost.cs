using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace HeartbeatDemo;

/// <summary>
/// Startup plumbing shared by the Worker and load generator entry points.
/// </summary>
public static class ProcessHost
{
    public static ILoggerFactory CreateLoggerFactory() => LoggerFactory.Create(builder => builder
        .AddSimpleConsole(options =>
        {
            options.SingleLine = true;
            options.TimestampFormat = "HH:mm:ss.fff ";
        })
        .SetMinimumLevel(LogLevel.Information));
}

/// <summary>
/// Cancellation token that trips on Ctrl+C or SIGTERM.
/// </summary>
public sealed class ShutdownSignal : IDisposable
{
    private readonly CancellationTokenSource cts = new();
    private readonly ConsoleCancelEventHandler ctrlCHandler;
    private readonly PosixSignalRegistration sigterm;

    public ShutdownSignal()
    {
        ctrlCHandler = (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            cts.Cancel();
        };
        Console.CancelKeyPress += ctrlCHandler;
        sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, context =>
        {
            context.Cancel = true;
            cts.Cancel();
        });
    }

    public CancellationToken Token => cts.Token;


    public void Dispose()
    {
        Console.CancelKeyPress -= ctrlCHandler;
        sigterm.Dispose();
        cts.Dispose();
    }
}
