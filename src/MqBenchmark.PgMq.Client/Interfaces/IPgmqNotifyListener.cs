namespace MqBenchmark.PgMq.Client;

public interface IPgmqNotifyListener : IAsyncDisposable
{
    Task StartAsync(CancellationToken ct = default);
    Task<bool> WaitAsync(TimeSpan timeout, CancellationToken ct = default);
    Task<bool> WaitAsync(CancellationToken ct);
}
