namespace MqBenchmark.PgMq.Client;

public interface INotifyOperations
{
    Task EnableAsync(string queueName, int throttleIntervalMs = 250, CancellationToken ct = default);
    Task DisableAsync(string queueName, CancellationToken ct = default);
    IPgmqNotifyListener CreateListener(string queueName);
}
