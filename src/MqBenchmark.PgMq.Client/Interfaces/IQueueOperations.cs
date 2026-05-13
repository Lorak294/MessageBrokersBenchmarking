using MqBenchmark.PgMq.Client.Models;

namespace MqBenchmark.PgMq.Client;

public interface IQueueOperations
{
    Task CreateAsync(string queueName, bool unlogged = false, CancellationToken ct = default);
    Task<bool> DropAsync(string queueName, CancellationToken ct = default);
    Task<long> PurgeAsync(string queueName, CancellationToken ct = default);
    Task<IReadOnlyList<PgmqQueueInfo>> ListAsync(CancellationToken ct = default);
}
