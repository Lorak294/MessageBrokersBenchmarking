using MqBenchmark.PgMq.Client.Models;

namespace MqBenchmark.PgMq.Client;

public interface IPopOperations
{
    Task<IReadOnlyList<PgmqMessage>> PopAsync(string queueName, int qty = 1, CancellationToken ct = default);
}
