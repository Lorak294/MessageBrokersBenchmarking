using MqBenchmark.PgMq.Client.Models;

namespace MqBenchmark.PgMq.Client;

public interface IReadOperations
{
    Task<IReadOnlyList<PgmqMessage>> ReadAsync(string queueName, int visibilityTimeout, int qty = 1, CancellationToken ct = default);
    Task<IReadOnlyList<PgmqMessage>> ReadWithPollAsync(string queueName, int visibilityTimeout, int qty = 1, int maxPollSeconds = 5, int pollIntervalMs = 100, CancellationToken ct = default);
    Task<IReadOnlyList<PgmqMessage>> ReadFromOffsetAsync(string queueName, long afterMsgId, int qty = 1, CancellationToken ct = default);
}
