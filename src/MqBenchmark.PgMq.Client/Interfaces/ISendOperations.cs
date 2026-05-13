namespace MqBenchmark.PgMq.Client;

public interface ISendOperations
{
    Task<long> SendAsync(string queueName, byte[] payload, CancellationToken ct = default);
    Task<long> SendAsync(string queueName, byte[] payload, int delaySeconds, CancellationToken ct = default);
    Task<long[]> SendBatchAsync(string queueName, IReadOnlyList<byte[]> payloads, CancellationToken ct = default);
    Task<long[]> SendBatchAsync(string queueName, IReadOnlyList<byte[]> payloads, int delaySeconds, CancellationToken ct = default);
}
