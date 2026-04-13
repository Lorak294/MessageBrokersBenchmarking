namespace MqBenchmark.PgMq.Client;

public interface IDeleteOperations
{
    Task<bool> DeleteAsync(string queueName, long msgId, CancellationToken ct = default);
    Task<long[]> DeleteBatchAsync(string queueName, long[] msgIds, CancellationToken ct = default);
}
