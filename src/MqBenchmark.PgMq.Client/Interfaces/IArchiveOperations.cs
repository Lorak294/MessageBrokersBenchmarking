namespace MqBenchmark.PgMq.Client;

public interface IArchiveOperations
{
    Task<bool> ArchiveAsync(string queueName, long msgId, CancellationToken ct = default);
    Task<long[]> ArchiveBatchAsync(string queueName, long[] msgIds, CancellationToken ct = default);
}
