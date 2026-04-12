namespace MqBenchmark.PgMq.Client.Models;

/// <summary>
/// Queue metrics returned by pgmq.metrics() and pgmq.metrics_all().
/// </summary>
public record PgmqMetrics
{
    public required string QueueName { get; init; }
    public required long QueueLength { get; init; }
    public required int NewestMsgAgeSec { get; init; }
    public required int OldestMsgAgeSec { get; init; }
    public required long TotalMessages { get; init; }
    public DateTime? ScrapeTime { get; init; }
}
