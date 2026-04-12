namespace MqBenchmark.PgMq.Client.Models;

/// <summary>
/// Queue information returned by pgmq.list_queues().
/// </summary>
public record PgmqQueueInfo
{
    public required string QueueName { get; init; }
    public required bool IsPartitioned { get; init; }
    public required bool IsUnlogged { get; init; }
    public required DateTime CreatedAt { get; init; }
}
