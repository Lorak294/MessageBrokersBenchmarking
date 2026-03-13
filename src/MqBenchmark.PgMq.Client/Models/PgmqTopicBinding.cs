namespace MqBenchmark.PgMq.Client.Models;

/// <summary>
/// Represents a topic-to-queue binding returned by pgmq.list_bindings().
/// </summary>
public record PgmqTopicBinding
{
    public required string TopicName { get; init; }
    public required string QueueName { get; init; }
    public required string RoutingKeyPattern { get; init; }
}
