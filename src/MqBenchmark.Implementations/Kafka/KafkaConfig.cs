using Confluent.Kafka;
using MqBenchmark.Core.Config;

namespace MqBenchmark.Implementations.Kafka;

public static class MqConfigKafkaExtensions
{
    public static KafkaConfig ToKafkaConfig(this MqConfig configuration)
    {
        return new KafkaConfig
        {
            BootstrapServers = configuration.GetRequiredSetting("BootstrapServers"),
            TopicName = configuration.GetRequiredSetting("TopicName"),
            GroupId = configuration.GetRequiredSetting("GroupId"),
            Acks = Enum.Parse<Acks>(configuration.GetOptionalSetting("Acks", "Leader"), ignoreCase: true),
            LingerMs = int.Parse(configuration.GetOptionalSetting("LingerMs", "5")),
            BatchSize = int.Parse(configuration.GetOptionalSetting("BatchSize", "65536")),
            EnableIdempotence = bool.Parse(configuration.GetOptionalSetting("EnableIdempotence", "false")),
            AutoOffsetReset = Enum.Parse<AutoOffsetReset>(configuration.GetOptionalSetting("AutoOffsetReset", "Earliest"), ignoreCase: true),
            EnableAutoCommit = bool.Parse(configuration.GetOptionalSetting("EnableAutoCommit", "true")),
            UseBufferedProducer = bool.Parse(configuration.GetOptionalSetting("UseBufferedProducer", "true")),
            NumPartitions = int.Parse(configuration.GetOptionalSetting("NumPartitions", "1"))
        };
    }
}

public class KafkaConfig
{
    public required string BootstrapServers { get; init; }
    public required string TopicName { get; init; }
    public required string GroupId { get; init; }
    
    /// <summary>
    /// Broker acknowledgment level for produced messages.
    /// None = no ack, Leader = leader ack only, All = all replicas must ack.
    /// Default: Leader.
    /// </summary>
    public Acks Acks { get; init; } = Acks.Leader;
    
    /// <summary>
    /// Milliseconds to buffer messages before sending a batch.
    /// Higher values improve throughput at the cost of latency.
    /// Default: 5.
    /// </summary>
    public int LingerMs { get; init; } = 5;
    
    /// <summary>
    /// Maximum batch size in bytes.
    /// Default: 65536 (64 KB).
    /// </summary>
    public int BatchSize { get; init; } = 65536;
    
    /// <summary>
    /// Enable idempotent producer to ensure exactly-once delivery.
    /// Requires Acks.All.
    /// Default: false.
    /// </summary>
    public bool EnableIdempotence { get; init; } = false;
    
    /// <summary>
    /// Where the consumer starts reading when no committed offset exists.
    /// Earliest = from the beginning, Latest = only new messages.
    /// Default: Earliest.
    /// </summary>
    public AutoOffsetReset AutoOffsetReset { get; init; } = AutoOffsetReset.Earliest;
    
    /// <summary>
    /// Whether the consumer automatically commits offsets.
    /// Default: true.
    /// </summary>
    public bool EnableAutoCommit { get; init; } = true;
    
    /// <summary>
    /// If true, uses Produce() (fire-and-forget into internal buffer) for higher throughput.
    /// If false, uses await ProduceAsync() which waits for broker acknowledgment per message.
    /// Buffered messages are guaranteed to be delivered via Flush() on dispose.
    /// Default: true.
    /// </summary>
    public bool UseBufferedProducer { get; init; } = true;
    
    /// <summary>
    /// Number of partitions to create for the topic.
    /// For PointToPoint mode, set this >= max consumers in any group to ensure all consumers are active.
    /// Default: 1.
    /// </summary>
    public int NumPartitions { get; init; } = 1;
}
