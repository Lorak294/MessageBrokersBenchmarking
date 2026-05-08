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
    
    /// <summary>Broker acknowledgment level. Default: Leader.</summary>
    public Acks Acks { get; init; } = Acks.Leader;
    
    /// <summary>Milliseconds to buffer messages before sending. Default: 5.</summary>
    public int LingerMs { get; init; } = 5;
    
    /// <summary>Maximum batch size in bytes. Default: 65536.</summary>
    public int BatchSize { get; init; } = 65536;
    
    /// <summary>Enable idempotent producer (requires Acks.All). Default: false.</summary>
    public bool EnableIdempotence { get; init; } = false;
    
    /// <summary>Where consumer starts when no committed offset exists. Default: Earliest.</summary>
    public AutoOffsetReset AutoOffsetReset { get; init; } = AutoOffsetReset.Earliest;
    
    /// <summary>Whether consumer automatically commits offsets. Default: true.</summary>
    public bool EnableAutoCommit { get; init; } = true;
    
    /// <summary>
    /// If true, uses Produce() (fire-and-forget into buffer) for higher throughput.
    /// If false, uses await ProduceAsync(). Default: true.
    /// </summary>
    public bool UseBufferedProducer { get; init; } = true;
    
    /// <summary>
    /// Number of partitions per topic. For PointToPoint, set >= max consumers in a group.
    /// Default: 1.
    /// </summary>
    public int NumPartitions { get; init; } = 1;
}

/// <summary>
/// Auto-generated resource naming conventions for Kafka.
/// </summary>
public static class KafkaNaming
{
    private const string Base = "benchmark";
    
    /// <summary>Topic name for a specific consumer group (PointToPoint mode).</summary>
    public static string GroupTopic(string groupName) => $"{Base}_{groupName}";
    
    /// <summary>Shared topic name (PubSub and Streaming modes).</summary>
    public static string SharedTopic() => Base;
    
    /// <summary>Consumer group ID for a specific group.</summary>
    public static string GroupId(string groupName) => $"{Base}_{groupName}";
    
    /// <summary>Shared consumer group ID (PointToPoint — competing consumers on same group topic).</summary>
    public static string SharedGroupId(string groupName) => $"{Base}_{groupName}";
}
