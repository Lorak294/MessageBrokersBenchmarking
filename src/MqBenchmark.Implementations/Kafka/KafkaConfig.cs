using Confluent.Kafka;
using MqBenchmark.Core.Config;

namespace MqBenchmark.Implementations.Kafka;


public static class KafkaConstants
{
    public const int FlushTimeoutMs = 500;
    public const int ConsumePollTimeoutMs = 500;
    public const int PartitionAssignmentTimeoutSeconds = 15;
}

public static class MqConfigKafkaExtensions
{
    public static KafkaConfig ToKafkaConfig(this MqConfig configuration)
    {
        return new KafkaConfig
        {
            BootstrapServers = configuration.GetRequiredSetting("bootstrapServers"),
            LingerMs = int.Parse(configuration.GetOptionalSetting("lingerMs", "5")),
            BatchSize = int.Parse(configuration.GetOptionalSetting("batchSize", "65536")),
            UseBufferedProducer = bool.Parse(configuration.GetOptionalSetting("useBufferedProducer", "true"))
        };
    }
}

public class KafkaConfig
{
    public required string BootstrapServers { get; init; }
    
    /// <summary>Milliseconds to buffer messages before sending. Default: 5.</summary>
    public int LingerMs { get; init; } = 5;
    
    /// <summary>Maximum batch size in bytes. Default: 65536.</summary>
    public int BatchSize { get; init; } = 65536;
    
    /// <summary>
    /// If true, uses Produce() (fire-and-forget into buffer) for higher throughput.
    /// If false, uses await ProduceAsync(). Default: true.
    /// </summary>
    public bool UseBufferedProducer { get; init; } = true;
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
