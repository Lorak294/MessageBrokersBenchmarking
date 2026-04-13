using MqBenchmark.Core.Config;

namespace MqBenchmark.Implementations.PgMq;

public static class MqConfigPgMqExtensions
{
    public static PgMqConfig ToPgMqConfig(this MqConfig configuration)
    {
        return new PgMqConfig
        {
            ConnectionString = configuration.GetRequiredSetting("connectionString"),
            VisibilityTimeout = int.Parse(configuration.GetOptionalSetting("visibilityTimeout", "30")),
            QueueMode = Enum.Parse<PgMqConfig.QueueModeEnum>(
                configuration.GetOptionalSetting("queueMode", "Default")),
            MessageReadMode = Enum.Parse<PgMqConfig.ReadModeEnum>(
                configuration.GetOptionalSetting("messageReadMode", "Delete")),
            ConsumerMode = Enum.Parse<PgMqConfig.ConsumerModeEnum>(
                configuration.GetOptionalSetting("consumerMode", "ClientPoll")),
            PollIntervalMs = int.Parse(configuration.GetOptionalSetting("pollIntervalMs", "5")),
            MaxPollSeconds = int.Parse(configuration.GetOptionalSetting("maxPollSeconds", "5")),
            NotifyThrottleMs = int.Parse(configuration.GetOptionalSetting("notifyThrottleMs", "5")),
            UsePop = bool.Parse(configuration.GetOptionalSetting("usePop", "true")),
            UseBufferedProducer = bool.Parse(configuration.GetOptionalSetting("useBufferedProducer", "false")),
            ProducerBatchSize = int.Parse(configuration.GetOptionalSetting("producerBatchSize", "100")),
            ProducerLingerMs = int.Parse(configuration.GetOptionalSetting("producerLingerMs", "5")),
            ConsumerBatchSize = int.Parse(configuration.GetOptionalSetting("consumerBatchSize", "1"))
        };
    }
}

public record PgMqConfig
{
    public required string ConnectionString { get; init; }
    public required int VisibilityTimeout { get; init; }
    public required QueueModeEnum QueueMode { get; init; }
    public required ReadModeEnum MessageReadMode { get; init; }

    /// <summary>Consumer strategy. Default: ClientPoll.</summary>
    public ConsumerModeEnum ConsumerMode { get; init; } = ConsumerModeEnum.ClientPoll;

    /// <summary>Polling interval in ms when no messages are available. Default: 5ms.</summary>
    public int PollIntervalMs { get; init; }

    /// <summary>Maximum seconds to block in server-side polling. Default: 5.</summary>
    public int MaxPollSeconds { get; init; }

    /// <summary>Throttle interval in ms for LISTEN/NOTIFY notifications. Default: 5ms.</summary>
    public int NotifyThrottleMs { get; init; }

    /// <summary>Use pgmq.pop() for atomic read+delete. Default: true.</summary>
    public bool UsePop { get; init; } = true;

    /// <summary>Buffer messages and send in batches. Default: false.</summary>
    public bool UseBufferedProducer { get; init; } = false;

    /// <summary>Batch size for buffered producer. Default: 100.</summary>
    public int ProducerBatchSize { get; init; }

    /// <summary>Linger time in ms for buffered producer. Default: 5.</summary>
    public int ProducerLingerMs { get; init; }

    /// <summary>Messages to read per consumer round-trip. Default: 1.</summary>
    public int ConsumerBatchSize { get; init; }

    public enum QueueModeEnum { Default, Unlogged }
    public enum ReadModeEnum { Delete, Archive }
    public enum ConsumerModeEnum { ClientPoll, ServerPoll, ListenNotify }
}

/// <summary>
/// Auto-generated resource naming conventions for PGMQ.
/// </summary>
public static class PgMqNaming
{
    private const string Base = "benchmark";
    
    /// <summary>Queue name for a specific consumer group (PointToPoint and PubSub).</summary>
    public static string GroupQueue(string groupName) => $"{Base}_{groupName}";
    
    /// <summary>Shared queue name (Streaming mode).</summary>
    public static string StreamQueue() => $"{Base}_stream";
    
    /// <summary>Topic routing key for a specific group (PointToPoint).</summary>
    public static string GroupRoutingKey(string groupName) => groupName;
    
    /// <summary>Broadcast routing key (PubSub) — all queues subscribe to this.</summary>
    public static string BroadcastRoutingKey() => "broadcast";
}
