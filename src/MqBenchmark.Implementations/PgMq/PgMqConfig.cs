using MqBenchmark.Core.Config;

namespace MqBenchmark.Implementations.PgMq;

public static class MqConfigPgMqExtensions
{
    public static PgMqConfig ToPgMqConfig(this MqConfig configuration)
    {
        return new PgMqConfig
        {
            ConnectionString = configuration.GetRequiredSetting("ConnectionString"),
            VisibilityTimeout = int.Parse(configuration.GetOptionalSetting("VisibilityTimeout", "30")),
            QueueMode = Enum.Parse<PgMqConfig.QueueModeEnum>(
                configuration.GetOptionalSetting("QueueMode", "NonPartitioned")),
            MessageReadMode = Enum.Parse<PgMqConfig.ReadModeEnum>(
                configuration.GetOptionalSetting("MessageReadMode", "Delete")),
            ConsumerMode = Enum.Parse<PgMqConfig.ConsumerModeEnum>(
                configuration.GetOptionalSetting("ConsumerMode", "ClientPoll")),
            PollIntervalMs = int.Parse(configuration.GetOptionalSetting("PollIntervalMs", "100")),
            MaxPollSeconds = int.Parse(configuration.GetOptionalSetting("MaxPollSeconds", "5")),
            NotifyThrottleMs = int.Parse(configuration.GetOptionalSetting("NotifyThrottleMs", "250")),
            DelaySeconds = int.Parse(configuration.GetOptionalSetting("DelaySeconds", "0")),
            UsePop = bool.Parse(configuration.GetOptionalSetting("UsePop", "true")),
            UseBufferedProducer = bool.Parse(configuration.GetOptionalSetting("UseBufferedProducer", "false")),
            ProducerBatchSize = int.Parse(configuration.GetOptionalSetting("ProducerBatchSize", "100")),
            ProducerLingerMs = int.Parse(configuration.GetOptionalSetting("ProducerLingerMs", "5")),
            ConsumerBatchSize = int.Parse(configuration.GetOptionalSetting("ConsumerBatchSize", "1"))
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

    /// <summary>Polling interval in ms when no messages are available. Default: 100ms.</summary>
    public int PollIntervalMs { get; init; } = 100;

    /// <summary>Maximum seconds to block in server-side polling. Default: 5.</summary>
    public int MaxPollSeconds { get; init; } = 5;

    /// <summary>Throttle interval in ms for LISTEN/NOTIFY notifications. Default: 10ms.</summary>
    public int NotifyThrottleMs { get; init; } = 10;

    /// <summary>Delay in seconds before messages become visible. Default: 0.</summary>
    public int DelaySeconds { get; init; } = 0;

    /// <summary>Use pgmq.pop() for atomic read+delete. Default: true.</summary>
    public bool UsePop { get; init; } = true;

    /// <summary>Buffer messages and send in batches. Default: false.</summary>
    public bool UseBufferedProducer { get; init; } = false;

    /// <summary>Batch size for buffered producer. Default: 100.</summary>
    public int ProducerBatchSize { get; init; } = 100;

    /// <summary>Linger time in ms for buffered producer. Default: 5.</summary>
    public int ProducerLingerMs { get; init; } = 5;

    /// <summary>Messages to read per consumer round-trip. Default: 1.</summary>
    public int ConsumerBatchSize { get; init; } = 1;

    public enum QueueModeEnum { NonPartitioned, Unlogged }
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
