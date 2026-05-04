using MqBenchmark.Core.Config;

namespace MqBenchmark.Implementations.PgMq;

public static class MqConfigPgMqExtensions
{
    public static PgMqConfig ToPgMqConfig(this MqConfig configuration)
    {
        return new PgMqConfig
        {
            QueueName = configuration.GetRequiredSetting("QueueName"),
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
            RoutingKey = configuration.GetOptionalSetting("RoutingKey", "")
        };
    }
}

public record PgMqConfig
{
    public required string ConnectionString { get; init; }
    public required string QueueName { get; init; }
    public required int VisibilityTimeout { get; init; }

    public required QueueModeEnum QueueMode { get; init; }
    public required ReadModeEnum MessageReadMode { get; init; }

    /// <summary>
    /// Consumer strategy: ClientPoll (Task.Delay loop), ServerPoll (read_with_poll),
    /// or ListenNotify (LISTEN/NOTIFY + pop + fallback sweep).
    /// Default: ClientPoll (backward-compatible with existing behavior).
    /// </summary>
    public ConsumerModeEnum ConsumerMode { get; init; } = ConsumerModeEnum.ClientPoll;

    /// <summary>
    /// Polling interval in milliseconds when no messages are available (ClientPoll mode).
    /// Also used as poll_interval_ms parameter for ServerPoll mode. Default: 100ms.
    /// </summary>
    public int PollIntervalMs { get; init; } = 100;

    /// <summary>
    /// Maximum seconds to block in server-side polling (ServerPoll mode).
    /// Maps to read_with_poll's max_poll_seconds parameter. Default: 5.
    /// </summary>
    public int MaxPollSeconds { get; init; } = 5;

    /// <summary>
    /// Throttle interval in milliseconds for LISTEN/NOTIFY insert notifications.
    /// Prevents notification storms during bulk inserts. Default: 250ms.
    /// </summary>
    public int NotifyThrottleMs { get; init; } = 250;

    /// <summary>
    /// Delay in seconds before messages become visible after sending.
    /// 0 means immediate visibility. Default: 0.
    /// </summary>
    public int DelaySeconds { get; init; } = 0;

    /// <summary>
    /// When true and MessageReadMode is Delete, use pgmq.pop() for atomic read+delete in one round-trip.
    /// When false or when MessageReadMode is Archive, use read()+delete()/archive().
    /// Default: true.
    /// </summary>
    public bool UsePop { get; init; } = true;

    /// <summary>
    /// Routing key for PubSub mode using PGMQ topics.
    /// Messages sent with this key will be delivered to all queues bound with a matching pattern.
    /// </summary>
    public string RoutingKey { get; init; } = "";

    public enum QueueModeEnum
    {
        NonPartitioned,
        Unlogged
    }

    public enum ReadModeEnum
    {
        Delete,
        Archive
    }

    public enum ConsumerModeEnum
    {
        /// <summary>
        /// Client-side polling with Task.Delay between reads. Simple but adds latency equal to poll interval.
        /// </summary>
        ClientPoll,

        /// <summary>
        /// Server-side long-polling via pgmq.read_with_poll(). PostgreSQL blocks and retries internally.
        /// More efficient than client-side polling — lower latency, fewer round-trips.
        /// </summary>
        ServerPoll,

        /// <summary>
        /// Event-driven via PostgreSQL LISTEN/NOTIFY. Dedicated connection listens for insert notifications,
        /// then pops messages on the main connection. Includes periodic fallback sweep to catch messages
        /// missed between throttled notifications.
        /// </summary>
        ListenNotify
    }
}
