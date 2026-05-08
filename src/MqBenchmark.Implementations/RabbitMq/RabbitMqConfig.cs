using MqBenchmark.Core.Config;

namespace MqBenchmark.Implementations.RabbitMq;

public static class MqConfigRabbitMqExtensions
{
    public static RabbitMqConfig ToRabbitMqConfig(this MqConfig configuration)
    {
        return new RabbitMqConfig
        {
            Hostname = configuration.GetRequiredSetting("Hostname"),
            Port = int.Parse(configuration.GetRequiredSetting("Port")),
            Username = configuration.GetRequiredSetting("Username"),
            Password = configuration.GetRequiredSetting("Password"),
            DurableMode = bool.Parse(configuration.GetOptionalSetting("DurableMode", "false")),
            PrefetchCount = ushort.Parse(configuration.GetOptionalSetting("PrefetchCount", "100")),
            ConsumerDispatchConcurrency = ushort.Parse(configuration.GetOptionalSetting("ConsumerDispatchConcurrency", "1")),
            PublisherConfirms = bool.Parse(configuration.GetOptionalSetting("PublisherConfirms", "false"))
        };
    }
}

public record RabbitMqConfig
{
    public required string Hostname { get; init; }
    public required int Port { get; init; }
    public required string Username { get; init; }
    public required string Password { get; init; }
    public required bool DurableMode { get; init; }
    
    /// <summary>
    /// Number of messages the broker sends to the consumer before waiting for acks.
    /// Higher values improve throughput; lower values give fairer dispatch across consumers.
    /// </summary>
    public ushort PrefetchCount { get; init; } = 100;
    
    /// <summary>
    /// Number of concurrent message handlers dispatched by the client.
    /// </summary>
    public ushort ConsumerDispatchConcurrency { get; init; } = 1;
    
    /// <summary>
    /// When true, enables publisher confirms on the channel.
    /// </summary>
    public bool PublisherConfirms { get; init; } = false;
}

/// <summary>
/// Auto-generated resource naming conventions for RabbitMQ.
/// </summary>
public static class RabbitMqNaming
{
    private const string Base = "benchmark";
    
    /// <summary>Queue name for a specific consumer group (used in PointToPoint and PubSub).</summary>
    public static string GroupQueue(string groupName) => $"{Base}_{groupName}";
    
    /// <summary>Topic exchange name (used in PointToPoint for routed delivery).</summary>
    public static string TopicExchange() => $"{Base}_topic";
    
    /// <summary>Fanout exchange name (used in PubSub).</summary>
    public static string FanoutExchange() => $"{Base}_fanout";
    
    /// <summary>Stream queue name (used in Streaming).</summary>
    public static string StreamQueue() => $"{Base}_stream";
}
