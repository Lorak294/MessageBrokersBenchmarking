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
            QueueName = configuration.GetRequiredSetting("QueueName"),
            ExchangeName = configuration.GetOptionalSetting("ExchangeName", ""),
            DurableMode = bool.Parse(configuration.GetOptionalSetting("DurableMode", "false")),
            QueueAutoDelete = bool.Parse(configuration.GetOptionalSetting("QueueAutodelete", "false")),
            PrefetchCount = ushort.Parse(configuration.GetOptionalSetting("PrefetchCount", "100")),
            ConsumerDispatchConcurrency = ushort.Parse(configuration.GetOptionalSetting("ConsumerDispatchConcurrency", "1"))
        };
    }
}

public record RabbitMqConfig
{
    public required string Hostname { get; init; }
    public required int Port { get; init; }
    public required string Username { get; init; }
    public required string Password { get; init; }
    public required string QueueName { get; init; }
    /// <summary>
    /// Exchange name used for PubSub mode (fanout exchange).
    /// Empty string means default exchange (PointToPoint).
    /// </summary>
    public string ExchangeName { get; init; } = "";
    public required bool DurableMode { get; init; } = false;
    public required bool QueueAutoDelete { get; init; } = false;
    /// <summary>
    /// Number of messages the broker sends to the consumer before waiting for acks.
    /// Higher values improve throughput; lower values give fairer dispatch across consumers.
    /// Default: 100.
    /// </summary>
    public ushort PrefetchCount { get; init; } = 100;
    /// <summary>
    /// Number of concurrent message handlers dispatched by the client.
    /// Default: 1 (sequential processing).
    /// </summary>
    public ushort ConsumerDispatchConcurrency { get; init; } = 1;
}
