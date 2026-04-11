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
            VisibilityTimeout = int.Parse(configuration.GetRequiredSetting("VisibilityTimeout")),
            QueueMode = Enum.Parse<PgMqConfig.QueueModeEnum>(configuration.GetRequiredSetting("QueueMode")),
            MessageReadMode = Enum.Parse<PgMqConfig.ReadModeEnum>(configuration.GetRequiredSetting("MessageReadMode")),
            PollIntervalMs = int.Parse(configuration.GetOptionalSetting("PollIntervalMs", "100")),
            UsePop = bool.Parse(configuration.GetOptionalSetting("UsePop", "true"))
        };
    }
}

public record PgMqConfig
{
    public required string ConnectionString {get; init;}
    public required string QueueName {get; init;}
    public required int VisibilityTimeout {get; init;}
    
    public required QueueModeEnum QueueMode {get; init;}
    public required ReadModeEnum MessageReadMode {get; init;}
    
    /// <summary>
    /// Polling interval in milliseconds when no messages are available. Default: 100ms.
    /// </summary>
    public int PollIntervalMs { get; init; } = 50;
    
    /// <summary>
    /// When true and MessageReadMode is Delete, use pgmq.pop() for atomic read+delete in one round-trip.
    /// When false or when MessageReadMode is Archive, use read()+delete()/archive().
    /// Default: true.
    /// </summary>
    public bool UsePop { get; init; } = true;
    
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
}