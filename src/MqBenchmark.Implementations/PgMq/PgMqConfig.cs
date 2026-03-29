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
            MessageReadMode = Enum.Parse<PgMqConfig.ReadModeEnum>(configuration.GetRequiredSetting("MessageReadMode"))
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