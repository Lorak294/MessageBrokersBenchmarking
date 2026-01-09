using MqBenchmark.Core.Config;

namespace MqBenchmark.Implementations.PgMq;

public static class MqConfigPgMqExtensions
{
    public static PgMqConfig ToPgMqConfig(this MqConfig configuration)
    {
        string GetRequiredSetting(string key)
        {
            if (configuration.AdditionalSettings.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value))
            {
                return value;
            }
            throw new ArgumentException($"Configuration setting '{key}' is required in AdditionalSettings.");
        }
        
        return new PgMqConfig
        {
            QueueName = GetRequiredSetting("QueueName"),
            ConnectionString =  GetRequiredSetting("ConnectionString"),
            VisibilityTimeout = int.Parse(GetRequiredSetting("VisibilityTimeout")),
            QueueMode = Enum.Parse<PgMqConfig.QueueModeEnum>(GetRequiredSetting("QueueMode")),
            MessageReadMode = Enum.Parse<PgMqConfig.ReadModeEnum>(GetRequiredSetting("MessageReadMode"))
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