using MqBenchmark.Core.Config;

namespace MqBenchmark.Implementations.RabbitMq;

public static class MqConfigRabbitMqExtensions
{
    public static RabbitMqConfig ToRabbitMqConfig(this MqConfig configuration)
    {
        string GetRequiredSetting(string key)
        {
            if (configuration.AdditionalSettings.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value))
            {
                return value;
            }
            throw new ArgumentException($"Configuration setting '{key}' is required in AdditionalSettings.");
        }
        
        string GetOptionalSetting(string key, string defaultValue)
        {
            if (configuration.AdditionalSettings.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value))
            {
                return value;
            }
            return defaultValue;
        }
        
        return new RabbitMqConfig
        {
            Hostname = GetRequiredSetting("Hostname"),
            Port = int.Parse(GetRequiredSetting("Port")),
            Username = GetRequiredSetting("Username"),
            Password = GetRequiredSetting("Password"),
            QueueName = GetRequiredSetting("VirtualHost"),
            DurableMode = bool.Parse(GetOptionalSetting("DurableMode", "false")),
            QueueAutoDelete = bool.Parse(GetOptionalSetting("QueueAutodelete", "false"))
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
    public required bool DurableMode { get; init; } = false;
    public required bool QueueAutoDelete { get; init; } = false;
}