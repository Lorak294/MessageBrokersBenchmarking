using MqBenchmark.Core.Config;

namespace MqBenchmark.Implementations.Kafka;

public static class MqConfigKafkaExtensions
{
    public static KafkaConfig ToKafkaConfig(this MqConfig configuration)
    {
        string GetRequiredSetting(string key)
        {
            if (configuration.AdditionalSettings.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value))
            {
                return value;
            }
            throw new ArgumentException($"Configuration setting '{key}' is required in AdditionalSettings.");
        }
        
        return new KafkaConfig
        {
            BootstrapServers = GetRequiredSetting("BootstrapServers"),
            TopicName = GetRequiredSetting("TopicName"),
            GroupId = GetRequiredSetting("GroupId")
        };
    }
}

public class KafkaConfig
{
    public required string BootstrapServers { get; init; }
    public required string TopicName { get; init; }
    public required string GroupId { get; init; }
}