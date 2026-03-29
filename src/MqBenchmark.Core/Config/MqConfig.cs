namespace MqBenchmark.Core.Config;

public record MqConfig
{
    public required string Implementation { get; set; }
    public Dictionary<string,string> AdditionalSettings { get; set; } = new();

    public string GetRequiredSetting(string key)
    {
        if (AdditionalSettings.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value))
        {
            return value;
        }
        throw new ArgumentException($"Configuration setting '{key}' is required in AdditionalSettings.");
    }

    public string GetOptionalSetting(string key, string defaultValue)
    {
        if (AdditionalSettings.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value))
        {
            return value;
        }
        return defaultValue;
    }
}