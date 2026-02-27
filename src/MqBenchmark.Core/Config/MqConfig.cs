namespace MqBenchmark.Core.Config;

public record MqConfig
{
    public required string Implementation { get; set; }
    public Dictionary<string,string> AdditionalSettings { get; set; } = new();
}