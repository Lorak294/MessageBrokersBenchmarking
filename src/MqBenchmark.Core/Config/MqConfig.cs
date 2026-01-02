namespace MqBenchmark.Core.Config;

public record MqConfig
{
    public required string Implementation { get; set; }
    public required string ConnectionString { get; set; }
}