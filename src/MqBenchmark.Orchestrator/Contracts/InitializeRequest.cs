using MqBenchmark.Core.Config;

namespace MqBenchmark.Orchestrator.Contracts;

public record InitializeRequest
{
    public int ConsumersCount { get; set; }
    public int ProducersCount { get; set; }
    public int MessageCount { get; set; }
    public int MessageSizeInBytes { get; set; }
    public int? SendFrequencyMps { get; set; }
    public required MqConfig MqConfig { get; set; }
}