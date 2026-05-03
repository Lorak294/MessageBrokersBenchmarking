using MqBenchmark.Core.Config;

namespace MqBenchmark.Orchestrator.Contracts;

public record InitializeRequest
{
    public int ProducersCount { get; set; }
    /// <summary>
    /// Array where each element represents the number of consumers in that group.
    /// E.g. [2, 3] means group 0 has 2 consumers, group 1 has 3 consumers.
    /// </summary>
    public required int[] ConsumerGroups { get; set; }
    public CommunicationMode CommunicationMode { get; set; } = CommunicationMode.PointToPoint;
    public int MessageCount { get; set; }
    public int MessageSizeInBytes { get; set; }
    public int? SendFrequencyMps { get; set; }
    public int ConsumerIdleTimeoutSeconds { get; set; } = 15;
    public required MqConfig MqConfig { get; set; }

    public int TotalConsumersCount => ConsumerGroups.Sum();
}