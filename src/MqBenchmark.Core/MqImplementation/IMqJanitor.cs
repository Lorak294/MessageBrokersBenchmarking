using MqBenchmark.Core.Config;

namespace MqBenchmark.Core.MqImplementation;

/// <summary>
/// Configuration for the Janitor role, providing all information needed
/// to prepare broker infrastructure for a clean test run.
/// </summary>
public record JanitorConfig
{
    public required MqConfig MqConfig { get; init; }
    public required CommunicationMode CommunicationMode { get; init; }
    public required int[] ConsumerGroups { get; init; }
}

/// <summary>
/// Responsible for preparing broker infrastructure before a test run.
/// Creates/purges topics, queues, exchanges, and resets consumer state as needed.
/// </summary>
public interface IMqJanitor : IAsyncDisposable
{
    Task PrepareInfrastructureAsync(JanitorConfig config);
}
