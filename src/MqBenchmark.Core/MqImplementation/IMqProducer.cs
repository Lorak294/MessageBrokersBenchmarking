using MqBenchmark.Core.Config;

namespace MqBenchmark.Core.MqImplementation;

/// <summary>
/// Responsible sending messages to the broker.
/// </summary>
public interface IMqProducer : IAsyncDisposable
{
    Task InitializeAsync(MqConfig configuration);
    Task SendAsync(Message message, string? routingTarget = null);
}