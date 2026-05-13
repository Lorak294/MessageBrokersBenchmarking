using MqBenchmark.Core.Config;

namespace MqBenchmark.Core.MqImplementation;

/// <summary>
/// Responsible sending messages to the broker.
/// </summary>
public interface IMqProducer : IDisposable
{
    Task InitializeAsync(MqConfig configuration);
    Task SendAsync(Message message, string? routingTarget = null);
}