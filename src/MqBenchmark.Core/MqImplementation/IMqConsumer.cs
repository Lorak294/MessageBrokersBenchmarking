using MqBenchmark.Core.Config;

namespace MqBenchmark.Core.MqImplementation;

/// <summary>
/// Responsible for reading anc consuming messaged from the broker.
/// </summary>
public interface IMqConsumer : IAsyncDisposable
{
    Task InitializeAsync(MqConfig configuration);

    Task SubscribeAsync(Func<Message, Task> messageReceivedHandler);
}