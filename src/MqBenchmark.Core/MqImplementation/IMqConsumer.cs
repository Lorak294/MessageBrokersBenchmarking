using Microsoft.Extensions.Configuration;
using MqBenchmark.Core.Config;

namespace MqBenchmark.Core.MqImplementation;

public interface IMqConsumer : IDisposable
{
    Task InitializeAsync(MqConfig configuration);

    Task SubscribeAsync(Func<Message, Task> messageReceivedHandler);
}