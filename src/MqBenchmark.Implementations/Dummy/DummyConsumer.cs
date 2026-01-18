using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;

namespace MqBenchmark.Implementations.Dummy;

public class DummyConsumer : IMqConsumer
{
    private readonly Guid _id = Guid.NewGuid();
    
    public async Task InitializeAsync(TestConfig configuration)
    {
        Console.WriteLine($"Consumer {_id} is connecting...");
        await Task.Delay(100);
        Console.WriteLine($"Consumer {_id} is connected!");
    }

    public async Task SubscribeAsync(Func<Message, Task> messageReceivedHandler)
    {
        Console.WriteLine($"Consumer {_id} is subscribing...");
        await Task.Delay(10000);
        Console.WriteLine($"Consumer {_id} is finished consuming.");
    }

    public void Dispose()
    {
        Console.WriteLine($"Consumer {_id} is destroyed!");
    }
}