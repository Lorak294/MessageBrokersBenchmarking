using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;

namespace MqBenchmark.Implementations.Dummy;

public class DummyProducer : IMqProducer
{
    private readonly Guid _id = Guid.NewGuid();
    
    public async Task InitializeAsync(TestConfig configuration)
    {
        Console.WriteLine($"Producer {_id} is connecting...");
        await Task.Delay(100);
        Console.WriteLine($"Producer {_id} is connected!");
    }

    public async Task SendAsync(Message message)
    {
        Console.WriteLine($"Producer {_id} sending message: ${message.Payload}...");
        await Task.Delay(100);
    }

    public void Dispose()
    {
        Console.WriteLine($"Producer {_id} is destroyed.");
    }
}