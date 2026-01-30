using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using Npgmq;

namespace MqBenchmark.Implementations.PgMq;

public class PgMqConsumer : IMqConsumer
{
    // private NpgmqClient _pgmqClient;
    // private PgMqConfig _config;
    private readonly TimeSpan _readTimeout = TimeSpan.FromSeconds(10);
    
    public void Dispose()
    {

    }

    public Task InitializeAsync(MqConfig configuration)
    {
        throw new NotImplementedException();
    }

    public Task SubscribeAsync(Func<Message, Task> messageReceivedHandler)
    {
        throw new NotImplementedException();
    }
}