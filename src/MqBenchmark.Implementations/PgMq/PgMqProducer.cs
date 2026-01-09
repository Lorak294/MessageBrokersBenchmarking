using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;

namespace MqBenchmark.Implementations.PgMq;

public class PgMqProducer : IMqProducer
{
    public void Dispose()
    {
        throw new NotImplementedException();
    }

    public Task InitializeAsync(MqConfig configuration)
    {
        throw new NotImplementedException();
    }

    public Task SendAsync(Message message)
    {
        throw new NotImplementedException();
    }
}