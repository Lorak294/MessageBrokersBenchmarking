using Microsoft.Extensions.Configuration;
using MqBenchmark.Core.Config;

namespace MqBenchmark.Core.MqImplementation;

public interface IMqProducer : IDisposable
{
    Task InitializeAsync(MqConfig configuration);
    Task SendAsync(Message message);
}