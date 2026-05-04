using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MqBenchmark.Core.MqImplementation;

public interface IMqImplementation
{
    IMqConsumer CreateConsumer();
    IMqProducer CreateProducer();
    IMqJanitor CreateJanitor();

    static string GetKey() => throw new NotImplementedException("The GetKey method is not implemented.");
}