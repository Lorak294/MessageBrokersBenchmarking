using MqBenchmark.Core.MqImplementation;

namespace MqBenchmark.Implementations.Dummy;

public class DummyImplementation : IMqImplementation
{
    public IMqConsumer CreateConsumer() => new  DummyConsumer();
    public IMqProducer CreateProducer() => new  DummyProducer();
    public static string GetKey() => "Dummy";
}