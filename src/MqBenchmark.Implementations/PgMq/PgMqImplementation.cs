using MqBenchmark.Core.MqImplementation;

namespace MqBenchmark.Implementations.PgMq;

public class PgMqImplementation : IMqImplementation
{
    public static string GetKey() => "PgMq";
    public IMqConsumer CreateConsumer()
    {
        return new PgMqConsumer();
    }

    public IMqProducer CreateProducer()
    {
        return new PgMqProducer();
    }

    public IMqJanitor CreateJanitor()
    {
        return new PgMqJanitor();
    }
}