using MqBenchmark.Core.MqImplementation;

namespace MqBenchmark.Implementations.RabbitMq;

public class RabbitMqImplementation : IMqImplementation
{
    public IMqConsumer CreateConsumer()
    {
        return new RabbitMqConsumer();
    }

    public IMqProducer CreateProducer()
    {
        return new RabbitMqProducer();
    }
    
    public static string GetKey() => "RabbitMQ";
}