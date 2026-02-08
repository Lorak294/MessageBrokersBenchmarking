using MqBenchmark.Core.MqImplementation;

namespace MqBenchmark.Implementations.RabbitMq;

public class RabbitMqImplementation : IMqImplementation
{
    public static string GetKey() => "RabbitMQ";
    
    public IMqConsumer CreateConsumer()
    {
        return new RabbitMqConsumer();
    }

    public IMqProducer CreateProducer()
    {
        return new RabbitMqProducer();
    }
    
}