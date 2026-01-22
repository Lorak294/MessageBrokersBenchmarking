using MqBenchmark.Core.MqImplementation;

namespace MqBenchmark.Implementations.Kafka;

public class KafkaImplementation : IMqImplementation
{
    public IMqConsumer CreateConsumer()
    {
        return new KafkaConsumer();
    }

    public IMqProducer CreateProducer()
    {
        return new KafkaProducer();
    }
}