using Confluent.Kafka;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;

namespace MqBenchmark.Implementations.Kafka;

public class KafkaProducer : IMqProducer
{
    private IProducer<Null, byte[]>? _producer;
    private KafkaConfig? _kafkaConfig;
    
    public void Dispose()
    {
        _producer?.Flush();
        _producer?.Dispose();
    }

    public Task InitializeAsync(MqConfig configuration)
    {
        _kafkaConfig = configuration.ToKafkaConfig();
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _kafkaConfig.BootstrapServers,
            AllowAutoCreateTopics = true,
            Acks = _kafkaConfig.Acks,
            LingerMs = _kafkaConfig.LingerMs,
            BatchSize = _kafkaConfig.BatchSize,
            EnableIdempotence = _kafkaConfig.EnableIdempotence
        };
        
        _producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build();
        return Task.CompletedTask;
    }

    public async Task SendAsync(Message message)
    {
        if (_producer == null || _kafkaConfig == null)
        {
            throw new InvalidOperationException("Producer is not initialized. Call InitializeAsync first.");
        }

        var kafkaMessage = new Message<Null, byte[]>
        {
            Value = message.Payload
        };

        if (_kafkaConfig.UseBufferedProducer)
        {
            _producer.Produce(_kafkaConfig.TopicName, kafkaMessage);
        }
        else
        {
            try
            {
                await _producer.ProduceAsync(_kafkaConfig.TopicName, kafkaMessage);
            }
            catch (ProduceException<Null, byte[]> e)
            {
                Console.WriteLine($"Failed to produce message to Kafka topic {_kafkaConfig.TopicName}: {e.Error.Reason}");
            }
        }
    }
}
