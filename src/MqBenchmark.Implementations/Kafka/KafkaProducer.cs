using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;

namespace MqBenchmark.Implementations.Kafka;

public class KafkaProducer : IMqProducer
{
    private IProducer<Null, byte[]>? _producer;
    KafkaConfig? _kafkaConfig;
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
            Acks = Acks.All
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

        try
        {
            var deliveryResult = await _producer.ProduceAsync(_kafkaConfig.TopicName, new Message<Null, byte[]>
            {
                Value = message.Payload
            });
            
            // TODO: Remove for benchmarking - this adds latency and is not needed for correctness
            // Console.WriteLine($"Message {message.Id} delivered to {deliveryResult.Topic}, Offset: {deliveryResult.Offset}, Partition: {deliveryResult.Partition}");
        }
        catch (ProduceException<Null, byte[]> e)
        {
            Console.WriteLine($"Failed to produce message to Kafka topic {_kafkaConfig.TopicName}: {e.Error.Reason}");
        }
    }
}