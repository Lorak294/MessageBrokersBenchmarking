using Confluent.Kafka;
using Confluent.Kafka.Admin;
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

    public async Task InitializeAsync(MqConfig configuration)
    {
        _kafkaConfig = configuration.ToKafkaConfig();
        
        // Delete and recreate topic to ensure a clean slate for each test run
        using var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = _kafkaConfig.BootstrapServers })
            .Build();

        try
        {
            await adminClient.DeleteTopicsAsync(new[] { _kafkaConfig.TopicName });
            await Task.Delay(2000); // Wait for deletion to propagate
        }
        catch (DeleteTopicsException) { /* Topic didn't exist — fine */ }

        await adminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification
            {
                Name = _kafkaConfig.TopicName,
                NumPartitions = _kafkaConfig.NumPartitions,
                ReplicationFactor = 1
            }
        });
        
        _producer = new ProducerBuilder<Null, byte[]>(new ProducerConfig
        {
            BootstrapServers = _kafkaConfig.BootstrapServers,
            Acks = _kafkaConfig.Acks,
            LingerMs = _kafkaConfig.LingerMs,
            BatchSize = _kafkaConfig.BatchSize,
            EnableIdempotence = _kafkaConfig.EnableIdempotence
        }).Build();
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
