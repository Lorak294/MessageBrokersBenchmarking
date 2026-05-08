using Confluent.Kafka;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;

namespace MqBenchmark.Implementations.Kafka;

public class KafkaProducer : IMqProducer
{
    private IProducer<Null, byte[]>? _producer;
    private KafkaConfig? _kafkaConfig;
    private CommunicationMode _communicationMode;
    
    public void Dispose()
    {
        _producer?.Flush();
        _producer?.Dispose();
    }

    public Task InitializeAsync(MqConfig configuration)
    {
        _kafkaConfig = configuration.ToKafkaConfig();
        _communicationMode = configuration.CommunicationMode;
        
        _producer = new ProducerBuilder<Null, byte[]>(new ProducerConfig
        {
            BootstrapServers = _kafkaConfig.BootstrapServers,
            Acks = _kafkaConfig.Acks,
            LingerMs = _kafkaConfig.LingerMs,
            BatchSize = _kafkaConfig.BatchSize,
            EnableIdempotence = _kafkaConfig.EnableIdempotence,
        }).Build();
        
        return Task.CompletedTask;
    }

    public async Task SendAsync(Message message, string? routingTarget = null)
    {
        if (_producer == null || _kafkaConfig == null)
            throw new InvalidOperationException("Producer is not initialized.");

        // Determine target topic based on mode
        var topicName = _communicationMode switch
        {
            CommunicationMode.PointToPoint => KafkaNaming.GroupTopic(routingTarget ?? throw new InvalidOperationException("PointToPoint requires a routing target.")),
            CommunicationMode.PubSub => KafkaNaming.SharedTopic(),
            CommunicationMode.Streaming => KafkaNaming.SharedTopic(),
            _ => throw new InvalidOperationException($"Unsupported mode: {_communicationMode}")
        };

        var kafkaMessage = new Message<Null, byte[]> { Value = message.Payload };

        if (_kafkaConfig.UseBufferedProducer)
        {
            try
            {
                _producer.Produce(topicName, kafkaMessage);
            }
            catch (ProduceException<Null, byte[]> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
            {
                _producer.Flush(TimeSpan.FromSeconds(5));
                _producer.Produce(topicName, kafkaMessage);
            }
        }
        else
        {
            await _producer.ProduceAsync(topicName, kafkaMessage);
        }
    }
}
