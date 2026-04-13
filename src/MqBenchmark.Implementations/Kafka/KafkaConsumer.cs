using Confluent.Kafka;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;

namespace MqBenchmark.Implementations.Kafka;

public class KafkaConsumer : IMqConsumer
{
    private IConsumer<Null, byte[]>? _consumer;
    private CancellationTokenSource? _consumptionCts;
    private Task? _consumptionTask;
    private bool _disposed;

    public void Dispose()
    {
        if (_disposed) return;

        _consumptionCts?.Cancel();
        
        try
        {
            _consumptionTask?.Wait(TimeSpan.FromSeconds(5));
        }
        catch { }

        _consumer?.Close();
        _consumer?.Dispose();
        _consumptionCts?.Dispose();
        _disposed = true;
    }

    public Task InitializeAsync(MqConfig configuration)
    {
        var kafkaConfig = configuration.ToKafkaConfig();
        var groupName = configuration.ConsumerGroupName;

        // Determine topic to subscribe to and group ID based on mode
        string topicName;
        string groupId;

        switch (configuration.CommunicationMode)
        {
            case CommunicationMode.PointToPoint:
                // Each consumer group has its own topic; consumers in that group compete
                topicName = KafkaNaming.GroupTopic(groupName!);
                groupId = KafkaNaming.SharedGroupId(groupName!);
                break;

            case CommunicationMode.PubSub:
            case CommunicationMode.Streaming:
                // All groups read from shared topic; each group has unique GroupId
                // so each group independently receives all messages
                topicName = KafkaNaming.SharedTopic();
                groupId = KafkaNaming.GroupId(groupName!);
                break;

            default:
                throw new InvalidOperationException($"Unsupported mode: {configuration.CommunicationMode}");
        }

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = kafkaConfig.BootstrapServers,
            GroupId = groupId
        };

        _consumer = new ConsumerBuilder<Null, byte[]>(consumerConfig).Build();
        
        // Subscribe and wait for partition assignment
        _consumer.Subscribe(topicName);
        var deadline = DateTime.UtcNow.AddSeconds(KafkaConstants.PartitionAssignmentTimeoutSeconds);
        while (DateTime.UtcNow < deadline)
        {
            _consumer.Consume(TimeSpan.FromMilliseconds(KafkaConstants.ConsumePollTimeoutMs));
            if (_consumer.Assignment.Count > 0) break;
        }
        
        return Task.CompletedTask;
    }

    public Task SubscribeAsync(Func<Message, Task> messageReceivedHandler)
    {
        if (_consumer == null)
            throw new InvalidOperationException("Consumer is not initialized.");

        _consumptionCts = new CancellationTokenSource();

        _consumptionTask = Task.Run(async () =>
        {
            while (!_consumptionCts.Token.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(_consumptionCts.Token);
                    if (consumeResult?.Message != null)
                    {
                        var message = Message.FromBytes(consumeResult.Message.Value);
                        await messageReceivedHandler(message);
                    }
                }
                catch (OperationCanceledException) { break; }
                catch (ConsumeException ex)
                {
                    Console.WriteLine($"Error consuming message: {ex.Error.Reason}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unexpected error in consumption loop: {ex.Message}");
                }
            }
        }, _consumptionCts.Token);

        return Task.CompletedTask;
    }
}
