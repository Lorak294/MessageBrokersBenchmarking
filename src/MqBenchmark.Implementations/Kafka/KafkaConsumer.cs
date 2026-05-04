using Confluent.Kafka;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;

namespace MqBenchmark.Implementations.Kafka;

public class KafkaConsumer : IMqConsumer
{
    private IConsumer<Null, byte[]>? _consumer;
    private KafkaConfig? _kafkaConfig;
    private CancellationTokenSource? _consumptionCts;
    private Task? _consumptionTask;
   
    private bool _disposed;

    public void Dispose()
    {
        if (_disposed) return;

        _consumptionCts?.Cancel();
        
        try
        {
            // Wait for the consumption loop to finish gracefully
            _consumptionTask?.Wait(TimeSpan.FromSeconds(5));
        }
        catch (Exception)
        {
            // Ignore cancellation or timeout exceptions during disposal
        }

        _consumer?.Close(); // Commit offsets and leave group cleanly
        _consumer?.Dispose();
        _consumptionCts?.Dispose();
        _disposed = true;
    }

    public Task InitializeAsync(MqConfig configuration)
    {
        _kafkaConfig = configuration.ToKafkaConfig();

        // For PubSub/Streaming, each consumer group gets a unique GroupId
        // so each group independently receives all messages.
        // For PointToPoint, all consumers share the same GroupId (competing consumers).
        var groupId = _kafkaConfig.GroupId;
        if (configuration.CommunicationMode is CommunicationMode.PubSub or CommunicationMode.Streaming)
        {
            groupId = $"{groupId}_group_{configuration.ConsumerGroupIndex}";
        }

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _kafkaConfig.BootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = _kafkaConfig.AutoOffsetReset,
            EnableAutoCommit = _kafkaConfig.EnableAutoCommit
        };

        _consumer = new ConsumerBuilder<Null, byte[]>(consumerConfig).Build();
        
        // Subscribe and wait for partition assignment (handles race with producer creating the topic)
        _consumer.Subscribe(_kafkaConfig.TopicName);
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            try { _consumer.Consume(TimeSpan.FromMilliseconds(500)); }
            catch (ConsumeException) { /* Topic may not exist yet */ }
            
            if (_consumer.Assignment.Count > 0) break;
        }
        
        return Task.CompletedTask;
    }

    public Task SubscribeAsync(Func<Message, Task> messageReceivedHandler)
    {
        if (_consumer == null || _kafkaConfig == null)
        {
            throw new InvalidOperationException("Consumer is not initialized. Call InitializeAsync first.");
        }

        // Consumer is already subscribed and has partition assignments from InitializeAsync.
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
                        var messageContent = consumeResult.Message.Value;
                        var message = Message.FromBytes(messageContent);
                        await messageReceivedHandler(message);
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
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