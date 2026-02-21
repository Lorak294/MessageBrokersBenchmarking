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

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _kafkaConfig.BootstrapServers,
            GroupId = _kafkaConfig.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true 
        };

        _consumer = new ConsumerBuilder<Null, byte[]>(consumerConfig).Build();
        return Task.CompletedTask;
    }

    public Task SubscribeAsync(Func<Message, Task> messageReceivedHandler)
    {
        if (_consumer == null || _kafkaConfig == null)
        {
            throw new InvalidOperationException("Consumer is not initialized. Call InitializeAsync first.");
        }

        _consumer.Subscribe(_kafkaConfig.TopicName);
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
                        // Assuming the Payload contains the ID embedded as per previous conversation context,
                        // or just passing the raw bytes if utilizing the previously discussed Message logic.
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