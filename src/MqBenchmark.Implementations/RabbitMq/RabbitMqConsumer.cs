using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MqBenchmark.Implementations.RabbitMq;

public class RabbitMqConsumer : IMqConsumer
{
    private IConnection? _connection;
    private IChannel? _channel;
    private string? _consumeQueueName;
    private string? _consumerTag;
    private CommunicationMode _communicationMode;

    public void Dispose()
    {
        if (_channel is not null && _consumerTag is not null)
        {
            try { _channel.BasicCancelAsync(_consumerTag).GetAwaiter().GetResult(); } catch { }
        }
        _channel?.Dispose();
        _connection?.Dispose();
    }

    public async Task InitializeAsync(MqConfig configuration)
    {
        var rabbitConfig = configuration.ToRabbitMqConfig();
        _communicationMode = configuration.CommunicationMode;
        var groupName = configuration.ConsumerGroupName;
        
        var factory = new ConnectionFactory
        {
            HostName = rabbitConfig.Hostname,
            UserName = rabbitConfig.Username,
            Password = rabbitConfig.Password,
            Port = rabbitConfig.Port
        };
        _connection = await factory.CreateConnectionAsync();
        _channel = await _connection.CreateChannelAsync();

        switch (_communicationMode)
        {
            case CommunicationMode.PointToPoint:
            case CommunicationMode.PubSub:
                // Both modes: consume from per-group queue (created by janitor)
                _consumeQueueName = RabbitMqNaming.GroupQueue(groupName!);
                
                await _channel.QueueDeclareAsync(
                    queue: _consumeQueueName,
                    durable: rabbitConfig.DurableMode,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);
                
                await _channel.BasicQosAsync(
                    prefetchSize: 0,
                    prefetchCount: rabbitConfig.PrefetchCount,
                    global: false);
                break;

            case CommunicationMode.Streaming:
                // All consumers read from the same stream queue
                _consumeQueueName = RabbitMqNaming.StreamQueue();
                
                var streamArgs = new Dictionary<string, object?>
                {
                    ["x-queue-type"] = "stream"
                };
                await _channel.QueueDeclareAsync(
                    queue: _consumeQueueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: streamArgs);
                
                await _channel.BasicQosAsync(
                    prefetchSize: 0,
                    prefetchCount: rabbitConfig.PrefetchCount,
                    global: false);
                break;
        }
    }

    public async Task SubscribeAsync(Func<Message, Task> messageReceivedHandler)
    {
        if (_channel is null || _consumeQueueName is null)
            throw new InvalidOperationException("Consumer is not initialized.");

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += async (sender, eventArgs) =>
        {
            var message = Message.FromBytes(eventArgs.Body.ToArray());
            try
            {
                await messageReceivedHandler(message);
                await ((AsyncEventingBasicConsumer)sender).Channel.BasicAckAsync(eventArgs.DeliveryTag, false);
            }
            catch
            {
                await ((AsyncEventingBasicConsumer)sender).Channel.BasicNackAsync(eventArgs.DeliveryTag, false, requeue: true);
                Console.WriteLine($"Message {message.Id} processing failed.");
            }
        };

        // For streaming, start reading from the beginning
        var consumeArgs = new Dictionary<string, object?>();
        if (_communicationMode == CommunicationMode.Streaming)
        {
            consumeArgs["x-stream-offset"] = "first";
        }

        _consumerTag = await _channel.BasicConsumeAsync(
            queue: _consumeQueueName,
            autoAck: false,
            consumerTag: string.Empty,
            noLocal: false,
            exclusive: false,
            arguments: consumeArgs,
            consumer: consumer);
    }
}
