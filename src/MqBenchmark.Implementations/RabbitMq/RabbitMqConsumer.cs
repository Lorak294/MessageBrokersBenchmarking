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
    private bool _autoAck;

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
        
        var factory = new ConnectionFactory
        {
            HostName = rabbitConfig.Hostname,
            UserName = rabbitConfig.Username,
            Password = rabbitConfig.Password,
            Port = rabbitConfig.Port,
            ConsumerDispatchConcurrency = rabbitConfig.ConsumerDispatchConcurrency
        };
        _connection = await factory.CreateConnectionAsync();
        _channel = await _connection.CreateChannelAsync();

        switch (_communicationMode)
        {
            case CommunicationMode.PointToPoint:
                _consumeQueueName = rabbitConfig.QueueName;
                _autoAck = false;
                
                await _channel.QueueDeclareAsync(
                    queue: _consumeQueueName,
                    durable: rabbitConfig.DurableMode,
                    exclusive: false,
                    autoDelete: rabbitConfig.QueueAutoDelete,
                    arguments: null);
                
                // Purge leftover messages from previous test run
                await _channel.QueuePurgeAsync(_consumeQueueName);
                
                await _channel.BasicQosAsync(
                    prefetchSize: 0,
                    prefetchCount: rabbitConfig.PrefetchCount,
                    global: false);
                break;

            case CommunicationMode.PubSub:
                // Each consumer group gets its own queue bound to the fanout exchange
                var exchangeName = string.IsNullOrEmpty(rabbitConfig.ExchangeName)
                    ? $"{rabbitConfig.QueueName}_fanout"
                    : rabbitConfig.ExchangeName;
                _consumeQueueName = $"{rabbitConfig.QueueName}_group_{configuration.ConsumerGroupIndex}";
                _autoAck = false;
                
                // Declare the fanout exchange (idempotent)
                await _channel.ExchangeDeclareAsync(
                    exchange: exchangeName,
                    type: ExchangeType.Fanout,
                    durable: rabbitConfig.DurableMode,
                    autoDelete: false);
                
                // Declare per-group queue
                await _channel.QueueDeclareAsync(
                    queue: _consumeQueueName,
                    durable: rabbitConfig.DurableMode,
                    exclusive: false,
                    autoDelete: rabbitConfig.QueueAutoDelete,
                    arguments: null);
                
                // Bind to fanout exchange
                await _channel.QueueBindAsync(
                    queue: _consumeQueueName,
                    exchange: exchangeName,
                    routingKey: string.Empty);
                
                // Purge leftover messages
                await _channel.QueuePurgeAsync(_consumeQueueName);
                
                await _channel.BasicQosAsync(
                    prefetchSize: 0,
                    prefetchCount: rabbitConfig.PrefetchCount,
                    global: false);
                break;

            case CommunicationMode.Streaming:
                // Stream queues: all consumers read from the same stream queue with offsets
                _consumeQueueName = rabbitConfig.QueueName;
                _autoAck = false; // Streams require manual ack (auto-ack not supported by stream queues)
                
                // Declare stream queue (idempotent)
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
                
                // QoS is required for stream consumers
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
        {
            throw new InvalidOperationException("Consumer is not initialized.");
        }

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += async (sender, eventArgs) =>
        {
            var message = Message.FromBytes(eventArgs.Body.ToArray());
            try
            {
                await messageReceivedHandler(message);

                if (!_autoAck)
                {
                    await ((AsyncEventingBasicConsumer)sender).Channel.BasicAckAsync(eventArgs.DeliveryTag, false);
                }
            }
            catch
            {
                if (!_autoAck)
                {
                    await ((AsyncEventingBasicConsumer)sender).Channel.BasicNackAsync(eventArgs.DeliveryTag, false, requeue: true);
                }
                Console.WriteLine($"Message {message.Id} processing failed.");
            }
        };

        // For streaming, start reading from the beginning of the stream
        var consumeArgs = new Dictionary<string, object?>();
        if (_communicationMode == CommunicationMode.Streaming)
        {
            consumeArgs["x-stream-offset"] = "first";
        }

        _consumerTag = await _channel.BasicConsumeAsync(
            queue: _consumeQueueName,
            autoAck: _autoAck,
            consumerTag: string.Empty,
            noLocal: false,
            exclusive: false,
            arguments: consumeArgs,
            consumer: consumer);
    }
}
