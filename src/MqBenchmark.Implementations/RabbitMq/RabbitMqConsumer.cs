using Microsoft.Extensions.Logging;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MqBenchmark.Implementations.RabbitMq;

public class RabbitMqConsumer : IMqConsumer
{
    private IConnection? _connection;
    private IChannel? _channel;
    private string? _queueName;
    private bool _durable;
    private ILogger<RabbitMqConsumer> _logger;
    
    public void Dispose()
    {
        _channel?.Dispose();
        _connection?.Dispose();
    }

    public async Task InitializeAsync(MqConfig configuration)
    {
        var rabbitConfig = configuration.ToRabbitMqConfig();
        var factory = new ConnectionFactory
        {
            HostName = rabbitConfig.Hostname,
            UserName = rabbitConfig.Username,
            Password = rabbitConfig.Password,
            Port = rabbitConfig.Port,
            //ConsumerDispatchConcurrency = X
        };
        _connection = await factory.CreateConnectionAsync();
        _channel = await _connection.CreateChannelAsync();
        _queueName = rabbitConfig.QueueName;
        _durable = rabbitConfig.DurableMode;
        
        await _channel.QueueDeclareAsync(
            queue: _queueName,
            durable: _durable,
            exclusive: false,
            autoDelete: false, 
            arguments: null
        );
        
        // TODO: Consider setting prefetch count based on expected load and processing time.
        // Define QoS (Quality of Service) to process one message at a time per consumer if needed,
        // or higher prefetch for throughput. Standard default often 0 (unlimited), 
        // setting prefetch to 1 ensures fair dispatch if processing is heavy.
        // await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);
    }

    public async Task SubscribeAsync(Func<Message, Task> messageReceivedHandler)
    {
        if (_channel is null || _queueName is null)
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
                
                await ((AsyncEventingBasicConsumer)sender).Channel.BasicAckAsync(eventArgs.DeliveryTag, false);
                // await _channel.BasicAckAsync(eventArgs.DeliveryTag, false);
            }
            catch
            {
                // On failure, we nack the message and requeue it for later processing.
                _logger.LogWarning("Message {MessageId} processing failed - requeuing message.", message.Id);
                await ((AsyncEventingBasicConsumer)sender).Channel.BasicAckAsync(eventArgs.DeliveryTag, false);
                // await _channel.BasicNackAsync(eventArgs.DeliveryTag, false, requeue: true);
            }
        };
        
        await _channel.BasicConsumeAsync(
            queue: _queueName,
            autoAck: false, // Manual ack after processing
            consumer: consumer);
    }
}