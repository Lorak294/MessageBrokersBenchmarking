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
            ConsumerDispatchConcurrency = rabbitConfig.ConsumerDispatchConcurrency
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
        
        // Allow the broker to send multiple messages before waiting for acks.
        // This avoids a round-trip per message and prevents artificial queue buildup
        // that inflates measured latency in benchmarks.
        // await _channel.BasicQosAsync(
        //     prefetchSize: 0,
        //     prefetchCount: rabbitConfig.PrefetchCount,
        //     global: false);
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
                await ((AsyncEventingBasicConsumer)sender).Channel.BasicNackAsync(eventArgs.DeliveryTag, false, requeue: true);
                // await _channel.BasicNackAsync(eventArgs.DeliveryTag, false, requeue: true);
                Console.WriteLine($"Message {message.Id} processing failed - requeuing message.");
            }
        };
        
        await _channel.BasicConsumeAsync(
            queue: _queueName,
            autoAck: false, // Manual ack after processing
            consumer: consumer);
    }
}