using System.Text;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using RabbitMQ.Client;

namespace MqBenchmark.Implementations.RabbitMq;

public class RabbitMqProducer : IMqProducer
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
            Port = rabbitConfig.Port
        };

        _connection = await factory.CreateConnectionAsync();
        _channel = await _connection.CreateChannelAsync();
        _queueName = rabbitConfig.QueueName;
        _durable = rabbitConfig.DurableMode;
        
        // Ensure queue exists
        await _channel.QueueDeclareAsync(
            queue: _queueName,
            durable: _durable,
            exclusive: false,
            autoDelete: false,
            arguments: null
        );
    }

    public async Task SendAsync(Message message)
    {
        if (_channel is null || _queueName is null)
        {
            throw new InvalidOperationException("Producer is not initialized.");
        }

        var body = message.Payload;
        var properties = new BasicProperties
        {
            DeliveryMode = _durable ? DeliveryModes.Persistent : DeliveryModes.Transient
        };
        await _channel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: _queueName,
            mandatory: true,
            basicProperties: properties,
            body: body);
    }
}