using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using RabbitMQ.Client;

namespace MqBenchmark.Implementations.RabbitMq;

public class RabbitMqProducer : IMqProducer
{
    private IConnection? _connection;
    private IChannel? _channel;
    private RabbitMqConfig? _rabbitConfig;
    private CommunicationMode _communicationMode;
    private string _publishExchange = string.Empty;

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null) await _channel.DisposeAsync();
        if (_connection is not null) await _connection.DisposeAsync();
    }

    public async Task InitializeAsync(MqConfig configuration)
    {
        _rabbitConfig = configuration.ToRabbitMqConfig();
        _communicationMode = configuration.CommunicationMode;
        
        var factory = new ConnectionFactory
        {
            HostName = _rabbitConfig.Hostname,
            UserName = _rabbitConfig.Username,
            Password = _rabbitConfig.Password,
            Port = _rabbitConfig.Port
        };

        _connection = await factory.CreateConnectionAsync();

        var channelOptions = new CreateChannelOptions(
            publisherConfirmationsEnabled: _rabbitConfig.PublisherConfirms,
            publisherConfirmationTrackingEnabled: _rabbitConfig.PublisherConfirms);
        _channel = await _connection.CreateChannelAsync(channelOptions);

        switch (_communicationMode)
        {
            case CommunicationMode.PointToPoint:
                // Topic exchange — routing keys target specific group queues
                _publishExchange = RabbitMqNaming.TopicExchange();
                await _channel.ExchangeDeclareAsync(
                    exchange: _publishExchange,
                    type: ExchangeType.Topic,
                    durable: _rabbitConfig.DurableMode,
                    autoDelete: false);
                break;

            case CommunicationMode.PubSub:
                // Fanout exchange — all messages go to all bound queues
                _publishExchange = RabbitMqNaming.FanoutExchange();
                await _channel.ExchangeDeclareAsync(
                    exchange: _publishExchange,
                    type: ExchangeType.Fanout,
                    durable: _rabbitConfig.DurableMode,
                    autoDelete: false);
                break;

            case CommunicationMode.Streaming:
                // Publish directly to stream queue via default exchange
                var streamQueue = RabbitMqNaming.StreamQueue();
                var streamArgs = new Dictionary<string, object?>
                {
                    ["x-queue-type"] = "stream"
                };
                await _channel.QueueDeclareAsync(
                    queue: streamQueue,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: streamArgs);
                _publishExchange = string.Empty; // default exchange
                break;
        }
    }

    public async Task SendAsync(Message message, string? routingTarget = null)
    {
        if (_channel is null || _rabbitConfig is null)
            throw new InvalidOperationException("Producer is not initialized.");

        var properties = new BasicProperties
        {
            DeliveryMode = _rabbitConfig.DurableMode || _communicationMode == CommunicationMode.Streaming
                ? DeliveryModes.Persistent
                : DeliveryModes.Transient
        };

        var routingKey = _communicationMode switch
        {
            CommunicationMode.PointToPoint => routingTarget ?? throw new InvalidOperationException("PointToPoint requires a routing target."),
            CommunicationMode.PubSub => string.Empty, // Fanout ignores routing key
            CommunicationMode.Streaming => RabbitMqNaming.StreamQueue(), // Default exchange uses queue name as routing key
            _ => string.Empty
        };

        await _channel.BasicPublishAsync(
            exchange: _publishExchange,
            routingKey: routingKey,
            mandatory: false,
            basicProperties: properties,
            body: message.Payload);
    }
}
