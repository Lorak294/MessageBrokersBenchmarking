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
    private string _publishRoutingKey = string.Empty;

    public void Dispose()
    {
        _channel?.Dispose();
        _connection?.Dispose();
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
                // Publish to default exchange with queue name as routing key
                await _channel.QueueDeclareAsync(
                    queue: _rabbitConfig.QueueName,
                    durable: _rabbitConfig.DurableMode,
                    exclusive: false,
                    autoDelete: _rabbitConfig.QueueAutoDelete,
                    arguments: null);
                _publishExchange = string.Empty;
                _publishRoutingKey = _rabbitConfig.QueueName;
                break;

            case CommunicationMode.PubSub:
                // Declare fanout exchange; consumers will bind their own queues
                var exchangeName = string.IsNullOrEmpty(_rabbitConfig.ExchangeName)
                    ? $"{_rabbitConfig.QueueName}_fanout"
                    : _rabbitConfig.ExchangeName;
                await _channel.ExchangeDeclareAsync(
                    exchange: exchangeName,
                    type: ExchangeType.Fanout,
                    durable: _rabbitConfig.DurableMode,
                    autoDelete: false);
                _publishExchange = exchangeName;
                _publishRoutingKey = string.Empty; // Fanout ignores routing key
                break;

            case CommunicationMode.Streaming:
                // Declare a stream queue (x-queue-type: stream)
                var streamArgs = new Dictionary<string, object?>
                {
                    ["x-queue-type"] = "stream"
                };
                await _channel.QueueDeclareAsync(
                    queue: _rabbitConfig.QueueName,
                    durable: true, // Streams are always durable
                    exclusive: false,
                    autoDelete: false, // Streams cannot auto-delete
                    arguments: streamArgs);
                _publishExchange = string.Empty;
                _publishRoutingKey = _rabbitConfig.QueueName;
                break;
        }
    }

    public async Task SendAsync(Message message)
    {
        if (_channel is null)
        {
            throw new InvalidOperationException("Producer is not initialized.");
        }

        var properties = new BasicProperties
        {
            DeliveryMode = _rabbitConfig!.DurableMode || _communicationMode == CommunicationMode.Streaming
                ? DeliveryModes.Persistent
                : DeliveryModes.Transient
        };
        
        await _channel.BasicPublishAsync(
            exchange: _publishExchange,
            routingKey: _publishRoutingKey,
            mandatory: _communicationMode == CommunicationMode.PointToPoint,
            basicProperties: properties,
            body: message.Payload);
    }
}
