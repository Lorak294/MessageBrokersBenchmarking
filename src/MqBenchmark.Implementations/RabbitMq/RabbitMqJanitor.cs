using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using RabbitMQ.Client;

namespace MqBenchmark.Implementations.RabbitMq;

public class RabbitMqJanitor : IMqJanitor
{
    private IConnection? _connection;
    private IChannel? _channel;

    public async Task PrepareInfrastructureAsync(JanitorConfig config)
    {
        var rabbitConfig = config.MqConfig.ToRabbitMqConfig();

        var factory = new ConnectionFactory
        {
            HostName = rabbitConfig.Hostname,
            UserName = rabbitConfig.Username,
            Password = rabbitConfig.Password,
            Port = rabbitConfig.Port
        };

        _connection = await factory.CreateConnectionAsync();
        _channel = await _connection.CreateChannelAsync();

        switch (config.CommunicationMode)
        {
            case CommunicationMode.PointToPoint:
                await PreparePointToPoint(config, rabbitConfig);
                break;
            case CommunicationMode.PubSub:
                await PreparePubSub(config, rabbitConfig);
                break;
            case CommunicationMode.Streaming:
                await PrepareStreaming(rabbitConfig);
                break;
        }
    }

    private async Task PreparePointToPoint(JanitorConfig config, RabbitMqConfig rabbitConfig)
    {
        // Topic exchange with per-group queues bound by routing key
        var exchangeName = RabbitMqNaming.TopicExchange();
        
        await _channel!.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: ExchangeType.Topic,
            durable: rabbitConfig.DurableMode,
            autoDelete: false);

        for (int i = 0; i < config.ConsumerGroups.Length; i++)
        {
            var queueName = RabbitMqNaming.GroupQueue($"group_{i}");
            await _channel.QueueDeclareAsync(
                queue: queueName,
                durable: rabbitConfig.DurableMode,
                exclusive: false,
                autoDelete: false,
                arguments: null);
            await _channel.QueueBindAsync(
                queue: queueName,
                exchange: exchangeName,
                routingKey: $"group_{i}");
            await _channel.QueuePurgeAsync(queueName);
        }
    }

    private async Task PreparePubSub(JanitorConfig config, RabbitMqConfig rabbitConfig)
    {
        // Fanout exchange with per-group queues
        var exchangeName = RabbitMqNaming.FanoutExchange();
        
        await _channel!.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: ExchangeType.Fanout,
            durable: rabbitConfig.DurableMode,
            autoDelete: false);

        for (int i = 0; i < config.ConsumerGroups.Length; i++)
        {
            var queueName = RabbitMqNaming.GroupQueue($"group_{i}");
            await _channel.QueueDeclareAsync(
                queue: queueName,
                durable: rabbitConfig.DurableMode,
                exclusive: false,
                autoDelete: false,
                arguments: null);
            await _channel.QueueBindAsync(
                queue: queueName,
                exchange: exchangeName,
                routingKey: string.Empty);
            await _channel.QueuePurgeAsync(queueName);
        }
    }

    private async Task PrepareStreaming(RabbitMqConfig rabbitConfig)
    {
        // Stream queues can't be purged — delete and recreate
        var queueName = RabbitMqNaming.StreamQueue();
        try { await _channel!.QueueDeleteAsync(queueName); }
        catch { /* Queue didn't exist */ }

        var streamArgs = new Dictionary<string, object?>
        {
            ["x-queue-type"] = "stream"
        };
        await _channel!.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: streamArgs);
    }

    public void Dispose()
    {
        _channel?.Dispose();
        _connection?.Dispose();
    }
}
