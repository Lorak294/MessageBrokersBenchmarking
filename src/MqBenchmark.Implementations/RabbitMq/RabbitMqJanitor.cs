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
                await _channel.QueueDeclareAsync(
                    queue: rabbitConfig.QueueName,
                    durable: rabbitConfig.DurableMode,
                    exclusive: false,
                    autoDelete: rabbitConfig.QueueAutoDelete,
                    arguments: null);
                await _channel.QueuePurgeAsync(rabbitConfig.QueueName);
                break;

            case CommunicationMode.PubSub:
                var exchangeName = string.IsNullOrEmpty(rabbitConfig.ExchangeName)
                    ? $"{rabbitConfig.QueueName}_fanout"
                    : rabbitConfig.ExchangeName;

                await _channel.ExchangeDeclareAsync(
                    exchange: exchangeName,
                    type: ExchangeType.Fanout,
                    durable: rabbitConfig.DurableMode,
                    autoDelete: false);

                for (int i = 0; i < config.ConsumerGroups.Length; i++)
                {
                    var queueName = $"{rabbitConfig.QueueName}_group_{i}";
                    await _channel.QueueDeclareAsync(
                        queue: queueName,
                        durable: rabbitConfig.DurableMode,
                        exclusive: false,
                        autoDelete: rabbitConfig.QueueAutoDelete,
                        arguments: null);
                    await _channel.QueueBindAsync(
                        queue: queueName,
                        exchange: exchangeName,
                        routingKey: string.Empty);
                    await _channel.QueuePurgeAsync(queueName);
                }
                break;

            case CommunicationMode.Streaming:
                // Stream queues can't be purged — delete and recreate
                try { await _channel.QueueDeleteAsync(rabbitConfig.QueueName); }
                catch { /* Queue didn't exist */ }

                var streamArgs = new Dictionary<string, object?>
                {
                    ["x-queue-type"] = "stream"
                };
                await _channel.QueueDeclareAsync(
                    queue: rabbitConfig.QueueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: streamArgs);
                break;
        }
    }

    public void Dispose()
    {
        _channel?.Dispose();
        _connection?.Dispose();
    }
}
