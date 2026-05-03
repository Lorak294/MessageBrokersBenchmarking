using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using MqBenchmark.PgMq.Client;

namespace MqBenchmark.Implementations.PgMq;

public class PgMqProducer : IMqProducer
{
    private PgmqClient? _pgmqClient;
    private PgMqConfig? _config;
    private CommunicationMode _communicationMode;

    public void Dispose()
    {
        if (_pgmqClient is not null)
        {
            _pgmqClient.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    public async Task InitializeAsync(MqConfig configuration)
    {
        _config = configuration.ToPgMqConfig();
        _communicationMode = configuration.CommunicationMode;
        _pgmqClient = new PgmqClient(_config.ConnectionString);
        await _pgmqClient.OpenAsync();

        var unlogged = _config.QueueMode == PgMqConfig.QueueModeEnum.Unlogged;

        switch (_communicationMode)
        {
            case CommunicationMode.PointToPoint:
            case CommunicationMode.Streaming:
                // Both use a single shared queue; streaming differs only on the consumer side
                await _pgmqClient.Queues.CreateAsync(_config.QueueName, unlogged);
                break;

            case CommunicationMode.PubSub:
                // Producer doesn't create queues — consumers create their per-group queues and bind.
                // Producer sends via topic routing key. Queues must exist before messages are sent,
                // which is guaranteed because consumers initialize before producers start.
                break;
        }
    }

    public async Task SendAsync(Message message)
    {
        if (_pgmqClient is null || _config is null)
        {
            throw new InvalidOperationException("Producer is not initialized. Call InitializeAsync first.");
        }

        switch (_communicationMode)
        {
            case CommunicationMode.PointToPoint:
            case CommunicationMode.Streaming:
                if (_config.DelaySeconds > 0)
                {
                    await _pgmqClient.Send.SendAsync(_config.QueueName, message.Payload, _config.DelaySeconds);
                }
                else
                {
                    await _pgmqClient.Send.SendAsync(_config.QueueName, message.Payload);
                }
                break;

            case CommunicationMode.PubSub:
                var routingKey = _config.RoutingKey;
                if (string.IsNullOrEmpty(routingKey))
                {
                    throw new InvalidOperationException("RoutingKey must be configured for PubSub mode with PGMQ.");
                }
                if (_config.DelaySeconds > 0)
                {
                    await _pgmqClient.Topics.SendAsync(routingKey, message.Payload, _config.DelaySeconds);
                }
                else
                {
                    await _pgmqClient.Topics.SendAsync(routingKey, message.Payload);
                }
                break;
        }
    }
}
