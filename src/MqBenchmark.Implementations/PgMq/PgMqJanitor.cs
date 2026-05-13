using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using MqBenchmark.PgMq.Client;

namespace MqBenchmark.Implementations.PgMq;

public class PgMqJanitor : IMqJanitor
{
    private PgmqClient? _pgmqClient;

    public async Task PrepareInfrastructureAsync(JanitorConfig config)
    {
        var pgConfig = config.MqConfig.ToPgMqConfig();
        var unlogged = pgConfig.QueueMode == PgMqConfig.QueueModeEnum.Unlogged;

        _pgmqClient = new PgmqClient(pgConfig.ConnectionString);
        await _pgmqClient.OpenAsync();

        switch (config.CommunicationMode)
        {
            case CommunicationMode.PointToPoint:
                await PreparePointToPoint(config, pgConfig, unlogged);
                break;
            case CommunicationMode.PubSub:
                await PreparePubSub(config, pgConfig, unlogged);
                break;
            case CommunicationMode.Streaming:
                await PrepareStreaming(pgConfig, unlogged);
                break;
        }
    }

    private async Task PreparePointToPoint(JanitorConfig config, PgMqConfig pgConfig, bool unlogged)
    {
        // Per-group queues with topic routing
        for (int i = 0; i < config.ConsumerGroups.Length; i++)
        {
            var groupName = $"group_{i}";
            var queueName = PgMqNaming.GroupQueue(groupName);
            await _pgmqClient!.Queues.CreateAsync(queueName, unlogged);
            await _pgmqClient.Queues.PurgeAsync(queueName);
            await _pgmqClient.Topics.BindAsync(PgMqNaming.GroupRoutingKey(groupName), queueName);
        }
    }

    private async Task PreparePubSub(JanitorConfig config, PgMqConfig pgConfig, bool unlogged)
    {
        // Per-group queues all bound to broadcast routing key
        for (int i = 0; i < config.ConsumerGroups.Length; i++)
        {
            var groupName = $"group_{i}";
            var queueName = PgMqNaming.GroupQueue(groupName);
            await _pgmqClient!.Queues.CreateAsync(queueName, unlogged);
            await _pgmqClient.Queues.PurgeAsync(queueName);
            await _pgmqClient.Topics.BindAsync(PgMqNaming.BroadcastRoutingKey(), queueName);
        }
    }

    private async Task PrepareStreaming(PgMqConfig pgConfig, bool unlogged)
    {
        var queueName = PgMqNaming.StreamQueue();
        await _pgmqClient!.Queues.CreateAsync(queueName, unlogged);
        await _pgmqClient.Queues.PurgeAsync(queueName);
    }

    public async ValueTask DisposeAsync()
    {
        if (_pgmqClient is not null) await _pgmqClient.DisposeAsync();
    }
}
