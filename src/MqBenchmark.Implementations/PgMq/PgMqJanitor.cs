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
            case CommunicationMode.Streaming:
                await _pgmqClient.Queues.CreateAsync(pgConfig.QueueName, unlogged);
                await _pgmqClient.Queues.PurgeAsync(pgConfig.QueueName);
                break;

            case CommunicationMode.PubSub:
                for (int i = 0; i < config.ConsumerGroups.Length; i++)
                {
                    var queueName = $"{pgConfig.QueueName}_group_{i}";
                    await _pgmqClient.Queues.CreateAsync(queueName, unlogged);
                    await _pgmqClient.Queues.PurgeAsync(queueName);

                    if (!string.IsNullOrEmpty(pgConfig.RoutingKey))
                    {
                        await _pgmqClient.Topics.BindAsync(pgConfig.RoutingKey, queueName);
                    }
                }
                break;
        }
    }

    public void Dispose()
    {
        _pgmqClient?.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
