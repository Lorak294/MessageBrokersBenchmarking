using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using MqBenchmark.PgMq.Client;

namespace MqBenchmark.Implementations.PgMq;

public class PgMqProducer : IMqProducer
{
    private PgmqClient? _pgmqClient;
    private PgMqConfig? _config;

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
        _pgmqClient = new PgmqClient(_config.ConnectionString);
        await _pgmqClient.OpenAsync();

        var unlogged = _config.QueueMode == PgMqConfig.QueueModeEnum.Unlogged;
        await _pgmqClient.Queues.CreateAsync(_config.QueueName, unlogged);
    }

    public async Task SendAsync(Message message)
    {
        if (_pgmqClient is null || _config is null)
        {
            throw new InvalidOperationException("Producer is not initialized. Call InitializeAsync first.");
        }

        if (_config.DelaySeconds > 0)
        {
            await _pgmqClient.Send.SendAsync(_config.QueueName, message.Payload, _config.DelaySeconds);
        }
        else
        {
            await _pgmqClient.Send.SendAsync(_config.QueueName, message.Payload);
        }
    }
}
