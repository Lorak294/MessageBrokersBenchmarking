using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using Npgmq;

namespace MqBenchmark.Implementations.PgMq;

public class PgMqConsumer : IMqConsumer
{
    private NpgmqClient _pgmqClient;
    private PgMqConfig _config;
    private readonly TimeSpan _readTimeout = TimeSpan.FromSeconds(10);
    
    public void Dispose()
    {

    }

    public async Task InitializeAsync(MqConfig configuration)
    {
        // _config = configuration.ToPgMqConfig();
        // _pgmqClient = new NpgmqClient(_config.ConnectionString);
        // await _pgmqClient.InitAsync();
        //
        // var queues = await _pgmqClient.ListQueuesAsync();
        // if (queues.Any(q => q.QueueName == _config.QueueName))
        // {
        //     switch (_config.QueueMode)
        //     {
        //         case PgMqConfig.QueueModeEnum.Unlogged:
        //             await _pgmqClient.CreateUnloggedQueueAsync(_config.QueueName);
        //             break;
        //         case PgMqConfig.QueueModeEnum.NonPartitioned:
        //             await _pgmqClient.CreateQueueAsync(_config.QueueName);
        //             break;
        //     }
        // }
        throw new NotImplementedException();
    }

    public async Task SubscribeAsync(Func<Message, Task> messageReceivedHandler)
    {
        // var msg = await _pgmqClient.ReadAsync<Message>(_config.QueueName, _config.VisibilityTimeout);
        // if (msg is null)
        // {
        //     throw new Exception($"Message {_config.QueueName} not found");
        // }
        // messageReceivedHandler.Invoke(msg.Message);
        // return Task.CompletedTask;
        throw new NotImplementedException();
    }
}