namespace MqBenchmark.PgMq.Client;

public interface IPgmqClient : IAsyncDisposable
{
    ISendOperations Send { get; }
    IReadOperations Read { get; }
    IPopOperations Pop { get; }
    IDeleteOperations Delete { get; }
    IArchiveOperations Archive { get; }
    IQueueOperations Queues { get; }
    ITopicOperations Topics { get; }
    INotifyOperations Notify { get; }

    Task OpenAsync(CancellationToken ct = default);
}
