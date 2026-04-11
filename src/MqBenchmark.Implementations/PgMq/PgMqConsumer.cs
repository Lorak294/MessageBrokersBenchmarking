using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using MqBenchmark.PgMq.Client;

namespace MqBenchmark.Implementations.PgMq;

public class PgMqConsumer : IMqConsumer
{
    private PgmqClient? _pgmqClient;
    private PgMqConfig? _config;
    private CancellationTokenSource? _consumptionCts;
    private Task? _consumptionTask;
    private bool _disposed;

    public void Dispose()
    {
        if (_disposed) return;

        _consumptionCts?.Cancel();

        try
        {
            _consumptionTask?.Wait(TimeSpan.FromSeconds(5));
        }
        catch (Exception)
        {
            // Ignore cancellation or timeout exceptions during disposal
        }

        if (_pgmqClient is not null)
        {
            // TODO: Consider changing IMqConsumer to IAsyncDisposable if possible for other implementations as well
            _pgmqClient.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        _consumptionCts?.Dispose();
        _disposed = true;
    }

    public async Task InitializeAsync(MqConfig configuration)
    {
        _config = configuration.ToPgMqConfig();
        _pgmqClient = new PgmqClient(_config.ConnectionString);
        await _pgmqClient.OpenAsync();

        var unlogged = _config.QueueMode == PgMqConfig.QueueModeEnum.Unlogged;
        await _pgmqClient.CreateQueueAsync(_config.QueueName, unlogged);
    }

    public Task SubscribeAsync(Func<Message, Task> messageReceivedHandler)
    {
        if (_pgmqClient is null || _config is null)
        {
            throw new InvalidOperationException("Consumer is not initialized. Call InitializeAsync first.");
        }

        _consumptionCts = new CancellationTokenSource();
        var ct = _consumptionCts.Token;

        // Determine consumption strategy:
        // - UsePop + Delete mode  => pgmq.pop() (atomic read+delete, one round-trip)
        // - Otherwise             => pgmq.read() + pgmq.delete()/pgmq.archive()
        var usePopStrategy = _config.UsePop && _config.MessageReadMode == PgMqConfig.ReadModeEnum.Delete;

        _consumptionTask = Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    PgmqMessage? pgmqMsg;

                    if (usePopStrategy)
                    {
                        pgmqMsg = await _pgmqClient.PopAsync(_config.QueueName, ct);
                    }
                    else
                    {
                        pgmqMsg = await _pgmqClient.ReadAsync(_config.QueueName, _config.VisibilityTimeout, ct);

                        if (pgmqMsg is not null)
                        {
                            if (_config.MessageReadMode == PgMqConfig.ReadModeEnum.Archive)
                            {
                                await _pgmqClient.ArchiveAsync(_config.QueueName, pgmqMsg.MsgId, ct);
                            }
                            else
                            {
                                await _pgmqClient.DeleteAsync(_config.QueueName, pgmqMsg.MsgId, ct);
                            }
                        }
                    }

                    if (pgmqMsg is not null)
                    {
                        var message = Message.FromBytes(pgmqMsg.Payload);
                        await messageReceivedHandler(message);
                    }
                    else
                    {
                        // No messages available — back off before polling again
                        await Task.Delay(_config.PollIntervalMs, ct);
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error consuming PGMQ message: {ex.Message}");
                    // Brief delay before retrying on error to avoid tight loops
                    try { await Task.Delay(_config.PollIntervalMs, ct); }
                    catch (OperationCanceledException) { break; }
                }
            }
        }, ct);

        return Task.CompletedTask;
    }
}
