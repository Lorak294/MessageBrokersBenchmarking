using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using MqBenchmark.PgMq.Client;
using MqBenchmark.PgMq.Client.Models;

namespace MqBenchmark.Implementations.PgMq;

public class PgMqConsumer : IMqConsumer
{
    private PgmqClient? _pgmqClient;
    private PgmqNotifyListener? _notifyListener;
    private PgMqConfig? _config;
    private CommunicationMode _communicationMode;
    private string? _consumeQueueName;
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

        if (_notifyListener is not null)
        {
            _notifyListener.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        if (_pgmqClient is not null)
        {
            _pgmqClient.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        _consumptionCts?.Dispose();
        _disposed = true;
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
                // Shared queue — all consumers compete for messages
                _consumeQueueName = _config.QueueName;
                await _pgmqClient.Queues.CreateAsync(_consumeQueueName, unlogged);
                break;

            case CommunicationMode.PubSub:
                // Per-group queue bound to topic pattern
                _consumeQueueName = $"{_config.QueueName}_group_{configuration.ConsumerGroupIndex}";
                await _pgmqClient.Queues.CreateAsync(_consumeQueueName, unlogged);
                
                // Bind to the routing key pattern so topic sends are delivered here
                var routingKey = _config.RoutingKey;
                if (!string.IsNullOrEmpty(routingKey))
                {
                    await _pgmqClient.Topics.BindAsync(routingKey, _consumeQueueName);
                }
                break;

            case CommunicationMode.Streaming:
                // All consumer groups read from the same queue using offset tracking
                _consumeQueueName = _config.QueueName;
                await _pgmqClient.Queues.CreateAsync(_consumeQueueName, unlogged);
                break;
        }

        // For ListenNotify mode (PointToPoint/PubSub only), enable notifications
        if (_communicationMode != CommunicationMode.Streaming
            && _config.ConsumerMode == PgMqConfig.ConsumerModeEnum.ListenNotify)
        {
            await _pgmqClient.Notify.EnableAsync(_consumeQueueName!, _config.NotifyThrottleMs);
            _notifyListener = _pgmqClient.Notify.CreateListener(_consumeQueueName!);
            await _notifyListener.StartAsync();
        }
    }

    public Task SubscribeAsync(Func<Message, Task> messageReceivedHandler)
    {
        if (_pgmqClient is null || _config is null || _consumeQueueName is null)
        {
            throw new InvalidOperationException("Consumer is not initialized. Call InitializeAsync first.");
        }

        _consumptionCts = new CancellationTokenSource();
        var ct = _consumptionCts.Token;

        if (_communicationMode == CommunicationMode.Streaming)
        {
            _consumptionTask = Task.Run(() => RunStreamingLoop(messageReceivedHandler, ct), ct);
        }
        else
        {
            _consumptionTask = _config.ConsumerMode switch
            {
                PgMqConfig.ConsumerModeEnum.ClientPoll => Task.Run(() => RunClientPollLoop(messageReceivedHandler, ct), ct),
                PgMqConfig.ConsumerModeEnum.ServerPoll => Task.Run(() => RunServerPollLoop(messageReceivedHandler, ct), ct),
                PgMqConfig.ConsumerModeEnum.ListenNotify => Task.Run(() => RunListenNotifyLoop(messageReceivedHandler, ct), ct),
                _ => throw new ArgumentOutOfRangeException(nameof(_config.ConsumerMode))
            };
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Streaming mode: reads messages from queue using offset tracking.
    /// Does NOT delete or archive messages — all consumer groups independently read the same data.
    /// </summary>
    private async Task RunStreamingLoop(Func<Message, Task> handler, CancellationToken ct)
    {
        long lastOffset = 0;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var messages = await _pgmqClient!.Read.ReadFromOffsetAsync(
                    _consumeQueueName!, lastOffset, qty: 10, ct: ct);

                if (messages.Count > 0)
                {
                    foreach (var pgmqMsg in messages)
                    {
                        var message = Message.FromBytes(pgmqMsg.Payload);
                        await handler(message);
                        lastOffset = pgmqMsg.MsgId;
                    }
                }
                else
                {
                    await Task.Delay(_config!.PollIntervalMs, ct);
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                Console.WriteLine($"Error consuming PGMQ message (Streaming): {ex.Message}");
                try { await Task.Delay(_config!.PollIntervalMs, ct); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    /// <summary>
    /// Client-side polling: read/pop + Task.Delay back-off when queue is empty.
    /// </summary>
    private async Task RunClientPollLoop(Func<Message, Task> handler, CancellationToken ct)
    {
        var usePopStrategy = _config!.UsePop && _config.MessageReadMode == PgMqConfig.ReadModeEnum.Delete;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var pgmqMsg = await ReadOrPopSingle(usePopStrategy, ct);

                if (pgmqMsg is not null)
                {
                    await ProcessMessage(pgmqMsg, handler, messageDeleted: usePopStrategy, ct);
                }
                else
                {
                    await Task.Delay(_config.PollIntervalMs, ct);
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                Console.WriteLine($"Error consuming PGMQ message: {ex.Message}");
                try { await Task.Delay(_config.PollIntervalMs, ct); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    /// <summary>
    /// Server-side long-polling: pgmq.read_with_poll() blocks in PostgreSQL until
    /// messages arrive or max_poll_seconds elapses.
    /// </summary>
    private async Task RunServerPollLoop(Func<Message, Task> handler, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var messages = await _pgmqClient!.Read.ReadWithPollAsync(
                    _consumeQueueName!,
                    _config!.VisibilityTimeout,
                    qty: 1,
                    maxPollSeconds: _config.MaxPollSeconds,
                    pollIntervalMs: _config.PollIntervalMs,
                    ct: ct);

                if (messages.Count > 0)
                {
                    var pgmqMsg = messages[0];
                    await ProcessMessage(pgmqMsg, handler, messageDeleted: false, ct);
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                Console.WriteLine($"Error consuming PGMQ message (ServerPoll): {ex.Message}");
                try { await Task.Delay(_config!.PollIntervalMs, ct); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    /// <summary>
    /// Event-driven LISTEN/NOTIFY: waits for insert notifications, then pops messages.
    /// Includes periodic fallback sweep.
    /// </summary>
    private async Task RunListenNotifyLoop(Func<Message, Task> handler, CancellationToken ct)
    {
        var fallbackInterval = TimeSpan.FromSeconds(Math.Max(_config!.MaxPollSeconds, 1));

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await _notifyListener!.WaitAsync(fallbackInterval, ct);

                bool gotMessage;
                do
                {
                    var pgmqMsg = await PopSingle(ct);
                    gotMessage = pgmqMsg is not null;

                    if (pgmqMsg is not null)
                    {
                        await ProcessMessage(pgmqMsg, handler, messageDeleted: true, ct);
                    }
                } while (gotMessage && !ct.IsCancellationRequested);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                Console.WriteLine($"Error consuming PGMQ message (ListenNotify): {ex.Message}");
                try { await Task.Delay(_config.PollIntervalMs, ct); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    private async Task<PgmqMessage?> ReadOrPopSingle(bool usePop, CancellationToken ct)
    {
        if (usePop)
        {
            return await PopSingle(ct);
        }

        var messages = await _pgmqClient!.Read.ReadAsync(_consumeQueueName!, _config!.VisibilityTimeout, qty: 1, ct: ct);
        return messages.Count > 0 ? messages[0] : null;
    }

    private async Task<PgmqMessage?> PopSingle(CancellationToken ct)
    {
        var messages = await _pgmqClient!.Pop.PopAsync(_consumeQueueName!, qty: 1, ct: ct);
        return messages.Count > 0 ? messages[0] : null;
    }

    private async Task ProcessMessage(PgmqMessage pgmqMsg, Func<Message, Task> handler, bool messageDeleted, CancellationToken ct)
    {
        if (!messageDeleted)
        {
            if (_config!.MessageReadMode == PgMqConfig.ReadModeEnum.Archive)
            {
                await _pgmqClient!.Archive.ArchiveAsync(_consumeQueueName!, pgmqMsg.MsgId, ct);
            }
            else
            {
                await _pgmqClient!.Delete.DeleteAsync(_consumeQueueName!, pgmqMsg.MsgId, ct);
            }
        }

        var message = Message.FromBytes(pgmqMsg.Payload);
        await handler(message);
    }
}
