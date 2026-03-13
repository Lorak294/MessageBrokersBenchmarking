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
        _pgmqClient = new PgmqClient(_config.ConnectionString);
        await _pgmqClient.OpenAsync();

        var unlogged = _config.QueueMode == PgMqConfig.QueueModeEnum.Unlogged;
        await _pgmqClient.Queues.CreateAsync(_config.QueueName, unlogged);

        // For ListenNotify mode, enable notifications and start the listener
        if (_config.ConsumerMode == PgMqConfig.ConsumerModeEnum.ListenNotify)
        {
            await _pgmqClient.Notify.EnableAsync(_config.QueueName, _config.NotifyThrottleMs);
            _notifyListener = _pgmqClient.Notify.CreateListener(_config.QueueName);
            await _notifyListener.StartAsync();
        }
    }

    public Task SubscribeAsync(Func<Message, Task> messageReceivedHandler)
    {
        if (_pgmqClient is null || _config is null)
        {
            throw new InvalidOperationException("Consumer is not initialized. Call InitializeAsync first.");
        }

        _consumptionCts = new CancellationTokenSource();
        var ct = _consumptionCts.Token;

        _consumptionTask = _config.ConsumerMode switch
        {
            PgMqConfig.ConsumerModeEnum.ClientPoll => Task.Run(() => RunClientPollLoop(messageReceivedHandler, ct), ct),
            PgMqConfig.ConsumerModeEnum.ServerPoll => Task.Run(() => RunServerPollLoop(messageReceivedHandler, ct), ct),
            PgMqConfig.ConsumerModeEnum.ListenNotify => Task.Run(() => RunListenNotifyLoop(messageReceivedHandler, ct), ct),
            _ => throw new ArgumentOutOfRangeException(nameof(_config.ConsumerMode))
        };

        return Task.CompletedTask;
    }

    /// <summary>
    /// Client-side polling: read/pop + Task.Delay back-off when queue is empty.
    /// This is the original behavior.
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
    /// messages arrive or max_poll_seconds elapses. More efficient than client-side polling.
    /// </summary>
    private async Task RunServerPollLoop(Func<Message, Task> handler, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var messages = await _pgmqClient!.Read.ReadWithPollAsync(
                    _config!.QueueName,
                    _config.VisibilityTimeout,
                    qty: 1,
                    maxPollSeconds: _config.MaxPollSeconds,
                    pollIntervalMs: _config.PollIntervalMs,
                    ct: ct);

                if (messages.Count > 0)
                {
                    var pgmqMsg = messages[0];
                    await ProcessMessage(pgmqMsg, handler, messageDeleted: false, ct);
                }
                // No delay needed — read_with_poll already waited server-side
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
    /// Event-driven LISTEN/NOTIFY: waits for insert notifications on a dedicated connection,
    /// then pops messages on the main connection. Includes a periodic fallback sweep
    /// (every MaxPollSeconds) to catch messages missed between throttled notifications.
    /// </summary>
    private async Task RunListenNotifyLoop(Func<Message, Task> handler, CancellationToken ct)
    {
        var fallbackInterval = TimeSpan.FromSeconds(Math.Max(_config!.MaxPollSeconds, 1));

        while (!ct.IsCancellationRequested)
        {
            try
            {
                // Wait for notification or fallback timeout
                var notified = await _notifyListener!.WaitAsync(fallbackInterval, ct);

                // Drain all available messages (notification may indicate multiple inserts)
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

    /// <summary>
    /// Reads or pops a single message depending on the configured strategy.
    /// </summary>
    private async Task<PgmqMessage?> ReadOrPopSingle(bool usePop, CancellationToken ct)
    {
        if (usePop)
        {
            return await PopSingle(ct);
        }

        var messages = await _pgmqClient!.Read.ReadAsync(_config!.QueueName, _config.VisibilityTimeout, qty: 1, ct: ct);
        return messages.Count > 0 ? messages[0] : null;
    }

    /// <summary>
    /// Pops a single message (atomic read+delete).
    /// </summary>
    private async Task<PgmqMessage?> PopSingle(CancellationToken ct)
    {
        var messages = await _pgmqClient!.Pop.PopAsync(_config!.QueueName, qty: 1, ct: ct);
        return messages.Count > 0 ? messages[0] : null;
    }

    /// <summary>
    /// Processes a message: handles delete/archive if needed, then invokes the handler.
    /// </summary>
    private async Task ProcessMessage(PgmqMessage pgmqMsg, Func<Message, Task> handler, bool messageDeleted, CancellationToken ct)
    {
        // If the message hasn't been deleted yet (e.g. read/read_with_poll), handle deletion/archival
        if (!messageDeleted)
        {
            if (_config!.MessageReadMode == PgMqConfig.ReadModeEnum.Archive)
            {
                await _pgmqClient!.Archive.ArchiveAsync(_config.QueueName, pgmqMsg.MsgId, ct);
            }
            else
            {
                await _pgmqClient!.Delete.DeleteAsync(_config.QueueName, pgmqMsg.MsgId, ct);
            }
        }

        var message = Message.FromBytes(pgmqMsg.Payload);
        await handler(message);
    }
}
