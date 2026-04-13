using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using MqBenchmark.PgMq.Client;

namespace MqBenchmark.Implementations.PgMq;

public class PgMqProducer : IMqProducer
{
    private IPgmqClient? _pgmqClient;
    private PgMqConfig? _config;
    private CommunicationMode _communicationMode;

    // Buffered producer state
    private readonly List<byte[]> _buffer = new();
    private readonly SemaphoreSlim _bufferSemaphore = new(1, 1);
    private Timer? _lingerTimer;
    private bool _timerRunning;
    private string? _lastRoutingTarget; // Track current target for buffered flush

    public PgMqProducer() { }

    internal PgMqProducer(IPgmqClient pgmqClient)
    {
        _pgmqClient = pgmqClient;
    }

    public async ValueTask DisposeAsync()
    {
        _lingerTimer?.Dispose();

        if (_config?.UseBufferedProducer == true && _buffer.Count > 0)
        {
            await _bufferSemaphore.WaitAsync();
            try
            {
                if (_buffer.Count > 0)
                    await FlushBufferAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error flushing PGMQ producer buffer on dispose: {ex.Message}");
            }
            finally
            {
                _bufferSemaphore.Release();
            }
        }

        if (_pgmqClient is not null) await _pgmqClient.DisposeAsync();
    }

    public async Task InitializeAsync(MqConfig configuration)
    {
        _config = configuration.ToPgMqConfig();
        _communicationMode = configuration.CommunicationMode;

        if (_pgmqClient is null)
        {
            _pgmqClient = new PgmqClient(_config.ConnectionString);
            await _pgmqClient.OpenAsync();
        }

        // For Streaming, ensure the queue exists (producer writes directly)
        if (_communicationMode == CommunicationMode.Streaming)
        {
            var unlogged = _config.QueueMode == PgMqConfig.QueueModeEnum.Unlogged;
            await _pgmqClient.Queues.CreateAsync(PgMqNaming.StreamQueue(), unlogged);
        }

        if (_config.UseBufferedProducer)
        {
            _lingerTimer = new Timer(OnLingerTimerFired, null, Timeout.Infinite, Timeout.Infinite);
        }
    }

    public async Task SendAsync(Message message, string? routingTarget = null)
    {
        if (_pgmqClient is null || _config is null)
            throw new InvalidOperationException("Producer is not initialized.");

        if (!_config.UseBufferedProducer)
        {
            await SendSingleAsync(message, routingTarget);
            return;
        }

        await _bufferSemaphore.WaitAsync();
        try
        {
            // If target changed, flush existing buffer first
            if (_buffer.Count > 0 && _lastRoutingTarget != routingTarget)
            {
                await FlushBufferAsync();
            }
            _lastRoutingTarget = routingTarget;

            _buffer.Add(message.Payload);
            if (_buffer.Count >= _config.ProducerBatchSize)
            {
                await FlushBufferAsync();
            }
            else if (!_timerRunning)
            {
                _lingerTimer?.Change(_config.ProducerLingerMs, Timeout.Infinite);
                _timerRunning = true;
            }
        }
        finally
        {
            _bufferSemaphore.Release();
        }
    }

    private async Task SendSingleAsync(Message message, string? routingTarget)
    {
        var (target, isDirectQueue) = ResolveSendTarget(routingTarget);
        if (isDirectQueue)
            await _pgmqClient!.Send.SendAsync(target, message.Payload);
        else
            await _pgmqClient!.Topics.SendAsync(target, message.Payload);
    }

    private async Task FlushBufferAsync()
    {
        if (_buffer.Count == 0) return;

        var payloads = _buffer.ToList();
        _buffer.Clear();
        _timerRunning = false;
        _lingerTimer?.Change(Timeout.Infinite, Timeout.Infinite);

        var (target, isDirectQueue) = ResolveSendTarget(_lastRoutingTarget);
        if (isDirectQueue)
            await _pgmqClient!.Send.SendBatchAsync(target, payloads);
        else
            await _pgmqClient!.Topics.SendBatchAsync(target, payloads);
    }

    /// <summary>
    /// Resolves the send target name and whether it's a direct queue (true) or topic routing key (false).
    /// </summary>
    private (string target, bool isDirectQueue) ResolveSendTarget(string? routingTarget)
    {
        return _communicationMode switch
        {
            CommunicationMode.PointToPoint => (
                PgMqNaming.GroupRoutingKey(routingTarget ?? throw new InvalidOperationException("PointToPoint requires a routing target.")),
                false),
            CommunicationMode.PubSub => (PgMqNaming.BroadcastRoutingKey(), false),
            CommunicationMode.Streaming => (PgMqNaming.StreamQueue(), true),
            _ => throw new InvalidOperationException($"Unsupported mode: {_communicationMode}")
        };
    }

    private void OnLingerTimerFired(object? state)
    {
        _bufferSemaphore.Wait();
        try
        {
            if (_buffer.Count > 0)
                FlushBufferAsync().GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error flushing PGMQ producer buffer (timer): {ex.Message}");
        }
        finally
        {
            _bufferSemaphore.Release();
        }
    }
}
