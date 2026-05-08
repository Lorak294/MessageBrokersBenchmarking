using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using MqBenchmark.PgMq.Client;

namespace MqBenchmark.Implementations.PgMq;

public class PgMqProducer : IMqProducer
{
    private PgmqClient? _pgmqClient;
    private PgMqConfig? _config;
    private CommunicationMode _communicationMode;

    // Buffered producer state
    private readonly List<byte[]> _buffer = new();
    private readonly SemaphoreSlim _bufferSemaphore = new(1, 1);
    private Timer? _lingerTimer;
    private bool _timerRunning;
    private string? _lastRoutingTarget; // Track current target for buffered flush

    public void Dispose()
    {
        _lingerTimer?.Dispose();

        if (_config?.UseBufferedProducer == true && _buffer.Count > 0)
        {
            _bufferSemaphore.Wait();
            try
            {
                if (_buffer.Count > 0)
                    FlushBufferAsync().GetAwaiter().GetResult();
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

        _pgmqClient?.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    public async Task InitializeAsync(MqConfig configuration)
    {
        _config = configuration.ToPgMqConfig();
        _communicationMode = configuration.CommunicationMode;
        _pgmqClient = new PgmqClient(_config.ConnectionString);
        await _pgmqClient.OpenAsync();

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
        switch (_communicationMode)
        {
            case CommunicationMode.PointToPoint:
                // Route to specific group via topic routing key
                var groupKey = PgMqNaming.GroupRoutingKey(routingTarget ?? throw new InvalidOperationException("PointToPoint requires a routing target."));
                if (_config!.DelaySeconds > 0)
                    await _pgmqClient!.Topics.SendAsync(groupKey, message.Payload, _config.DelaySeconds);
                else
                    await _pgmqClient!.Topics.SendAsync(groupKey, message.Payload);
                break;

            case CommunicationMode.PubSub:
                // Broadcast to all groups via broadcast routing key
                var broadcastKey = PgMqNaming.BroadcastRoutingKey();
                if (_config!.DelaySeconds > 0)
                    await _pgmqClient!.Topics.SendAsync(broadcastKey, message.Payload, _config.DelaySeconds);
                else
                    await _pgmqClient!.Topics.SendAsync(broadcastKey, message.Payload);
                break;

            case CommunicationMode.Streaming:
                // Write directly to shared stream queue
                var queueName = PgMqNaming.StreamQueue();
                if (_config!.DelaySeconds > 0)
                    await _pgmqClient!.Send.SendAsync(queueName, message.Payload, _config.DelaySeconds);
                else
                    await _pgmqClient!.Send.SendAsync(queueName, message.Payload);
                break;
        }
    }

    private async Task FlushBufferAsync()
    {
        if (_buffer.Count == 0) return;

        var payloads = _buffer.ToList();
        _buffer.Clear();
        _timerRunning = false;
        _lingerTimer?.Change(Timeout.Infinite, Timeout.Infinite);

        switch (_communicationMode)
        {
            case CommunicationMode.PointToPoint:
                var groupKey = PgMqNaming.GroupRoutingKey(_lastRoutingTarget!);
                if (_config!.DelaySeconds > 0)
                    await _pgmqClient!.Topics.SendBatchAsync(groupKey, payloads, _config.DelaySeconds);
                else
                    await _pgmqClient!.Topics.SendBatchAsync(groupKey, payloads);
                break;

            case CommunicationMode.PubSub:
                var broadcastKey = PgMqNaming.BroadcastRoutingKey();
                if (_config!.DelaySeconds > 0)
                    await _pgmqClient!.Topics.SendBatchAsync(broadcastKey, payloads, _config.DelaySeconds);
                else
                    await _pgmqClient!.Topics.SendBatchAsync(broadcastKey, payloads);
                break;

            case CommunicationMode.Streaming:
                var queueName = PgMqNaming.StreamQueue();
                if (_config!.DelaySeconds > 0)
                    await _pgmqClient!.Send.SendBatchAsync(queueName, payloads, _config.DelaySeconds);
                else
                    await _pgmqClient!.Send.SendBatchAsync(queueName, payloads);
                break;
        }
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
