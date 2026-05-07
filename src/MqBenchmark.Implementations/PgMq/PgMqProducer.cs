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

    public void Dispose()
    {
        _lingerTimer?.Dispose();

        // Flush remaining buffered messages
        if (_config?.UseBufferedProducer == true && _buffer.Count > 0)
        {
            _bufferSemaphore.Wait();
            try
            {
                if (_buffer.Count > 0)
                {
                    FlushBufferAsync().GetAwaiter().GetResult();
                }
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

        if (_pgmqClient is not null)
        {
            _pgmqClient.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
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
            case CommunicationMode.Streaming:
                await _pgmqClient.Queues.CreateAsync(_config.QueueName, unlogged);
                break;

            case CommunicationMode.PubSub:
                // Producer doesn't create queues — janitor handles that.
                break;
        }

        if (_config.UseBufferedProducer)
        {
            _lingerTimer = new Timer(OnLingerTimerFired, null, Timeout.Infinite, Timeout.Infinite);
        }
    }

    public async Task SendAsync(Message message)
    {
        if (_pgmqClient is null || _config is null)
        {
            throw new InvalidOperationException("Producer is not initialized. Call InitializeAsync first.");
        }

        if (!_config.UseBufferedProducer)
        {
            await SendSingleAsync(message);
            return;
        }

        await _bufferSemaphore.WaitAsync();
        try
        {
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

    private async Task SendSingleAsync(Message message)
    {
        switch (_communicationMode)
        {
            case CommunicationMode.PointToPoint:
            case CommunicationMode.Streaming:
                if (_config!.DelaySeconds > 0)
                    await _pgmqClient!.Send.SendAsync(_config.QueueName, message.Payload, _config.DelaySeconds);
                else
                    await _pgmqClient!.Send.SendAsync(_config.QueueName, message.Payload);
                break;

            case CommunicationMode.PubSub:
                var routingKey = _config!.RoutingKey;
                if (string.IsNullOrEmpty(routingKey))
                    throw new InvalidOperationException("RoutingKey must be configured for PubSub mode with PGMQ.");
                if (_config.DelaySeconds > 0)
                    await _pgmqClient!.Topics.SendAsync(routingKey, message.Payload, _config.DelaySeconds);
                else
                    await _pgmqClient!.Topics.SendAsync(routingKey, message.Payload);
                break;
        }
    }

    private async Task FlushBufferAsync()
    {
        // Assumes semaphore is held by caller
        if (_buffer.Count == 0) return;

        var payloads = _buffer.ToList();
        _buffer.Clear();
        _timerRunning = false;
        _lingerTimer?.Change(Timeout.Infinite, Timeout.Infinite);

        switch (_communicationMode)
        {
            case CommunicationMode.PointToPoint:
            case CommunicationMode.Streaming:
                if (_config!.DelaySeconds > 0)
                    await _pgmqClient!.Send.SendBatchAsync(_config.QueueName, payloads, _config.DelaySeconds);
                else
                    await _pgmqClient!.Send.SendBatchAsync(_config.QueueName, payloads);
                break;

            case CommunicationMode.PubSub:
                var routingKey = _config!.RoutingKey;
                if (string.IsNullOrEmpty(routingKey))
                    throw new InvalidOperationException("RoutingKey must be configured for PubSub mode with PGMQ.");
                if (_config.DelaySeconds > 0)
                    await _pgmqClient!.Topics.SendBatchAsync(routingKey, payloads, _config.DelaySeconds);
                else
                    await _pgmqClient!.Topics.SendBatchAsync(routingKey, payloads);
                break;
        }
    }

    private void OnLingerTimerFired(object? state)
    {
        // Timer callback — flush any pending messages
        _bufferSemaphore.Wait();
        try
        {
            if (_buffer.Count > 0)
            {
                FlushBufferAsync().GetAwaiter().GetResult();
            }
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
