using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.Metrics;
using MqBenchmark.Core.MqImplementation;

namespace MqBenchmark.Worker;

public class Worker(
    ILogger<Worker> logger,
    IServiceProvider serviceProvider)
    : BackgroundService
{
    public Guid Id { get; } = Guid.NewGuid();
    private IMqProducer? _producer;
    private IMqConsumer? _consumer;
    private WorkerConfig? _config;
    
    private ConcurrentDictionary<Guid, long> _messageTimestamps = new();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Worker started. Waiting for commands...");
        
        // This task keeps the service alive. 
        // The actual work is triggered by the SignalR 'StartTest' event.
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    public async Task InitializeTestAsync(WorkerConfig config)
    {
        // Clean up any state from a previous test run
        await ResetAsync();
        
        var role = config.WorkerRole;
        logger.LogInformation($"Initializing benchmark test as {role.ToString()} ...");
        var implementation = serviceProvider.GetRequiredKeyedService<IMqImplementation>(config.MqConfig.Implementation);

        // Pre-allocate dictionary with expected capacity for better insert performance
        // Using concurrencyLevel of Environment.ProcessorCount as default
        _messageTimestamps = new ConcurrentDictionary<Guid, long>(
            concurrencyLevel: Environment.ProcessorCount,
            capacity: config.MessageCount);

        switch (role)
        {
            case WorkerConfig.Roles.Consumer:
                _consumer = implementation.CreateConsumer();
                await _consumer.InitializeAsync(config.MqConfig);
                break;
            case WorkerConfig.Roles.Producer:
                _producer = implementation.CreateProducer();
                await _producer.InitializeAsync(config.MqConfig);
                break;
            default:
                throw new InvalidOperationException($"Unsupported worker role: {role}");
        }
        _config = config;
        logger.LogInformation("Benchmark test initialized...");
    }

    /// <summary>
    /// Disposes previous producer/consumer, clears timestamps, and resets all state
    /// so the worker can be reused for another test run.
    /// </summary>
    private Task ResetAsync()
    {
        logger.LogInformation("Resetting worker state for new test run...");
        
        _config = null;
        _messageTimestamps = new ConcurrentDictionary<Guid, long>();
        
        // Dispose old producer/consumer to release connections/channels
        // and stop any lingering subscriptions from a previous run.
        if (_producer != null)
        {
            _producer.Dispose();
            _producer = null;
        }
        if (_consumer != null)
        {
            _consumer.Dispose();
            _consumer = null;
        }
        
        return Task.CompletedTask;
    }

    public async Task StartTestAsync()
    {
        if(_config == null)
        {
            throw new InvalidOperationException("Worker configuration is not initialized.");
        }
        
        logger.LogInformation("Starting benchmark test...");
        logger.LogInformation("Test configuration: {Config}", JsonSerializer.Serialize(_config));

        switch (_config.WorkerRole)
        {
            case WorkerConfig.Roles.Consumer:
                if (_consumer == null)
                    throw new InvalidOperationException("Consumer is not initialized!");
                await ExecuteConsumerTest();
                break;
            case WorkerConfig.Roles.Producer:
                if (_producer == null)
                    throw new InvalidOperationException("Producer is not initialized!");
                await ExecuteProducerTest();
                break;
            default:
                throw new InvalidOperationException($"Unsupported worker role: {_config.WorkerRole}");
        }
    }

    private async Task ExecuteProducerTest()
    {
        if(_config == null)
        {
            throw new InvalidOperationException("Worker configuration is not initialized.");
        }
        
        if(_producer == null)
            throw new InvalidOperationException("Producer is not initialized!");

        RateLimiter? rateLimiter = _config.SendFrequencyMps is > 0
            ? new RateLimiter(_config.SendFrequencyMps.Value)
            : null;

        long totalSendTicks = 0;
        long minSendTicks = long.MaxValue;
        long maxSendTicks = 0;

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < _config.MessageCount; i++)
        {
            if (rateLimiter != null)
                await rateLimiter.WaitAsync();

            var message = Message.CreateMessage(_config.MessageSizeInBytes);
            _messageTimestamps[message.Id] = DateTime.UtcNow.Ticks;

            var sendStart = Stopwatch.GetTimestamp();
            await _producer.SendAsync(message);
            var sendElapsed = Stopwatch.GetTimestamp() - sendStart;

            totalSendTicks += sendElapsed;
            if (sendElapsed < minSendTicks) minSendTicks = sendElapsed;
            if (sendElapsed > maxSendTicks) maxSendTicks = sendElapsed;
        }
        sw.Stop();

        var avgSendMs = (double)totalSendTicks / _config.MessageCount / Stopwatch.Frequency * 1000;
        var minSendMs = (double)minSendTicks / Stopwatch.Frequency * 1000;
        var maxSendMs = (double)maxSendTicks / Stopwatch.Frequency * 1000;
        var avgIntervalMs = sw.Elapsed.TotalMilliseconds / _config.MessageCount;

        logger.LogInformation(
            "Producer Finished. Sent {Count} msgs in {S:F3}s | " +
            "AvgInterval={AvgInterval:F3}ms, AvgSend={AvgSend:F3}ms, MinSend={MinSend:F3}ms, MaxSend={MaxSend:F3}ms",
            _config.MessageCount, sw.Elapsed.TotalSeconds,
            avgIntervalMs, avgSendMs, minSendMs, maxSendMs);
    }

    private async Task ExecuteConsumerTest()
    {
        int receivedCount = 0;
        var sw = new Stopwatch();
        var completionSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        if (_consumer == null)
        {
            throw new InvalidOperationException("Consumer is not initialized!");
        }
        if(_config == null)
        {
            throw new InvalidOperationException("Worker configuration is not initialized.");
        }
        
        await _consumer.SubscribeAsync((message) =>
        {
            // Record timestamp immediately upon receiving
            var timestamp = DateTime.UtcNow.Ticks;
            _messageTimestamps[message.Id] = timestamp;
            
            if (!sw.IsRunning) sw.Start();
            
            var current = Interlocked.Increment(ref receivedCount);

            if (current >= _config.MessageCount)
            {
                sw.Stop();
                logger.LogInformation("Consumer Finished. Received {Count} msgs in {S}s", 
                    current, sw.Elapsed.TotalSeconds);
                completionSource.TrySetResult();
            }
            
            return Task.CompletedTask;
        });

        // Wait for all messages to be received, with a timeout to avoid hanging forever
        var timeout = TimeSpan.FromMinutes(20);
        var completedTask = await Task.WhenAny(completionSource.Task, Task.Delay(timeout));
        if (completedTask != completionSource.Task)
        {
            sw.Stop();
            logger.LogWarning(
                "Consumer timed out after {Timeout}. Received {Count}/{Expected} messages in {S}s",
                timeout, receivedCount, _config.MessageCount, sw.Elapsed.TotalSeconds);
        }
        
        // Stop the consumer's background loop so it doesn't keep reading after the test ends.
        // Timestamps are stored on the Worker, so disposing the consumer here is safe.
        _consumer?.Dispose();
        _consumer = null;
    }
    
    public WorkerTimestampData GetTimestampData()
    {
        var role = _config?.WorkerRole ?? WorkerConfig.Roles.Unknown;
        var timestamps = _messageTimestamps.Select(kvp => new MessageTimestamp
        {
            MessageId = kvp.Key,
            TimestampTicks = kvp.Value
        }).ToList();

        return new WorkerTimestampData
        {
            WorkerId = Id,
            Role = role,
            Timestamps = timestamps,
            ConsumerGroupIndex = _config?.MqConfig.ConsumerGroupIndex ?? 0
        };
    }
}