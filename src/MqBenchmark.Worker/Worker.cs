using System.Diagnostics;
using System.Text.Json;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;

namespace MqBenchmark.Worker;

public class Worker(
    ILogger<Worker> logger,
    IServiceProvider serviceProvider)
    : BackgroundService
{
    private IMqProducer? _producer;
    private IMqConsumer? _consumer;
    public Guid Id { get; } = Guid.NewGuid();
    private WorkerConfig? _config;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Worker started. Waiting for commands...");
        
        // This task keeps the service alive. 
        // The actual work is triggered by the SignalR 'StartTest' event.
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    public async Task InitializeTestAsync(WorkerConfig config)
    {
        var role = config.WorkerRole;
        logger.LogInformation($"Initializing benchmark test as {role.ToString()} ...");
        var implementation = serviceProvider.GetRequiredKeyedService<IMqImplementation>(config.MqConfig.Implementation);

        switch (role)
        {
            case WorkerConfig.Role.Consumer:
                _consumer = implementation.CreateConsumer();
                await _consumer.InitializeAsync(config.MqConfig);
                break;
            case WorkerConfig.Role.Producer:
                _producer = implementation.CreateProducer();
                await _producer.InitializeAsync(config.MqConfig);
                break;
            default:
                throw new InvalidOperationException($"Unsupported worker role: {role}");
        }
        _config = config;
        logger.LogInformation("Benchmark test initialized...");
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
            case WorkerConfig.Role.Consumer:
                if (_consumer == null)
                    throw new InvalidOperationException("Consumer is not initialized!");
                await ExecuteConsumerTest();
                break;
            case WorkerConfig.Role.Producer:
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
        
        var payload = new byte[_config.MessageSizeInBytes];
        new Random().NextBytes(payload);
        
        if(_producer == null)
            throw new InvalidOperationException("Producer is not initialized!");

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < _config.MessageCount; i++)
        {
            await _producer.SendAsync(new Message { Payload = payload });
            // TODO: add delay logic to ensure sending frequency
        }
        sw.Stop();

        logger.LogInformation("Producer Finished. Sent {Count} msgs in {S}s", 
            _config.MessageCount, sw.Elapsed.TotalSeconds);
    }

    private async Task ExecuteConsumerTest()
    {
        int receivedCount = 0;
        var sw = new Stopwatch();

        if (_consumer == null)
        {
            throw new InvalidOperationException("Consumer is not initialized!");
        }
        if(_config == null)
        {
            throw new InvalidOperationException("Worker configuration is not initialized.");
        }

        await _consumer.SubscribeAsync(async (data) =>
        {
            if (!sw.IsRunning) sw.Start();
            
            Interlocked.Increment(ref receivedCount);

            if (receivedCount >= _config.MessageCount)
            {
                sw.Stop();
                logger.LogInformation("Consumer Finished. Received {Count} msgs in {S}s", 
                    receivedCount, sw.Elapsed.TotalSeconds);
            }
            
            await Task.CompletedTask;
        });
    }
}