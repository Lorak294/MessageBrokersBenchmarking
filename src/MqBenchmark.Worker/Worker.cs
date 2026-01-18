using System.Diagnostics;
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

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Worker started. Waiting for commands...");
        
        // This task keeps the service alive. 
        // The actual work is triggered by the SignalR 'StartTest' event.
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    public async Task InitializeTestAsync(TestConfig config)
    {
        var role = config.IsConsumer ? "consumer" : "producer";
        logger.LogInformation($"Initializing benchmark test as {role} ...");
        var implementation = serviceProvider.GetRequiredKeyedService<IMqImplementation>(config.MqConfig.Implementation);

        if (config.IsConsumer)
        {
            _consumer = implementation.CreateConsumer();
            await _consumer.InitializeAsync(config);
        }
        else
        {
            _producer = implementation.CreateProducer();
            await _producer.InitializeAsync(config);
        }
        logger.LogInformation("Benchmark test initialized...");
    }

    public async Task RunTestAsync(TestConfig config)
    {
        logger.LogInformation("Starting benchmark test...");

        if (config.IsConsumer && _consumer != null)
        {
            await ExecuteConsumerTest(config);
        } 
        else if (_producer != null)
        {
            await ExecuteProducerTest(config);
        }
    }

    private async Task ExecuteProducerTest(TestConfig config)
    {
        var payload = new byte[config.MessageSizeInBytes];
        new Random().NextBytes(payload);
        
        if(_producer == null)
            throw new InvalidOperationException("Producer is not initialized!");

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < config.MessageCount; i++)
        {
            await _producer.SendAsync(new Message { Payload = payload });
            // TODO: add delay logic to ensure sending frequency
        }
        sw.Stop();

        logger.LogInformation("Producer Finished. Sent {Count} msgs in {S}s", 
            config.MessageCount, sw.Elapsed.TotalSeconds);
    }

    private async Task ExecuteConsumerTest(TestConfig config)
    {
        int receivedCount = 0;
        var sw = new Stopwatch();
        
        if(_consumer == null)
            throw new InvalidOperationException("Consumer is not initialized!");

        await _consumer.SubscribeAsync(async (data) =>
        {
            if (!sw.IsRunning) sw.Start();
            
            Interlocked.Increment(ref receivedCount);

            if (receivedCount >= config.MessageCount)
            {
                sw.Stop();
                logger.LogInformation("Consumer Finished. Received {Count} msgs in {S}s", 
                    receivedCount, sw.Elapsed.TotalSeconds);
            }
            
            await Task.CompletedTask;
        });
    }
}