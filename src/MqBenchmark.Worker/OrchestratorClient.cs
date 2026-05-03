using Microsoft.AspNetCore.SignalR.Client;
using MqBenchmark.Core.Constants;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.Metrics;

namespace MqBenchmark.Worker;

public class OrchestratorClient : BackgroundService
{
    private readonly ILogger<OrchestratorClient> _logger;
    private readonly HubConnection _connection;
    private readonly Worker _worker;
    
    public OrchestratorClient(IConfiguration config, Worker worker, ILogger<OrchestratorClient> logger)
    {
        _logger = logger;
        _worker = worker;
        
        var url =
            $"{config["OrchestratorUrl"]}?{OrchestratorConstants.TypeKey}=worker&{OrchestratorConstants.IdKey}={worker.Id}";
        _logger.LogInformation("Connecting to orchestrator at {Url}", url);
        // Connect to the hub
        _connection = new HubConnectionBuilder()
            .WithUrl(url)
            .WithAutomaticReconnect()
            .Build();
        
        // Allow long-running tests without server assuming we disconnected
        _connection.ServerTimeout = TimeSpan.FromMinutes(30);
        _connection.KeepAliveInterval = TimeSpan.FromSeconds(15);

        // Register handlers for orchestrator methods
        _connection.On<WorkerConfig>(OrchestratorMethods.InitializeTest, async (testConfig) =>
        {
            try
            {
                await _worker.InitializeTestAsync(testConfig);
                await _connection.InvokeAsync(OrchestratorMethods.WorkerReady);
            } 
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred while initializing the worker.");
            }
        });
        
        _connection.On(OrchestratorMethods.ProducersDone, () =>
        {
            _logger.LogInformation(">>> Received ProducersDone signal from orchestrator via SignalR");
            _worker.SignalProducersDone();
        });
        
        // Run the test on a background thread so the SignalR connection can
        // continue processing keep-alive pings during long-running tests.
        _connection.On(OrchestratorMethods.StartTest, () =>
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    _logger.LogInformation("Received order to start test");
                    
                    await _worker.StartTestAsync();
                    
                    _logger.LogInformation("Test finished successfully. Preparing to send timestamps (role: {Role}).", 
                        _worker.GetTimestampData().Role);
                    
                    // Collect, compress, and send timestamp data in batches
                    var timestampData = _worker.GetTimestampData();
                    var batches = TimestampBatchTransfer.CompressBatches(timestampData);
                    _logger.LogInformation(
                        "Sending {Count} timestamps to orchestrator in {BatchCount} compressed batch(es)",
                        timestampData.Timestamps.Count, batches.Count);
                    
                    foreach (var batch in batches)
                    {
                        await _connection.InvokeAsync(OrchestratorMethods.SubmitTimestampBatch, batch);
                        _logger.LogDebug("Sent batch {BatchIndex}/{TotalBatches}", batch.BatchIndex + 1, batch.TotalBatches);
                    }
                    _logger.LogInformation(">>> Calling WorkerFinished on orchestrator");
                    await _connection.InvokeAsync(OrchestratorMethods.WorkerFinished);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "An error occurred during the test execution.");
                }
            });
        });
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _connection.StartAsync(stoppingToken);
    }
}