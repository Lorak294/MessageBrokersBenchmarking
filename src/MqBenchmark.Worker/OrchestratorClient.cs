using System.ComponentModel;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR.Client;
using MqBenchmark.Core.Config;
using MqBenchmark.Orchestrator;

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
            $"{config["OrchestratorUrl"]}?{OrchestratorQueryParams.TypeKey}=worker&{OrchestratorQueryParams.IdKey}={worker.Id}";
        _logger.LogInformation("Connecting to orchestrator at {Url}", url);
        // Connect to the hub
        _connection = new HubConnectionBuilder()
            .WithUrl(url)
            .WithAutomaticReconnect()
            .Build();

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
        
        _connection.On(OrchestratorMethods.StartTest, async () =>
        {
            _logger.LogInformation($"Received order to start test");
            await _worker.StartTestAsync();
        });
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _connection.StartAsync(stoppingToken);
    }
}