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
        
        _connection = new HubConnectionBuilder()
            .WithUrl($"{config["OrchestratorUrl"]}?type={config["Role"]}")
            .WithAutomaticReconnect()
            .Build();

        _connection.On<TestConfig>(OrchestratorMethods.InitializeTest, async (testConfig) =>
        {
            try
            {
                await _worker.InitializeTestAsync(testConfig);
                await _connection.InvokeAsync(OrchestratorMethods.WorkerReady, testConfig);
            } 
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred while initializing the worker.");
            }
        });
        
        _connection.On<TestConfig>(OrchestratorMethods.StartTest, async (testConfig) =>
        {
            _logger.LogInformation($"Starting test with config: {JsonSerializer.Serialize(testConfig)}");
            await Task.CompletedTask;
            //await _worker.RunTestAsync(testConfig);
        });
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _connection.StartAsync(stoppingToken);
    }
}