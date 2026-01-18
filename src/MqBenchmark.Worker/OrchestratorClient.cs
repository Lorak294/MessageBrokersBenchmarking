using System.ComponentModel;
using Microsoft.AspNetCore.SignalR.Client;
using MqBenchmark.Core.Config;

namespace MqBenchmark.Worker;

public class OrchestratorClient : BackgroundService
{
    private readonly HubConnection _connection;
    private readonly Worker _worker;

    public OrchestratorClient(IConfiguration config, IEnumerable<IHostedService> hostedServices)
    {
        _worker = hostedServices.OfType<Worker>().Single();
        
        _connection = new HubConnectionBuilder()
            .WithUrl($"{config["OrchestratorUrl"]}?type={config["Role"]}")
            .WithAutomaticReconnect()
            .Build();

        _connection.On<TestConfig>("InitializeTest", async (testConfig) =>
        {
            await _worker.InitializeTestAsync(testConfig);
        });
        
        _connection.On<TestConfig>("StartTest", async (testConfig) =>
        {
            await _worker.InitializeTestAsync(testConfig);
        });
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _connection.StartAsync(stoppingToken);
    }
}