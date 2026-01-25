using Microsoft.AspNetCore.SignalR;
using MqBenchmark.Orchestrator.Services;

namespace MqBenchmark.Orchestrator;

public static class OrchestratorMethods
{
    public const string InitializeTest = "InitializeTest";
    public const string StartTest = "StartTest";
    public const string WorkerReady = "WorkerReady";
}

public class OrchestratorHub : Hub
{
    private readonly ILogger<OrchestratorHub> _logger;
    private readonly WorkerRegistry _workerRegistry;
    
    public OrchestratorHub(ILogger<OrchestratorHub> logger, WorkerRegistry workerRegistry)
    {
        _logger = logger;
        _workerRegistry = workerRegistry;
    }
    
    public override async Task OnConnectedAsync()
    {
        var type = Context.GetHttpContext()?.Request.Query["type"].ToString();
        var connectionId = Context.ConnectionId;
        _workerRegistry.RegisterWorker(connectionId);
        _logger.LogInformation("Client connected: {ConnectionId} as {ConnectionType}", connectionId, type);
        
        // Group connections by type
        if (!string.IsNullOrEmpty(type))
        {
            await Groups.AddToGroupAsync(connectionId,"worker"); // may be redundant as _workerRegistry keeps Ids - review later
            await Groups.AddToGroupAsync(connectionId, type);
        }
        
        await base.OnConnectedAsync();
    }
    
    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        var type = Context.GetHttpContext()?.Request.Query["type"].ToString();
        var connectionId = Context.ConnectionId;
        
        _workerRegistry.UnregisterWorker(connectionId);
        _logger.LogInformation("Client disconnected: {ConnectionId} type: {ConnectionType}", connectionId, type);
        
        // Remove from group on disconnect
        if (!string.IsNullOrEmpty(type))
        {
            await Groups.RemoveFromGroupAsync(connectionId, type);
            await Groups.RemoveFromGroupAsync(connectionId,"worker");
        }
        
        await base.OnDisconnectedAsync(exception);
    }

    // Called by workers to indicate they are ready
    public void WorkerReady()
    {
        _workerRegistry.MarkWorkerAsReady(Context.ConnectionId);
        _logger.LogInformation("Worker {Id} is ready", Context.ConnectionId);
    }
}