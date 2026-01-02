using Microsoft.AspNetCore.SignalR;
using MqBenchmark.Orchestrator.Services;

namespace MqBenchmark.Orchestrator;

public static class OrchestratorMethods
{
    public const string InitializeTest = "InitializeTest";
    public const string StartTest = "StartTest";
    public const string WorkerReady = "WorkerReady";
}

public static class OrchestratorQueryParams
{
    public const string IdKey = "workerId";
    public const string TypeKey = "type";
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
        var type = Context.GetHttpContext()?.Request.Query[OrchestratorQueryParams.TypeKey].ToString();
        var idStr = Context.GetHttpContext()?.Request.Query[OrchestratorQueryParams.IdKey].ToString();
        if (Guid.TryParse(idStr, out var workerId))
        {
            Context.Items[OrchestratorQueryParams.IdKey] = workerId;
            Context.Items[OrchestratorQueryParams.TypeKey] = type;

            _workerRegistry.RegisterWorker(workerId, Context.ConnectionId);
            _logger.LogInformation("Client connected: {ConnectionId} as {ConnectionType} (ID: {WorkerId})", 
                Context.ConnectionId, type, workerId);

            if (!string.IsNullOrEmpty(type))
            {
                await Groups.AddToGroupAsync(Context.ConnectionId, type);
            }
        }
        else
        {
            _logger.LogWarning("Invalid connection attempt without valid 'id'. ConnectionId: {ConnectionId}", Context.ConnectionId);
            Context.Abort();
            return;
        }

        await base.OnConnectedAsync();
    }
    
    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        _logger.LogInformation("Client disconnected: {ConnectionId}", Context.ConnectionId);
        if (Context.Items.TryGetValue(OrchestratorQueryParams.IdKey, out var workerIdObj) && workerIdObj is Guid workerId)
        {
            var type = Context.Items[OrchestratorQueryParams.TypeKey]?.ToString();
            _workerRegistry.UpdateWorkerState(workerId, WorkerState.Disconnected);
            _logger.LogInformation("Client disconnected: {WorkerId} type: {ConnectionType}", workerId, type);
            // SignalR automatically removes connections from Groups on disconnect
        }
        await base.OnDisconnectedAsync(exception);
    }

    // Called by workers to indicate they are ready
    public void WorkerReady()
    {
        if (Context.Items.TryGetValue(OrchestratorQueryParams.IdKey, out var workerIdObj) && workerIdObj is Guid workerId)
        {
            _workerRegistry.UpdateWorkerState(workerId, WorkerState.Ready);
            _logger.LogInformation("Worker {Id} is ready", workerId);
        }
        else
        {
            _logger.LogWarning("WorkerReady called from connection {ConnectionId} without known WorkerId.", Context.ConnectionId);
        }
    }
}