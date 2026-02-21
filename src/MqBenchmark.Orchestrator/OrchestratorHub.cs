using Microsoft.AspNetCore.SignalR;
using MqBenchmark.Core.Metrics;
using MqBenchmark.Orchestrator.Services;

namespace MqBenchmark.Orchestrator;

public static class OrchestratorMethods
{
    public const string InitializeTest = "InitializeTest";
    public const string StartTest = "StartTest";
    public const string WorkerReady = "WorkerReady";
    public const string WorkerFinished = "WorkerFinished";
    public const string SubmitTimestamps = "SubmitTimestamps";
}

public static class OrchestratorQueryParams
{
    public const string IdKey = "workerId";
    public const string TypeKey = "type";
}

public class OrchestratorHub(
    ILogger<OrchestratorHub> logger, 
    WorkerRegistry workerRegistry,
    TimestampAggregator timestampAggregator)
    : Hub
{
    public override async Task OnConnectedAsync()
    {
        var type = Context.GetHttpContext()?.Request.Query[OrchestratorQueryParams.TypeKey].ToString();
        var idStr = Context.GetHttpContext()?.Request.Query[OrchestratorQueryParams.IdKey].ToString();
        if (Guid.TryParse(idStr, out var workerId))
        {
            Context.Items[OrchestratorQueryParams.IdKey] = workerId;
            Context.Items[OrchestratorQueryParams.TypeKey] = type;

            workerRegistry.RegisterWorker(workerId, Context.ConnectionId);
            logger.LogInformation("Client connected: {ConnectionId} as {ConnectionType} (ID: {WorkerId})", 
                Context.ConnectionId, type, workerId);

            if (!string.IsNullOrEmpty(type))
            {
                await Groups.AddToGroupAsync(Context.ConnectionId, type);
            }
        }
        else
        {
            logger.LogWarning("Invalid connection attempt without valid 'id'. ConnectionId: {ConnectionId}", Context.ConnectionId);
            Context.Abort();
            return;
        }

        await base.OnConnectedAsync();
    }
    
    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        logger.LogInformation("Client disconnected: {ConnectionId}", Context.ConnectionId);
        if (Context.Items.TryGetValue(OrchestratorQueryParams.IdKey, out var workerIdObj) && workerIdObj is Guid workerId)
        {
            var type = Context.Items[OrchestratorQueryParams.TypeKey]?.ToString();
            workerRegistry.UpdateWorkerState(workerId, WorkerState.Disconnected);
            logger.LogInformation("Client disconnected: {WorkerId} type: {ConnectionType}", workerId, type);
            // SignalR automatically removes connections from Groups on disconnect
        }
        await base.OnDisconnectedAsync(exception);
    }

    // Called by workers to indicate they are ready
    public void WorkerReady()
    {
        if (Context.Items.TryGetValue(OrchestratorQueryParams.IdKey, out var workerIdObj) && workerIdObj is Guid workerId)
        {
            workerRegistry.UpdateWorkerState(workerId, WorkerState.Ready);
            logger.LogInformation("Worker {Id} is ready", workerId);
        }
        else
        {
            logger.LogWarning("WorkerReady called from connection {ConnectionId} without known WorkerId.", Context.ConnectionId);
        }
    }

    // Called by workers to indicate they are finished
    public void WorkerFinished()
    {
        if (Context.Items.TryGetValue(OrchestratorQueryParams.IdKey, out var workerIdObj) && workerIdObj is Guid workerId)
        {
            workerRegistry.UpdateWorkerState(workerId, WorkerState.Finished);
            logger.LogInformation("Worker {Id} is ready", workerId);
        }
        else
        {
            logger.LogWarning("WorkerFinished called from connection {ConnectionId} without known WorkerId.", Context.ConnectionId);
        }
    }

    // Called by workers to submit their timestamp data
    public void SubmitTimestamps(WorkerTimestampData data)
    {
        if (Context.Items.TryGetValue(OrchestratorQueryParams.IdKey, out var workerIdObj) && workerIdObj is Guid workerId)
        {
            logger.LogInformation("Worker {Id} submitted {Count} timestamps", workerId, data.Timestamps.Count);
            timestampAggregator.SubmitTimestamps(data);
        }
        else
        {
            logger.LogWarning("SubmitTimestamps called from connection {ConnectionId} without known WorkerId.", Context.ConnectionId);
        }
    }
}