using Microsoft.AspNetCore.SignalR;
using MqBenchmark.Core.Constants;
using MqBenchmark.Core.Metrics;
using MqBenchmark.Orchestrator.Services;

namespace MqBenchmark.Orchestrator;


public class OrchestratorHub(
    ILogger<OrchestratorHub> logger, 
    WorkerRegistry workerRegistry,
    TimestampAggregator timestampAggregator)
    : Hub
{
    public override async Task OnConnectedAsync()
    {
        var type = Context.GetHttpContext()?.Request.Query[OrchestratorConstants.TypeKey].ToString();
        var idStr = Context.GetHttpContext()?.Request.Query[OrchestratorConstants.IdKey].ToString();
        if (Guid.TryParse(idStr, out var workerId))
        {
            Context.Items[OrchestratorConstants.IdKey] = workerId;
            Context.Items[OrchestratorConstants.TypeKey] = type;

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
        if (Context.Items.TryGetValue(OrchestratorConstants.IdKey, out var workerIdObj) && workerIdObj is Guid workerId)
        {
            var type = Context.Items[OrchestratorConstants.TypeKey]?.ToString();
            workerRegistry.UpdateWorkerState(workerId, WorkerState.Disconnected);
            logger.LogInformation("Client disconnected: {WorkerId} type: {ConnectionType}", workerId, type);
            // SignalR automatically removes connections from Groups on disconnect
        }
        await base.OnDisconnectedAsync(exception);
    }

    // Called by workers to indicate they are ready
    public void WorkerReady()
    {
        if (Context.Items.TryGetValue(OrchestratorConstants.IdKey, out var workerIdObj) && workerIdObj is Guid workerId)
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
        if (Context.Items.TryGetValue(OrchestratorConstants.IdKey, out var workerIdObj) && workerIdObj is Guid workerId)
        {
            var workers = workerRegistry.GetAllWorkers();
            var worker = workers.FirstOrDefault(w => w.WorkerId == workerId);
            logger.LogInformation(">>> Worker {Id} finished (Role: {Role})", workerId, worker?.Role);
            workerRegistry.UpdateWorkerState(workerId, WorkerState.Finished);
        }
        else
        {
            logger.LogWarning("WorkerFinished called from connection {ConnectionId} without known WorkerId.", Context.ConnectionId);
        }
    }
    
    // Called by workers to submit compressed timestamp batches
    public void SubmitTimestampBatch(CompressedTimestampBatch batch)
    {
        if (Context.Items.TryGetValue(OrchestratorConstants.IdKey, out var workerIdObj) && workerIdObj is Guid workerId)
        {
            var data = TimestampBatchTransfer.DecompressBatch(batch);
            logger.LogInformation(
                "Worker {Id} submitted compressed batch {BatchIndex}/{TotalBatches} with {Count} timestamps",
                workerId, batch.BatchIndex + 1, batch.TotalBatches, data.Timestamps.Count);
            timestampAggregator.SubmitTimestamps(data);
        }
        else
        {
            logger.LogWarning("SubmitTimestampBatch called from connection {ConnectionId} without known WorkerId.", Context.ConnectionId);
        }
    }
}