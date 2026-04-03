using Microsoft.AspNetCore.SignalR;
using MqBenchmark.Core.Constants;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.Metrics;
using MqBenchmark.Orchestrator.Contracts;

namespace MqBenchmark.Orchestrator.Services;

public class TestScheduler(
    WorkerRegistry workerRegistry,
    IHubContext<OrchestratorHub> hubContext,
    TimestampAggregator timestampAggregator,
    ILogger<TestScheduler> logger)
{
    public async Task InitializeTestAsync(InitializeRequest request)
    {
        int workerCount = workerRegistry.ConnectedWorkerCount;
        if (workerCount == 0)
        {
            throw new InvalidOperationException("No workers connected.");
        }
        logger.LogInformation("Initializing test with config: {@TestConfig}", request);
        
        // Reset timestamp aggregator for new test
        timestampAggregator.Reset();
        
        await SplitWorkersAsync(request.ProducersCount, request.ConsumersCount);
        
        // reset and broadcast reinitialization to workers
        workerRegistry.ResetReadiness();
        await hubContext.Clients.Group(OrchestratorConstants.ProducerGroup).SendAsync(OrchestratorMethods.InitializeTest, new WorkerConfig
        {
            WorkerRole = WorkerConfig.Roles.Producer,
            MessageCount =  request.MessageCount,
            MessageSizeInBytes = request.MessageSizeInBytes,
            MqConfig = request.MqConfig,
            SendFrequencyMps = request.SendFrequencyMps,
        });
        await hubContext.Clients.Group(OrchestratorConstants.ConsumerGroup).SendAsync(OrchestratorMethods.InitializeTest, new WorkerConfig
        {
            WorkerRole = WorkerConfig.Roles.Consumer,
            MessageCount =  request.MessageCount,
            MessageSizeInBytes = request.MessageSizeInBytes,
            MqConfig = request.MqConfig,
        });
        
        // throws TimeoutException on timeout
        await workerRegistry.WaitForAllWorkersReadyAsync(TimeSpan.FromSeconds(30));
    }
    
    public async Task<BenchmarkResults> StartTestAsync()
    {
        logger.LogInformation("Starting benchmark test on all workers.");
        await hubContext.Clients.Group(OrchestratorConstants.ConsumerGroup).SendAsync(OrchestratorMethods.StartTest);
        await hubContext.Clients.Group(OrchestratorConstants.ProducerGroup).SendAsync(OrchestratorMethods.StartTest);
        logger.LogInformation("All workers started.");
        
        // TODO: move this to controller? Or make it configurable?
        await workerRegistry.WaitForAllWorkersFinishedAsync(TimeSpan.FromMinutes(30));
        logger.LogInformation("All workers finished.");
        
        // Compute and return benchmark results from collected timestamps
        var results = timestampAggregator.ComputeResults();
        return results;
    }

    private Task SplitWorkersAsync(int producerCount, int consumerCount)
    {
        int workerCount = workerRegistry.ConnectedWorkerCount;
        if(workerCount < producerCount + consumerCount)
        {
            throw new InvalidOperationException($"Rquested {producerCount+consumerCount} workers, but only {workerCount} are connected.");
        }
        
        // split producers and consumers into groups
        var allWorkers = workerRegistry.GetAllWorkers().ToList();
        var tasks = allWorkers.Select((worker, i) =>
        {
            if(i < producerCount)
            {
                logger.LogInformation("Worker {WorkerID} selected as producer", worker.WorkerId);
                return hubContext.Groups.RemoveFromGroupAsync(worker.ConnectionId, OrchestratorConstants.ConsumerGroup)
                    .ContinueWith(_ => hubContext.Groups.AddToGroupAsync(worker.ConnectionId, OrchestratorConstants.ProducerGroup));
            }
            if(i < producerCount + consumerCount)
            {
                logger.LogInformation("Worker {WorkerID} selected as consumer", worker.WorkerId);
                return hubContext.Groups.RemoveFromGroupAsync(worker.ConnectionId, OrchestratorConstants.ProducerGroup)
                    .ContinueWith(_ => hubContext.Groups.AddToGroupAsync(worker.ConnectionId, OrchestratorConstants.ConsumerGroup));
            }
            return Task.WhenAll(
                hubContext.Groups.RemoveFromGroupAsync(worker.ConnectionId, OrchestratorConstants.ProducerGroup),
                hubContext.Groups.RemoveFromGroupAsync(worker.ConnectionId, OrchestratorConstants.ConsumerGroup)
            ).ContinueWith(_ => 
                logger.LogInformation("Worker {WorkerId} will remain idle", worker.WorkerId)
            );
        });
        return Task.WhenAll(tasks);
    }
}