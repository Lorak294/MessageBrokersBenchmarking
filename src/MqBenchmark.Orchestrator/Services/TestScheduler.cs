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
        int totalConsumers = request.TotalConsumersCount;
        int totalRequired = request.ProducersCount + totalConsumers;
        
        if (workerCount < totalRequired)
        {
            throw new InvalidOperationException(
                $"Requested {totalRequired} workers ({request.ProducersCount} producers + {totalConsumers} consumers), but only {workerCount} are connected.");
        }
        
        logger.LogInformation("Initializing test: Mode={Mode}, Producers={Producers}, ConsumerGroups={Groups}",
            request.CommunicationMode, request.ProducersCount, request.ConsumerGroups);
        
        // Reset timestamp aggregator for new test
        timestampAggregator.Reset();
        
        // Assign workers to roles
        var allWorkers = workerRegistry.GetAllWorkers();
        await AssignWorkersAsync(allWorkers, request);
        
        // Reset and broadcast initialization to workers
        workerRegistry.ResetReadiness();
        
        // Send config to producers
        await hubContext.Clients.Group(OrchestratorConstants.ProducerGroup).SendAsync(
            OrchestratorMethods.InitializeTest, new WorkerConfig
            {
                WorkerRole = WorkerConfig.Roles.Producer,
                MessageCount = request.MessageCount,
                MessageSizeInBytes = request.MessageSizeInBytes,
                MqConfig = request.MqConfig,
                SendFrequencyMps = request.SendFrequencyMps,
                CommunicationMode = request.CommunicationMode,
            });
        
        // Send config to each consumer individually with their group index
        int consumerIndex = 0;
        for (int groupIndex = 0; groupIndex < request.ConsumerGroups.Length; groupIndex++)
        {
            int consumersInGroup = request.ConsumerGroups[groupIndex];
            for (int i = 0; i < consumersInGroup; i++)
            {
                var worker = allWorkers[request.ProducersCount + consumerIndex];
                await hubContext.Clients.Client(worker.ConnectionId).SendAsync(
                    OrchestratorMethods.InitializeTest, new WorkerConfig
                    {
                        WorkerRole = WorkerConfig.Roles.Consumer,
                        MessageCount = request.MessageCount,
                        MessageSizeInBytes = request.MessageSizeInBytes,
                        MqConfig = request.MqConfig,
                        CommunicationMode = request.CommunicationMode,
                        ConsumerGroupIndex = groupIndex,
                        ConsumerGroupCount = request.ConsumerGroups.Length,
                        ConsumerIdleTimeoutSeconds = request.ConsumerIdleTimeoutSeconds,
                    });
                consumerIndex++;
            }
        }
        
        // throws TimeoutException on timeout
        await workerRegistry.WaitForAllWorkersReadyAsync(TimeSpan.FromSeconds(30));
    }
    
    public async Task<BenchmarkResults> StartTestAsync()
    {
        logger.LogInformation("Starting benchmark test on all workers.");
        await hubContext.Clients.Group(OrchestratorConstants.ConsumerGroup).SendAsync(OrchestratorMethods.StartTest);
        await hubContext.Clients.Group(OrchestratorConstants.ProducerGroup).SendAsync(OrchestratorMethods.StartTest);
        logger.LogInformation("All workers started.");
        
        // Wait for producers to finish, then notify consumers
        await workerRegistry.WaitForAllProducersFinishedAsync(TimeSpan.FromMinutes(30));
        logger.LogInformation("All producers finished. Sending ProducersDone signal to consumers.");
        await hubContext.Clients.Group(OrchestratorConstants.ConsumerGroup).SendAsync(OrchestratorMethods.ProducersDone);
        
        // Wait for all workers (including consumers) to finish
        await workerRegistry.WaitForAllWorkersFinishedAsync(TimeSpan.FromMinutes(30));
        logger.LogInformation("All workers finished.");
        
        // Compute and return benchmark results from collected timestamps
        var results = timestampAggregator.ComputeResults();
        return results;
    }

    private async Task AssignWorkersAsync(List<WorkerInfo> allWorkers, InitializeRequest request)
    {
        int totalConsumers = request.TotalConsumersCount;
        var tasks = new List<Task>();
        
        for (int i = 0; i < allWorkers.Count; i++)
        {
            var worker = allWorkers[i];
            if (i < request.ProducersCount)
            {
                logger.LogInformation("Worker {WorkerId} assigned as producer", worker.WorkerId);
                workerRegistry.SetWorkerRole(worker.WorkerId, WorkerRole.Producer);
                tasks.Add(hubContext.Groups.RemoveFromGroupAsync(worker.ConnectionId, OrchestratorConstants.ConsumerGroup));
                tasks.Add(hubContext.Groups.AddToGroupAsync(worker.ConnectionId, OrchestratorConstants.ProducerGroup));
            }
            else if (i < request.ProducersCount + totalConsumers)
            {
                // Determine which consumer group this worker belongs to
                int consumerOffset = i - request.ProducersCount;
                int groupIndex = 0;
                int accumulated = 0;
                for (int g = 0; g < request.ConsumerGroups.Length; g++)
                {
                    accumulated += request.ConsumerGroups[g];
                    if (consumerOffset < accumulated)
                    {
                        groupIndex = g;
                        break;
                    }
                }
                
                logger.LogInformation("Worker {WorkerId} assigned as consumer (group {GroupIndex})", worker.WorkerId, groupIndex);
                workerRegistry.SetWorkerRole(worker.WorkerId, WorkerRole.Consumer, groupIndex);
                tasks.Add(hubContext.Groups.RemoveFromGroupAsync(worker.ConnectionId, OrchestratorConstants.ProducerGroup));
                tasks.Add(hubContext.Groups.AddToGroupAsync(worker.ConnectionId, OrchestratorConstants.ConsumerGroup));
            }
            else
            {
                logger.LogInformation("Worker {WorkerId} will remain idle", worker.WorkerId);
                tasks.Add(hubContext.Groups.RemoveFromGroupAsync(worker.ConnectionId, OrchestratorConstants.ProducerGroup));
                tasks.Add(hubContext.Groups.RemoveFromGroupAsync(worker.ConnectionId, OrchestratorConstants.ConsumerGroup));
            }
        }
        
        await Task.WhenAll(tasks);
    }
}
