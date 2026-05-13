using Microsoft.AspNetCore.SignalR;
using MqBenchmark.Core.Constants;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.Metrics;
using MqBenchmark.Core.MqImplementation;
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
        
        // Validate streaming mode constraints: RabbitMQ and PGMQ streams don't support
        // competing consumers within a group — each consumer reads the full log independently.
        if (request.CommunicationMode == CommunicationMode.Streaming
            && request.MqConfig.Implementation is "RabbitMQ" or "PgMq")
        {
            var invalidGroups = request.ConsumerGroups.Where(g => g > 1).ToArray();
            if (invalidGroups.Length > 0)
            {
                throw new InvalidOperationException(
                    $"Streaming mode for {request.MqConfig.Implementation} does not support multiple consumers per group " +
                    $"(each consumer reads the full log independently). Use 1 consumer per group, e.g. [{string.Join(", ", request.ConsumerGroups.Select(_ => "1"))}] " +
                    $"instead of [{string.Join(", ", request.ConsumerGroups)}].");
            }
        }
        
        logger.LogInformation("Initializing test: Mode={Mode}, Producers={Producers}, ConsumerGroups={Groups}",
            request.CommunicationMode, request.ProducersCount, request.ConsumerGroups);
        
        timestampAggregator.Reset();
        
        // Set expected messages per group for accurate loss calculation
        if (request.CommunicationMode == CommunicationMode.PointToPoint)
        {
            timestampAggregator.SetExpectedMessagesPerGroup(request.GetMessagesPerGroup());
        }
        
        // Reset worker state from any previous test run, then assign new roles
        workerRegistry.ResetReadiness();
        
        // Janitor step: pick first worker, send PrepareInfrastructure, wait for InfrastructureReady
        var allWorkers = workerRegistry.GetAllWorkers();
        var janitorWorker = allWorkers.First();
        var janitorConfig = new JanitorConfig
        {
            MqConfig = request.MqConfig with { CommunicationMode = request.CommunicationMode, ConsumerGroupCount = request.ConsumerGroups.Length },
            CommunicationMode = request.CommunicationMode,
            ConsumerGroups = request.ConsumerGroups
        };
        
        logger.LogInformation("Sending PrepareInfrastructure to janitor worker {WorkerId}", janitorWorker.WorkerId);
        await hubContext.Clients.Client(janitorWorker.ConnectionId).SendAsync(
            OrchestratorMethods.PrepareInfrastructure, janitorConfig);
        
        await workerRegistry.WaitForInfrastructureReadyAsync(TimeSpan.FromSeconds(30));
        logger.LogInformation("Infrastructure ready. Proceeding with worker assignment...");
        
        await AssignWorkersAsync(allWorkers, request);
        
        // Build routing plan for producers
        RoutingPlan? routingPlan = null;
        int totalMessageCount = request.GetTotalMessageCount();
        if (request.CommunicationMode == CommunicationMode.PointToPoint)
        {
            var perGroup = request.GetMessagesPerGroup();
            var targets = new RoutingTarget[perGroup.Length];
            for (int i = 0; i < perGroup.Length; i++)
            {
                targets[i] = new RoutingTarget
                {
                    Target = $"group_{i}",
                    MessageCount = perGroup[i]
                };
            }
            routingPlan = new RoutingPlan { Targets = targets };
        }
        
        // Compute per-producer message count
        int messagesPerProducer = totalMessageCount / request.ProducersCount;
        int producerRemainder = totalMessageCount % request.ProducersCount;
        
        // Send config to each producer with their share of the routing plan
        var producerWorkers = allWorkers.Take(request.ProducersCount).ToList();
        for (int p = 0; p < producerWorkers.Count; p++)
        {
            // Split routing plan proportionally across producers
            RoutingPlan? producerRoutingPlan = null;
            if (routingPlan != null)
            {
                producerRoutingPlan = SplitRoutingPlan(routingPlan, request.ProducersCount, p);
            }
            
            // For PointToPoint, derive message count from routing plan to avoid mismatch
            var producerMessageCount = producerRoutingPlan != null
                ? producerRoutingPlan.Targets.Sum(t => t.MessageCount)
                : messagesPerProducer + (p < producerRemainder ? 1 : 0);
            
            await hubContext.Clients.Client(producerWorkers[p].ConnectionId).SendAsync(
                OrchestratorMethods.InitializeTest, new WorkerConfig
                {
                    WorkerRole = WorkerConfig.Roles.Producer,
                    MessageCount = producerMessageCount,
                    MessageSizeInBytes = request.MessageSizeInBytes,
                    MqConfig = request.MqConfig with
                    {
                        CommunicationMode = request.CommunicationMode,
                        ConsumerGroupCount = request.ConsumerGroups.Length,
                    },
                    SendFrequencyMps = request.SendFrequencyMps,
                    RoutingPlan = producerRoutingPlan,
                });
        }
        
        // Send config to each consumer individually with their group name
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
                        MessageCount = totalMessageCount,
                        MessageSizeInBytes = request.MessageSizeInBytes,
                        MqConfig = request.MqConfig with
                        {
                            CommunicationMode = request.CommunicationMode,
                            ConsumerGroupName = $"group_{groupIndex}",
                            ConsumerGroupCount = request.ConsumerGroups.Length,
                        },
                        ConsumerIdleTimeoutSeconds = OrchestratorConstants.ConsumerIdleWaitTimeSeconds,
                    });
                consumerIndex++;
            }
        }
        
        // throws TimeoutException on timeout
        await workerRegistry.WaitForAllWorkersReadyAsync(TimeSpan.FromSeconds(OrchestratorConstants.WorkerInitializationTimeoutSeconds));
        logger.LogInformation("All workers acknowledged initialization.");
    }
    
    public async Task<BenchmarkResults> StartTestAsync()
    {
        logger.LogInformation("Starting benchmark test on all workers...");
        await hubContext.Clients.Group(OrchestratorConstants.ConsumerGroup).SendAsync(OrchestratorMethods.StartTest);
        await hubContext.Clients.Group(OrchestratorConstants.ProducerGroup).SendAsync(OrchestratorMethods.StartTest);
        logger.LogInformation("All workers started.");
        
        // Wait for producers to finish, then notify consumers
        await workerRegistry.WaitForAllProducersFinishedAsync(TimeSpan.FromMinutes(30));
        logger.LogInformation("All producers finished. Sending ProducersDone signal to consumers...");
        await hubContext.Clients.Group(OrchestratorConstants.ConsumerGroup).SendAsync(OrchestratorMethods.ProducersDone);
        
        // Wait for all workers (including consumers) to finish
        await workerRegistry.WaitForAllWorkersFinishedAsync(TimeSpan.FromMinutes(30));
        logger.LogInformation("All workers finished.");
        
        // Compute and return benchmark results from collected timestamps
        var results = timestampAggregator.ComputeResults();
        return results;
    }

    /// <summary>
    /// Splits a routing plan among multiple producers. Each producer gets a proportional share
    /// of messages for each target.
    /// </summary>
    private static RoutingPlan SplitRoutingPlan(RoutingPlan plan, int producerCount, int producerIndex)
    {
        var targets = new RoutingTarget[plan.Targets.Length];
        for (int i = 0; i < plan.Targets.Length; i++)
        {
            var total = plan.Targets[i].MessageCount;
            var perProducer = total / producerCount;
            var remainder = total % producerCount;
            var count = perProducer + (producerIndex < remainder ? 1 : 0);
            targets[i] = new RoutingTarget { Target = plan.Targets[i].Target, MessageCount = count };
        }
        return new RoutingPlan { Targets = targets };
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
