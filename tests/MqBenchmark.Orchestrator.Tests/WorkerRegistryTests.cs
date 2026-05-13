using FluentAssertions;
using MqBenchmark.Orchestrator.Services;
using Xunit;

namespace MqBenchmark.Orchestrator.Tests;

public class WorkerRegistryTests
{
    private readonly WorkerRegistry _registry = new();

    [Fact]
    public void RegisterAndUnregister_TracksWorkers()
    {
        // Arrange
        var id = Guid.NewGuid();

        // Act
        _registry.RegisterWorker(id, "conn1");

        // Assert
        _registry.ConnectedWorkerCount.Should().Be(1);
        _registry.GetAllWorkers().Should().ContainSingle(w => w.WorkerId == id);

        // Act — unregister
        _registry.UnregisterWorker(id);

        // Assert
        _registry.ConnectedWorkerCount.Should().Be(0);
    }

    [Fact]
    public async Task WaitForAllWorkersReady_CompletesWhenAllReady()
    {
        // Arrange
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();
        _registry.RegisterWorker(id1, "c1");
        _registry.RegisterWorker(id2, "c2");
        _registry.ResetReadiness();
        _registry.SetWorkerRole(id1, WorkerRole.Producer);
        _registry.SetWorkerRole(id2, WorkerRole.Consumer);

        // Act
        var waitTask = _registry.WaitForAllWorkersReadyAsync(TimeSpan.FromSeconds(5));

        // Assert — not yet complete
        waitTask.IsCompleted.Should().BeFalse();

        _registry.UpdateWorkerState(id1, WorkerState.Ready);
        waitTask.IsCompleted.Should().BeFalse();

        _registry.UpdateWorkerState(id2, WorkerState.Ready);
        await waitTask; // should complete
    }

    [Fact]
    public async Task WaitForAllWorkersReady_ImmediateIfAlreadyReady()
    {
        // Arrange
        var id = Guid.NewGuid();
        _registry.RegisterWorker(id, "c1");
        _registry.ResetReadiness();
        _registry.SetWorkerRole(id, WorkerRole.Producer);
        _registry.UpdateWorkerState(id, WorkerState.Ready);

        // Act
        var task = _registry.WaitForAllWorkersReadyAsync(TimeSpan.FromSeconds(1));

        // Assert
        task.IsCompleted.Should().BeTrue();
        await task;
    }

    [Fact]
    public async Task WaitForAllWorkersReady_IgnoresUnassignedWorkers()
    {
        // Arrange
        var assigned = Guid.NewGuid();
        var idle = Guid.NewGuid();
        _registry.RegisterWorker(assigned, "c1");
        _registry.RegisterWorker(idle, "c2");
        _registry.ResetReadiness();
        _registry.SetWorkerRole(assigned, WorkerRole.Producer);
        // idle remains Unassigned

        // Act
        _registry.UpdateWorkerState(assigned, WorkerState.Ready);
        var task = _registry.WaitForAllWorkersReadyAsync(TimeSpan.FromSeconds(1));

        // Assert — completes even though idle worker is not Ready
        task.IsCompleted.Should().BeTrue();
        await task;
    }

    [Fact]
    public async Task WaitForAllWorkersFinished_CompletesWhenAssignedFinish()
    {
        // Arrange
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();
        _registry.RegisterWorker(id1, "c1");
        _registry.RegisterWorker(id2, "c2");
        _registry.ResetReadiness();
        _registry.SetWorkerRole(id1, WorkerRole.Producer);
        _registry.SetWorkerRole(id2, WorkerRole.Consumer);

        // Act
        var waitTask = _registry.WaitForAllWorkersFinishedAsync(TimeSpan.FromSeconds(5));

        // Assert — partial finish doesn't complete
        _registry.UpdateWorkerState(id1, WorkerState.Finished);
        waitTask.IsCompleted.Should().BeFalse();

        _registry.UpdateWorkerState(id2, WorkerState.Finished);
        await waitTask;
    }

    [Fact]
    public async Task MarkProducerDone_TriggersWhenAllDone()
    {
        // Arrange
        _registry.ResetReadiness();
        _registry.SetProducerCount(3);

        // Act
        var waitTask = _registry.WaitForAllProducersDoneAsync(TimeSpan.FromSeconds(5));
        _registry.MarkProducerDone();
        _registry.MarkProducerDone();

        // Assert — not yet
        waitTask.IsCompleted.Should().BeFalse();

        _registry.MarkProducerDone();
        await waitTask; // now completes
    }

    [Fact]
    public async Task MarkProducerDone_PartialDoesNotTrigger()
    {
        // Arrange
        _registry.ResetReadiness();
        _registry.SetProducerCount(3);
        _registry.MarkProducerDone();
        _registry.MarkProducerDone();

        // Act
        var waitTask = _registry.WaitForAllProducersDoneAsync(TimeSpan.FromMilliseconds(100));

        // Assert
        var act = () => waitTask;
        await act.Should().ThrowAsync<TimeoutException>();
    }

    [Fact]
    public void ResetReadiness_ClearsDisconnectedAndResetsState()
    {
        // Arrange
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();
        _registry.RegisterWorker(id1, "c1");
        _registry.RegisterWorker(id2, "c2");
        _registry.SetWorkerRole(id1, WorkerRole.Producer);
        _registry.UpdateWorkerState(id2, WorkerState.Disconnected);

        // Act
        _registry.ResetReadiness();

        // Assert
        _registry.ConnectedWorkerCount.Should().Be(1); // id2 removed
        var workers = _registry.GetAllWorkers();
        workers.Should().ContainSingle();
        workers[0].State.Should().Be(WorkerState.Connected);
        workers[0].Role.Should().Be(WorkerRole.Unassigned);
    }
}
