namespace MqBenchmark.Orchestrator.Services;

public enum WorkerState
{
    Connected,
    Ready,
    Running,
    Finished,
    Disconnected
}

public enum WorkerRole
{
    Unassigned,
    Producer,
    Consumer
}

public class WorkerInfo
{
    public Guid WorkerId { get; set; }
    public string ConnectionId { get; set; } = string.Empty;
    public WorkerState State { get; set; }
    public WorkerRole Role { get; set; }
    public int? ConsumerGroupIndex { get; set; }
    public DateTime LastHeartbeat { get; set; }
}

public class WorkerRegistry
{
    private readonly Dictionary<Guid, WorkerInfo> _workers = new();
    private TaskCompletionSource? _allWorkersReadyTcs;
    private TaskCompletionSource? _allWorkersFinishedTcs;
    private TaskCompletionSource? _allProducersFinishedTcs;
    private TaskCompletionSource? _allProducersDoneTcs;
    private int _producersDoneCount;
    private int _totalProducerCount;
    private TaskCompletionSource? _infrastructureReadyTcs;
    private readonly object _lock = new();

    public int ConnectedWorkerCount
    {
        get { lock(_lock) return _workers.Count; }
    }

    public void RegisterWorker(Guid workerId, string connectionId)
    {
        lock (_lock)
        {
            _workers[workerId] = new WorkerInfo
            {
                WorkerId = workerId,
                ConnectionId = connectionId,
                State = WorkerState.Connected,
                Role = WorkerRole.Unassigned,
                LastHeartbeat = DateTime.UtcNow
            };
        }
    }
    
    public void UnregisterWorker(Guid workerId)
    {
        lock (_lock)
        {
            if (_workers.Remove(workerId))
            {
                CheckReadiness();
                CheckProducersFinish();
                CheckFinish();
            }
        }
    }
    
    public void SetWorkerRole(Guid workerId, WorkerRole role, int? consumerGroupIndex = null)
    {
        lock (_lock)
        {
            if (_workers.TryGetValue(workerId, out var workerInfo))
            {
                workerInfo.Role = role;
                workerInfo.ConsumerGroupIndex = consumerGroupIndex;
            }
        }
    }
    
    public void UpdateWorkerState(Guid workerId, WorkerState newState)
    {
        lock (_lock)
        {
            if (_workers.TryGetValue(workerId, out var workerInfo))
            {
                workerInfo.State = newState;
                workerInfo.LastHeartbeat = DateTime.UtcNow;
                
                if(newState == WorkerState.Ready)
                {
                    CheckReadiness();
                }

                if (newState == WorkerState.Finished)
                {
                    CheckProducersFinish();
                    CheckFinish();
                }
            }
        }
    }
    
    public void UpdateHeartbeat(Guid workerId)
    {
        lock (_lock)
        {
            if (_workers.TryGetValue(workerId, out var workerInfo))
            {
                workerInfo.LastHeartbeat = DateTime.UtcNow;
            }
        }
    }
    
    public void ResetReadiness()
    {
        lock (_lock)
        {
            // Remove workers that disconnected during or between tests
            var disconnected = _workers.Where(w => w.Value.State == WorkerState.Disconnected)
                .Select(w => w.Key).ToList();
            foreach (var id in disconnected)
            {
                _workers.Remove(id);
            }

            foreach (var worker in _workers.Values)
            {
                worker.State = WorkerState.Connected;
                worker.Role = WorkerRole.Unassigned;
                worker.ConsumerGroupIndex = null;
            }
            _allWorkersReadyTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _allWorkersFinishedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _allProducersFinishedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _allProducersDoneTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _producersDoneCount = 0;
            _totalProducerCount = 0;
            _infrastructureReadyTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }
    
    public Task WaitForAllWorkersReadyAsync(TimeSpan timeout)
    {
        Task task;
        lock (_lock)
        {
            if (_workers.Count > 0 && _workers.Values.All(w => w.State == WorkerState.Ready))
            {
                return Task.CompletedTask;
            }
            
            if (_allWorkersReadyTcs == null || _allWorkersReadyTcs.Task.IsCompleted)
            {
                _allWorkersReadyTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
            
            task = _allWorkersReadyTcs.Task;
        }
        return task.WaitAsync(timeout);
    }
    
    public Task WaitForAllWorkersFinishedAsync(TimeSpan timeout)
    {
        Task task;
        lock (_lock)
        {
            if (_workers.Count > 0 && _workers.Values
                    .Where(w => w.Role != WorkerRole.Unassigned)
                    .All(w => w.State == WorkerState.Finished))
            {
                return Task.CompletedTask;
            }
            
            if (_allWorkersFinishedTcs == null || _allWorkersFinishedTcs.Task.IsCompleted)
            {
                _allWorkersFinishedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
            
            task = _allWorkersFinishedTcs.Task;
        }
        return task.WaitAsync(timeout);
    }
    
    public Task WaitForAllProducersFinishedAsync(TimeSpan timeout)
    {
        Task task;
        lock (_lock)
        {
            var producers = _workers.Values.Where(w => w.Role == WorkerRole.Producer).ToList();
            if (producers.Count > 0 && producers.All(w => w.State == WorkerState.Finished))
            {
                return Task.CompletedTask;
            }
            
            if (_allProducersFinishedTcs == null || _allProducersFinishedTcs.Task.IsCompleted)
            {
                _allProducersFinishedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
            
            task = _allProducersFinishedTcs.Task;
        }
        return task.WaitAsync(timeout);
    }

    public void MarkInfrastructureReady()
    {
        lock (_lock)
        {
            _infrastructureReadyTcs?.TrySetResult();
        }
    }

    /// <summary>
    /// Called when a producer signals it has finished sending all messages (but may not have sent timestamps yet).
    /// </summary>
    public void MarkProducerDone()
    {
        lock (_lock)
        {
            _producersDoneCount++;
            if (_totalProducerCount > 0 && _producersDoneCount >= _totalProducerCount)
            {
                _allProducersDoneTcs?.TrySetResult();
            }
        }
    }

    public void SetProducerCount(int count)
    {
        lock (_lock)
        {
            _totalProducerCount = count;
        }
    }

    public Task WaitForAllProducersDoneAsync(TimeSpan timeout)
    {
        Task task;
        lock (_lock)
        {
            if (_totalProducerCount > 0 && _producersDoneCount >= _totalProducerCount)
            {
                return Task.CompletedTask;
            }

            if (_allProducersDoneTcs == null || _allProducersDoneTcs.Task.IsCompleted)
            {
                _allProducersDoneTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            task = _allProducersDoneTcs.Task;
        }
        return task.WaitAsync(timeout);
    }

    public Task WaitForInfrastructureReadyAsync(TimeSpan timeout)
    {
        Task task;
        lock (_lock)
        {
            if (_infrastructureReadyTcs == null || _infrastructureReadyTcs.Task.IsCompleted)
            {
                return Task.CompletedTask;
            }
            task = _infrastructureReadyTcs.Task;
        }
        return task.WaitAsync(timeout);
    }

    public List<WorkerInfo> GetAllWorkers()
    {
        lock (_lock)
        {
            return _workers.Values.ToList();
        }
    }
    
    private void CheckReadiness()
    {
        // Internal helper: assumes lock is already held
        if (_workers.Count > 0 && _workers.Values.All(w => w.State == WorkerState.Ready))
        {
            _allWorkersReadyTcs?.TrySetResult();
        }
    }
    
    private void CheckProducersFinish()
    {
        var producers = _workers.Values.Where(w => w.Role == WorkerRole.Producer).ToList();
        if (producers.Count > 0 && producers.All(w => w.State == WorkerState.Finished))
        {
            _allProducersFinishedTcs?.TrySetResult();
        }
    }
    
    private void CheckFinish()
    {
        var assigned = _workers.Values.Where(w => w.Role != WorkerRole.Unassigned).ToList();
        if (assigned.Count > 0 && assigned.All(w => w.State == WorkerState.Finished))
        {
            _allWorkersFinishedTcs?.TrySetResult();
        }
    }
}
