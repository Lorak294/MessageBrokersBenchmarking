namespace MqBenchmark.Orchestrator.Services;

public enum WorkerState
{
    Connected,
    Ready,
    Running,
    Finished,
    Disconnected
}

public class WorkerInfo
{
    public Guid WorkerId { get; set; }
    public string ConnectionId { get; set; } = string.Empty;
    public WorkerState State { get; set; }
    public DateTime LastHeartbeat { get; set; }
}

public class WorkerRegistry
{
    private readonly Dictionary<Guid, WorkerInfo> _workers = new();
    private TaskCompletionSource? _allWorkersReadyTcs;
    private TaskCompletionSource? _allWorkersFinishedTcs;
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
                // TODO: Consider setting state to disconnected instead of removing
                CheckReadiness();
                CheckFinish();
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
            }
            _allWorkersReadyTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _allWorkersFinishedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
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
            if (_workers.Count > 0 && _workers.Values.All(w => w.State == WorkerState.Finished))
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
    

    public List<WorkerInfo> GetAllWorkers()
    {
        lock (_lock)
        {
            // Return a copy to avoid enumeration issues outside the lock
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
    private void CheckFinish()
    {
        // Internal helper: assumes lock is already held
        if (_workers.Count > 0 && _workers.Values.All(w => w.State == WorkerState.Finished))
        {
            _allWorkersFinishedTcs?.TrySetResult();
        }
    }
}