namespace MqBenchmark.Orchestrator.Services;

public class WorkerRegistry
{
    private readonly HashSet<string> _connectedWorkerIds = new();
    private readonly HashSet<string> _readyWorkerIds = new();
    private TaskCompletionSource? _allWorkersReadyTcs;
    private readonly object _lock = new();

    public int ConnectedWorkerCount
    {
        get { lock(_lock) return _connectedWorkerIds.Count; }
    }

    public void RegisterWorker(string workerId)
    {
        lock (_lock)
        {
            _connectedWorkerIds.Add(workerId);
        }
    }
    
    public void UnregisterWorker(string workerId)
    {
        lock (_lock)
        {
            _connectedWorkerIds.Remove(workerId);
            _readyWorkerIds.Remove(workerId); 
            CheckReadiness();
        }
    }
    
    public void MarkWorkerAsReady(string workerId)
    {
        lock (_lock)
        {
            if (_connectedWorkerIds.Contains(workerId))
            {
                _readyWorkerIds.Add(workerId);
                CheckReadiness();
            }
        }
    }
    
    public void ResetReadiness()
    {
        lock (_lock)
        {
            _readyWorkerIds.Clear();
            _allWorkersReadyTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }
    
    public Task WaitForAllWorkersReadyAsync(TimeSpan timeout)
    {
        Task task;
        lock (_lock)
        {
            if (_connectedWorkerIds.Count > 0 && _readyWorkerIds.Count == _connectedWorkerIds.Count)
            {
                return Task.CompletedTask;
            }
            task = _allWorkersReadyTcs?.Task ?? Task.CompletedTask;
        }
        return task.WaitAsync(timeout);
    }
    
    private void CheckReadiness()
    {
        if (_connectedWorkerIds.Count > 0 && _readyWorkerIds.Count == _connectedWorkerIds.Count)
        {
            _allWorkersReadyTcs?.TrySetResult();
        }
    }
}