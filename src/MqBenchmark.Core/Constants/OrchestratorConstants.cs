namespace MqBenchmark.Core.Constants;

/// <summary>
/// SignalR hub method names shared between orchestrator and workers.
/// </summary>
public static class OrchestratorMethods
{
    public const string InitializeTest = "InitializeTest";
    public const string StartTest = "StartTest";
    public const string WorkerReady = "WorkerReady";
    public const string WorkerFinished = "WorkerFinished";
    public const string ProducerDone = "ProducerDone";
    public const string SubmitTimestampBatch = "SubmitTimestampBatch";
    public const string ProducersDone = "ProducersDone";
    public const string PrepareInfrastructure = "PrepareInfrastructure";
    public const string InfrastructureReady = "InfrastructureReady";
}

/// <summary>
/// Shared constants for orchestrator/worker communication.
/// </summary>
public static class OrchestratorConstants
{
    // Worker groups
    public const string ProducerGroup = "producer";
    public const string ConsumerGroup = "consumer";
    // SignalR connection settings
    public const int ClientTimeoutIntervalMinutes = 30;
    public const int ClientKeepAliveIntervalSeconds = 15;
    // Query params
    public const string IdKey = "workerId";
    public const string TypeKey = "type";
    // Worker behaviour settings
    public const int WorkerInitializationTimeoutSeconds = 30;
    public const int ConsumerIdleWaitTimeSeconds = 10;
}
