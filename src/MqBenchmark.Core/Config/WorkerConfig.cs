namespace MqBenchmark.Core.Config;

public class WorkerConfig
{
    public static class Roles
    {
        public const string Producer = "Producer";
        public const string Consumer = "Consumer";
        public const string Unknown = "Unknown";
    }
    
    public required string WorkerRole { get; set; }
    public required MqConfig MqConfig { get; set; }
    /// <summary>
    /// Total messages this producer will send, or pre-allocation hint for consumers.
    /// </summary>
    public required int MessageCount { get; set; }
    public required int MessageSizeInBytes { get; set; }
    public int? SendFrequencyMps { get; set; }
    /// <summary>
    /// After receiving ProducersDone signal, how long to wait with no messages before stopping.
    /// </summary>
    public int ConsumerIdleTimeoutSeconds { get; set; } = 15;
    
    /// <summary>
    /// Routing plan for producers in PointToPoint mode.
    /// Defines how many messages to send to each consumer group target.
    /// </summary>
    public RoutingPlan? RoutingPlan { get; set; }
}

/// <summary>
/// Defines how a producer should distribute messages across consumer groups.
/// </summary>
public record RoutingPlan
{
    public required RoutingTarget[] Targets { get; init; }
}

/// <summary>
/// A single routing destination with a message quota.
/// </summary>
public record RoutingTarget
{
    /// <summary>
    /// Logical target identifier (e.g., "group_0"). 
    /// The broker implementation translates this to a concrete routing key/topic.
    /// </summary>
    public required string Target { get; init; }
    public required int MessageCount { get; init; }
}
