namespace MqBenchmark.Core.Metrics;

/// <summary>
/// Represents a timestamp record for a single message.
/// </summary>
public record MessageTimestamp
{
    public required Guid MessageId { get; init; }
    public required long TimestampTicks { get; init; }
    
    public DateTime Timestamp => new(TimestampTicks, DateTimeKind.Utc);
}

/// <summary>
/// Contains timestamp data collected by a worker during a benchmark test.
/// </summary>
public record WorkerTimestampData
{
    public required Guid WorkerId { get; init; }
    public required string Role { get; init; }
    public required List<MessageTimestamp> Timestamps { get; init; }
}

/// <summary>
/// Aggregated benchmark results computed from producer and consumer timestamps.
/// </summary>
public record BenchmarkResults
{
    public int TotalMessagesSent { get; init; }
    public int TotalMessagesReceived { get; init; }
    public int MessagesLost { get; init; }
    public double AverageLatencyMs { get; init; }
    public double MinLatencyMs { get; init; }
    public double MaxLatencyMs { get; init; }
    public double P50LatencyMs { get; init; }
    public double P95LatencyMs { get; init; }
    public double P99LatencyMs { get; init; }
    public double TotalDurationSeconds { get; init; }
    public double MessagesPerSecond { get; init; }
    public string? ResultsFileName { get; init; }
}
