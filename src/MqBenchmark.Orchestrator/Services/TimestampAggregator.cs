using System.Collections.Concurrent;
using System.Text.Json;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.Metrics;

namespace MqBenchmark.Orchestrator.Services;

public class TimestampAggregator
{
    public const string ResultsDirectory = "results";
    
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };
    
    private readonly ILogger<TimestampAggregator> _logger;
    private readonly ConcurrentDictionary<Guid, WorkerTimestampData> _workerTimestamps = new();
    
    public int WorkerCount => _workerTimestamps.Count;

    public TimestampAggregator(ILogger<TimestampAggregator> logger)
    {
        _logger = logger;
        Directory.CreateDirectory(ResultsDirectory);
    }
    
    public void SubmitTimestamps(WorkerTimestampData data)
    {
        _workerTimestamps.AddOrUpdate(
            data.WorkerId,
            data,
            (_, existing) =>
            {
                var merged = new List<MessageTimestamp>(existing.Timestamps.Count + data.Timestamps.Count);
                merged.AddRange(existing.Timestamps);
                merged.AddRange(data.Timestamps);
                return existing with { Timestamps = merged };
            });
        _logger.LogInformation("Received {Count} timestamps from worker {WorkerId} ({Role}), total now: {Total}", 
            data.Timestamps.Count, data.WorkerId, data.Role,
            _workerTimestamps[data.WorkerId].Timestamps.Count);
    }
    
    public void Reset()
    {
        _workerTimestamps.Clear();
        _logger.LogInformation("Timestamp aggregator reset");
    }
    
    public BenchmarkResults ComputeResults()
    {
        var allTimestamps = _workerTimestamps.Values.ToList();
        
        // Separate producer and consumer timestamps
        var producerTimestamps = allTimestamps
            .Where(w => w.Role == WorkerConfig.Roles.Producer)
            .SelectMany(w => w.Timestamps)
            .ToDictionary(t => t.MessageId, t => t.TimestampTicks);

        var consumerTimestamps = allTimestamps
            .Where(w => w.Role == WorkerConfig.Roles.Consumer)
            .SelectMany(w => w.Timestamps)
            .ToDictionary(t => t.MessageId, t => t.TimestampTicks);

        _logger.LogInformation("Computing results: {ProducerCount} sent, {ConsumerCount} received",
            producerTimestamps.Count, consumerTimestamps.Count);

        // Calculate latencies for messages that were both sent and received
        var latencies = new Dictionary<Guid, double>();
        foreach (var (messageId, sendTicks) in producerTimestamps)
        {
            if (consumerTimestamps.TryGetValue(messageId, out var receiveTicks))
            {
                var latencyMs = (receiveTicks - sendTicks) / (double)TimeSpan.TicksPerMillisecond;
                latencies[messageId] = latencyMs;
            }
        }

        var messagesLost = producerTimestamps.Count - latencies.Count;
        
        if (latencies.Count == 0)
        {
            _logger.LogWarning("No matched messages found between producers and consumers");
            return new BenchmarkResults
            {
                TotalMessagesSent = producerTimestamps.Count,
                TotalMessagesReceived = consumerTimestamps.Count,
                MessagesLost = messagesLost,
                AverageLatencyMs = 0,
                MinLatencyMs = 0,
                MaxLatencyMs = 0,
                P50LatencyMs = 0,
                P95LatencyMs = 0,
                P99LatencyMs = 0,
                TotalDurationSeconds = 0,
                MessagesPerSecond = 0,
            };
        }

        var sortedLatencies = latencies.Values.OrderBy(l => l).ToList();
        
        // Calculate percentiles
        double GetPercentile(List<double> sorted, double percentile)
        {
            if (sorted.Count == 0) return 0;
            int index = (int)Math.Ceiling(percentile / 100.0 * sorted.Count) - 1;
            return sorted[Math.Max(0, Math.Min(index, sorted.Count - 1))];
        }

        // Calculate total duration (from first send to last receive)
        var firstSendTicks = producerTimestamps.Values.Min();
        var lastReceiveTicks = consumerTimestamps.Values.Max();
        var totalDurationSeconds = (lastReceiveTicks - firstSendTicks) / (double)TimeSpan.TicksPerSecond;

        // Save full results (aggregations + per-message latencies) to file
        var fileName = $"benchmark_{DateTime.UtcNow:yyyy-MM-dd_HHmmss}.json";
        var filePath = Path.Combine(ResultsDirectory, fileName);
        SaveResultsFile(filePath, latencies, producerTimestamps, consumerTimestamps, sortedLatencies,
            messagesLost, totalDurationSeconds, GetPercentile);

        var results = new BenchmarkResults
        {
            TotalMessagesSent = producerTimestamps.Count,
            TotalMessagesReceived = consumerTimestamps.Count,
            MessagesLost = messagesLost,
            AverageLatencyMs = sortedLatencies.Average(),
            MinLatencyMs = sortedLatencies.Min(),
            MaxLatencyMs = sortedLatencies.Max(),
            P50LatencyMs = GetPercentile(sortedLatencies, 50),
            P95LatencyMs = GetPercentile(sortedLatencies, 95),
            P99LatencyMs = GetPercentile(sortedLatencies, 99),
            TotalDurationSeconds = totalDurationSeconds,
            MessagesPerSecond = latencies.Count / totalDurationSeconds,
            ResultsFileName = fileName
        };

        _logger.LogInformation(
            "Benchmark Results: Sent={Sent}, Received={Received}, Lost={Lost}, AvgLatency={Avg:F2}ms, " +
            "P50={P50:F2}ms, P95={P95:F2}ms, P99={P99:F2}ms, Throughput={Throughput:F2} msg/s, File={File}",
            results.TotalMessagesSent, results.TotalMessagesReceived, results.MessagesLost,
            results.AverageLatencyMs, results.P50LatencyMs, results.P95LatencyMs, results.P99LatencyMs,
            results.MessagesPerSecond, fileName);

        return results;
    }

    public string? GetResultsFilePath(string fileName)
    {
        // Prevent path traversal — only allow simple filenames
        if (fileName.Contains("..") || fileName.Contains('/') || fileName.Contains('\\'))
            return null;
        
        var filePath = Path.Combine(ResultsDirectory, fileName);
        return File.Exists(filePath) ? filePath : null;
    }

    private void SaveResultsFile(
        string filePath,
        Dictionary<Guid, double> latencies,
        Dictionary<Guid, long> producerTimestamps,
        Dictionary<Guid, long> consumerTimestamps,
        List<double> sortedLatencies,
        int messagesLost,
        double totalDurationSeconds,
        Func<List<double>, double, double> getPercentile)
    {
        var fullResults = new
        {
            totalMessagesSent = producerTimestamps.Count,
            totalMessagesReceived = consumerTimestamps.Count,
            messagesLost,
            averageLatencyMs = sortedLatencies.Average(),
            minLatencyMs = sortedLatencies.Min(),
            maxLatencyMs = sortedLatencies.Max(),
            p50LatencyMs = getPercentile(sortedLatencies, 50),
            p95LatencyMs = getPercentile(sortedLatencies, 95),
            p99LatencyMs = getPercentile(sortedLatencies, 99),
            totalDurationSeconds,
            messagesPerSecond = latencies.Count / totalDurationSeconds,
            messageLatencies = latencies.ToDictionary(
                kvp => kvp.Key.ToString(),
                kvp => kvp.Value)
        };

        var json = JsonSerializer.Serialize(fullResults, JsonOptions);
        File.WriteAllText(filePath, json);
        _logger.LogInformation("Full results saved to {FilePath} ({Size} bytes)", filePath, json.Length);
    }
}
