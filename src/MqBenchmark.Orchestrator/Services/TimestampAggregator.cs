using System.Collections.Concurrent;
using System.Text;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.Metrics;

namespace MqBenchmark.Orchestrator.Services;

public class TimestampAggregator
{
    public const string ResultsDirectory = "results";
    
    private readonly ILogger<TimestampAggregator> _logger;
    private readonly ConcurrentDictionary<Guid, WorkerTimestampData> _workerTimestamps = new();
    
    /// <summary>
    /// Expected messages per consumer group. If null, all groups expect total sent count (PubSub/Streaming).
    /// </summary>
    private int[]? _expectedMessagesPerGroup;
    
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
        _expectedMessagesPerGroup = null;
        _logger.LogInformation("Timestamp aggregator reset");
    }
    
    /// <summary>
    /// Sets expected message counts per consumer group for accurate "messages lost" calculation.
    /// For PointToPoint mode, each group has its own quota. For PubSub/Streaming, pass null (all groups expect all messages).
    /// </summary>
    public void SetExpectedMessagesPerGroup(int[]? expectedPerGroup)
    {
        _expectedMessagesPerGroup = expectedPerGroup;
    }
    
    public BenchmarkResults ComputeResults()
    {
        var allTimestamps = _workerTimestamps.Values.ToList();
        
        // Separate producer and consumer timestamps
        var producerTimestamps = allTimestamps
            .Where(w => w.Role == WorkerConfig.Roles.Producer)
            .SelectMany(w => w.Timestamps)
            .ToDictionary(t => t.MessageId, t => t.TimestampTicks);

        // Group consumer timestamps by consumer group
        var consumersByGroup = allTimestamps
            .Where(w => w.Role == WorkerConfig.Roles.Consumer)
            .GroupBy(w => w.ConsumerGroupIndex)
            .OrderBy(g => g.Key)
            .ToList();

        _logger.LogInformation("Computing results: {ProducerCount} sent, {GroupCount} consumer group(s)",
            producerTimestamps.Count, consumersByGroup.Count);

        // If no consumer groups found, create a single empty group
        if (consumersByGroup.Count == 0)
            consumersByGroup = [new FakeGrouping(0, [])];

        // Helper for percentiles
        double GetPercentile(List<double> sorted, double percentile)
        {
            if (sorted.Count == 0) return 0;
            int index = (int)Math.Ceiling(percentile / 100.0 * sorted.Count) - 1;
            return sorted[Math.Max(0, Math.Min(index, sorted.Count - 1))];
        }

        // Compute per-group results
        var perGroupResults = new List<GroupResults>();
        // Flat list of all consumer timestamps for CSV and duration calculation.
        // Uses a list (not dictionary) because the same MessageId can appear in multiple groups (PubSub/Streaming).
        var allConsumerTimestamps = new List<(Guid MessageId, long Ticks, Guid WorkerId, int GroupIndex)>();

        foreach (var group in consumersByGroup)
        {
            var groupTimestamps = group
                .SelectMany(w => w.Timestamps.Select(t => (t, w.WorkerId)))
                .ToList();

            // Track for CSV
            foreach (var (t, workerId) in groupTimestamps)
                allConsumerTimestamps.Add((t.MessageId, t.TimestampTicks, workerId, group.Key));

            // Calculate latencies for this group
            var groupLatencies = new List<double>();
            foreach (var (t, _) in groupTimestamps)
            {
                if (producerTimestamps.TryGetValue(t.MessageId, out var sendTicks))
                {
                    groupLatencies.Add((t.TimestampTicks - sendTicks) / (double)TimeSpan.TicksPerMillisecond);
                }
            }

            var sorted = groupLatencies.OrderBy(l => l).ToList();
            perGroupResults.Add(new GroupResults
            {
                GroupIndex = group.Key,
                TotalMessagesReceived = groupTimestamps.Count,
                MessagesLost = GetExpectedForGroup(group.Key, producerTimestamps.Count) - groupLatencies.Count,
                AverageLatencyMs = sorted.Count > 0 ? sorted.Average() : 0,
                MinLatencyMs = sorted.Count > 0 ? sorted.Min() : 0,
                MaxLatencyMs = sorted.Count > 0 ? sorted.Max() : 0,
                P50LatencyMs = GetPercentile(sorted, 50),
                P95LatencyMs = GetPercentile(sorted, 95),
                P99LatencyMs = GetPercentile(sorted, 99),
            });
        }

        // Calculate total duration (from first send to last receive across all groups)
        double totalDurationSeconds = 0;
        if (producerTimestamps.Count > 0 && allConsumerTimestamps.Count > 0)
        {
            var firstSendTicks = producerTimestamps.Values.Min();
            var lastReceiveTicks = allConsumerTimestamps.Max(x => x.Ticks);
            totalDurationSeconds = (lastReceiveTicks - firstSendTicks) / (double)TimeSpan.TicksPerSecond;
        }

        // Save raw timestamps to CSV file
        var fileName = $"benchmark_{DateTime.UtcNow:yyyy-MM-dd_HHmmss}.csv";
        var filePath = Path.Combine(ResultsDirectory, fileName);
        SaveResultsFile(filePath, producerTimestamps, allConsumerTimestamps);

        var totalReceived = allConsumerTimestamps.Count;
        var results = new BenchmarkResults
        {
            TotalMessagesSent = producerTimestamps.Count,
            TotalDurationSeconds = totalDurationSeconds,
            MessagesPerSecond = totalDurationSeconds > 0 ? totalReceived / totalDurationSeconds : 0,
            ResultsFileName = fileName,
            PerGroupResults = perGroupResults
        };

        foreach (var gr in perGroupResults)
        {
            _logger.LogInformation(
                "Group {Group}: Received={Received}, Lost={Lost}, AvgLatency={Avg:F2}ms, " +
                "P50={P50:F2}ms, P95={P95:F2}ms, P99={P99:F2}ms",
                gr.GroupIndex, gr.TotalMessagesReceived, gr.MessagesLost,
                gr.AverageLatencyMs, gr.P50LatencyMs, gr.P95LatencyMs, gr.P99LatencyMs);
        }

        _logger.LogInformation(
            "Benchmark Results: Sent={Sent}, Duration={Duration:F2}s, Throughput={Throughput:F2} msg/s, File={File}",
            results.TotalMessagesSent, results.TotalDurationSeconds, results.MessagesPerSecond, fileName);

        return results;
    }

    private sealed class FakeGrouping(int key, List<WorkerTimestampData> items) 
        : List<WorkerTimestampData>(items), IGrouping<int, WorkerTimestampData>
    {
        public int Key => key;
    }

    private int GetExpectedForGroup(int groupIndex, int totalSent)
    {
        if (_expectedMessagesPerGroup != null && groupIndex < _expectedMessagesPerGroup.Length)
            return _expectedMessagesPerGroup[groupIndex];
        return totalSent;
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
        Dictionary<Guid, long> producerTimestamps,
        List<(Guid MessageId, long Ticks, Guid WorkerId, int GroupIndex)> consumerTimestamps)
    {
        // Index consumer timestamps by MessageId for lookup, grouped by (MessageId, GroupIndex)
        var consumerLookup = consumerTimestamps
            .ToLookup(c => c.MessageId);

        var sb = new StringBuilder();
        sb.AppendLine("MessageId,SendTimestampTicks,ReceiveTimestampTicks,ConsumerId,ConsumerGroup");

        foreach (var (messageId, sendTicks) in producerTimestamps.OrderBy(kvp => kvp.Value))
        {
            var entries = consumerLookup[messageId];
            if (entries.Any())
            {
                foreach (var entry in entries)
                {
                    sb.AppendLine($"{messageId},{sendTicks},{entry.Ticks},{entry.WorkerId},{entry.GroupIndex}");
                }
            }
            else
            {
                sb.AppendLine($"{messageId},{sendTicks},,,");
            }
        }

        File.WriteAllText(filePath, sb.ToString());
        _logger.LogInformation("Results CSV saved to {FilePath}", filePath);
    }
}
