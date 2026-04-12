using MqBenchmark.PgMq.Client.Models;
using Npgsql;

namespace MqBenchmark.PgMq.Client.Operations;

/// <summary>
/// Metrics operations: get queue metrics for a single queue or all queues.
/// These are monitoring commands, not hot-path — use ad-hoc (non-prepared) commands.
/// </summary>
public sealed class MetricsOperations : PgmqOperationsBase
{
    public MetricsOperations(NpgsqlConnection connection) : base(connection) { }

    /// <summary>
    /// Gets metrics for a single queue.
    /// </summary>
    public async Task<PgmqMetrics?> GetAsync(string queueName, CancellationToken ct = default)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT queue_name, queue_length, newest_msg_age_sec, oldest_msg_age_sec, total_messages, scrape_time FROM pgmq.metrics($1)",
            Connection);
        cmd.Parameters.Add(new NpgsqlParameter { Value = queueName });

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return ReadMetrics(reader);
        }
        return null;
    }

    /// <summary>
    /// Gets metrics for all queues.
    /// </summary>
    public async Task<IReadOnlyList<PgmqMetrics>> GetAllAsync(CancellationToken ct = default)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT queue_name, queue_length, newest_msg_age_sec, oldest_msg_age_sec, total_messages, scrape_time FROM pgmq.metrics_all()",
            Connection);

        var metrics = new List<PgmqMetrics>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            metrics.Add(ReadMetrics(reader));
        }
        return metrics;
    }

    private static PgmqMetrics ReadMetrics(NpgsqlDataReader reader)
    {
        return new PgmqMetrics
        {
            QueueName = reader.GetString(0),
            QueueLength = reader.GetInt64(1),
            NewestMsgAgeSec = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
            OldestMsgAgeSec = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
            TotalMessages = reader.GetInt64(4),
            ScrapeTime = reader.IsDBNull(5) ? null : reader.GetDateTime(5)
        };
    }
}
