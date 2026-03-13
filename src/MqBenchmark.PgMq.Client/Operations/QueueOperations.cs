using MqBenchmark.PgMq.Client.Models;
using Npgsql;

namespace MqBenchmark.PgMq.Client.Operations;

/// <summary>
/// Queue management operations: create, drop, purge, list.
/// These are management commands, not hot-path — use ad-hoc (non-prepared) commands.
/// </summary>
public sealed class QueueOperations : PgmqOperationsBase
{
    public QueueOperations(NpgsqlConnection connection) : base(connection) { }

    /// <summary>
    /// Creates a PGMQ queue. Idempotent — does not error if the queue already exists.
    /// </summary>
    public async Task CreateAsync(string queueName, bool unlogged = false, CancellationToken ct = default)
    {
        var fn = unlogged ? "pgmq.create_unlogged" : "pgmq.create";
        await using var cmd = new NpgsqlCommand($"SELECT {fn}($1)", Connection);
        cmd.Parameters.Add(new NpgsqlParameter { Value = queueName });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Drops a PGMQ queue and its associated tables.
    /// </summary>
    public async Task<bool> DropAsync(string queueName, CancellationToken ct = default)
    {
        await using var cmd = new NpgsqlCommand("SELECT pgmq.drop_queue($1)", Connection);
        cmd.Parameters.Add(new NpgsqlParameter { Value = queueName });
        var result = await cmd.ExecuteScalarAsync(ct);
        return result is true;
    }

    /// <summary>
    /// Purges all messages from a queue using TRUNCATE. Returns the number of messages purged.
    /// </summary>
    public async Task<long> PurgeAsync(string queueName, CancellationToken ct = default)
    {
        await using var cmd = new NpgsqlCommand("SELECT pgmq.purge_queue($1)", Connection);
        cmd.Parameters.Add(new NpgsqlParameter { Value = queueName });
        var result = await cmd.ExecuteScalarAsync(ct);
        return result is long l ? l : 0;
    }

    /// <summary>
    /// Lists all PGMQ queues. Returns queue metadata from pgmq.queue_record.
    /// </summary>
    public async Task<IReadOnlyList<PgmqQueueInfo>> ListAsync(CancellationToken ct = default)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT queue_name, is_partitioned, is_unlogged, created_at FROM pgmq.list_queues()", Connection);

        var queues = new List<PgmqQueueInfo>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            queues.Add(new PgmqQueueInfo
            {
                QueueName = reader.GetString(0),
                IsPartitioned = reader.GetBoolean(1),
                IsUnlogged = reader.GetBoolean(2),
                CreatedAt = reader.GetDateTime(3)
            });
        }
        return queues;
    }
}
