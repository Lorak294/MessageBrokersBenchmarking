using Npgsql;
using NpgsqlTypes;

namespace MqBenchmark.PgMq.Client.Operations;

/// <summary>
/// Archive operations: move messages from the queue table to the archive table.
/// Batch archive returns the IDs of successfully archived messages (long[]).
/// </summary>
public sealed class ArchiveOperations : PgmqOperationsBase
{
    private readonly CommandSlot _archiveSlot = Slot();
    private readonly CommandSlot _archiveBatchSlot = Slot();

    public ArchiveOperations(NpgsqlConnection connection) : base(connection) { }

    /// <summary>
    /// Archives a single message by ID. Returns true if the message was archived.
    /// </summary>
    public async Task<bool> ArchiveAsync(string queueName, long msgId, CancellationToken ct = default)
    {
        var cmd = await GetOrPrepareAsync(_archiveSlot,
            "SELECT pgmq.archive($1, $2)",
            c =>
            {
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });   // queue_name
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint }); // msg_id
            }, ct);

        cmd.Parameters[0].Value = queueName;
        cmd.Parameters[1].Value = msgId;
        var result = await cmd.ExecuteScalarAsync(ct);
        return result is true;
    }

    /// <summary>
    /// Archives a batch of messages by IDs. Returns the IDs of successfully archived messages.
    /// </summary>
    public async Task<long[]> ArchiveBatchAsync(string queueName, long[] msgIds, CancellationToken ct = default)
    {
        var cmd = await GetOrPrepareAsync(_archiveBatchSlot,
            "SELECT * FROM pgmq.archive($1, $2)",
            c =>
            {
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });                        // queue_name
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint }); // msg_ids[]
            }, ct);

        cmd.Parameters[0].Value = queueName;
        cmd.Parameters[1].Value = msgIds;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await ReadIds(reader, ct);
    }
}
