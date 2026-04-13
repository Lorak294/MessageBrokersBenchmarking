using Npgsql;
using NpgsqlTypes;

namespace MqBenchmark.PgMq.Client.Operations;

/// <summary>
/// Delete operations: delete single messages and batches by ID.
/// Batch delete returns the IDs of successfully deleted messages (long[]).
/// </summary>
public sealed class DeleteOperations : PgmqOperationsBase, IDeleteOperations
{
    private readonly CommandSlot _deleteSlot = Slot();
    private readonly CommandSlot _deleteBatchSlot = Slot();

    public DeleteOperations(NpgsqlConnection connection) : base(connection) { }

    /// <summary>
    /// Deletes a single message by ID. Returns true if the message was deleted.
    /// </summary>
    public async Task<bool> DeleteAsync(string queueName, long msgId, CancellationToken ct = default)
    {
        var cmd = await GetOrPrepareAsync(_deleteSlot,
            "SELECT pgmq.delete($1, $2)",
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
    /// Deletes a batch of messages by IDs. Returns the IDs of successfully deleted messages.
    /// </summary>
    public async Task<long[]> DeleteBatchAsync(string queueName, long[] msgIds, CancellationToken ct = default)
    {
        var cmd = await GetOrPrepareAsync(_deleteBatchSlot,
            "SELECT * FROM pgmq.delete($1, $2)",
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
