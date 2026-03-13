using MqBenchmark.PgMq.Client.Models;
using Npgsql;
using NpgsqlTypes;

namespace MqBenchmark.PgMq.Client.Operations;

/// <summary>
/// Pop operations: atomically read and delete messages (no visibility timeout).
/// Consolidated to single method with qty parameter. Lazy-prepared (hot path).
/// </summary>
public sealed class PopOperations : PgmqOperationsBase
{
    private readonly CommandSlot _popSlot = Slot();

    public PopOperations(NpgsqlConnection connection) : base(connection) { }

    /// <summary>
    /// Atomically reads and deletes up to <paramref name="qty"/> messages from a queue.
    /// Returns an empty list if no messages are available.
    /// </summary>
    public async Task<IReadOnlyList<PgmqMessage>> PopAsync(
        string queueName, int qty = 1, CancellationToken ct = default)
    {
        var cmd = await GetOrPrepareAsync(_popSlot,
            "SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message FROM pgmq.pop($1, $2)",
            c =>
            {
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });    // queue_name
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer }); // qty
            }, ct);

        cmd.Parameters[0].Value = queueName;
        cmd.Parameters[1].Value = qty;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await ReadMessages(reader, ct);
    }
}
