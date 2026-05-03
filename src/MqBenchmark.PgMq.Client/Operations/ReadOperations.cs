using MqBenchmark.PgMq.Client.Models;
using Npgsql;
using NpgsqlTypes;

namespace MqBenchmark.PgMq.Client.Operations;

/// <summary>
/// Read operations: read single/batch messages, and server-side long-polling via read_with_poll.
/// All commands are lazy-prepared on first use (hot path).
/// </summary>
public sealed class ReadOperations : PgmqOperationsBase
{
    private readonly CommandSlot _readSlot = Slot();
    private readonly CommandSlot _readWithPollSlot = Slot();

    public ReadOperations(NpgsqlConnection connection) : base(connection) { }

    /// <summary>
    /// Reads up to <paramref name="qty"/> messages from a queue, making them invisible
    /// for the specified visibility timeout (seconds).
    /// Returns an empty list if no messages are available.
    /// </summary>
    public async Task<IReadOnlyList<PgmqMessage>> ReadAsync(
        string queueName, int visibilityTimeout, int qty = 1, CancellationToken ct = default)
    {
        var cmd = await GetOrPrepareAsync(_readSlot,
            "SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message FROM pgmq.read($1, $2, $3)",
            c =>
            {
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });    // queue_name
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer }); // vt
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer }); // qty
            }, ct);

        cmd.Parameters[0].Value = queueName;
        cmd.Parameters[1].Value = visibilityTimeout;
        cmd.Parameters[2].Value = qty;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await ReadMessages(reader, ct);
    }

    /// <summary>
    /// Server-side long-polling read. Blocks in PostgreSQL until messages are available
    /// or <paramref name="maxPollSeconds"/> elapses. More efficient than client-side polling.
    /// CancellationToken cancels via Npgsql's cancel mechanism.
    /// </summary>
    public async Task<IReadOnlyList<PgmqMessage>> ReadWithPollAsync(
        string queueName,
        int visibilityTimeout,
        int qty = 1,
        int maxPollSeconds = 5,
        int pollIntervalMs = 100,
        CancellationToken ct = default)
    {
        var cmd = await GetOrPrepareAsync(_readWithPollSlot,
            "SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message FROM pgmq.read_with_poll($1, $2, $3, $4, $5)",
            c =>
            {
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });    // queue_name
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer }); // vt
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer }); // qty
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer }); // max_poll_seconds
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer }); // poll_interval_ms
            }, ct);

        cmd.Parameters[0].Value = queueName;
        cmd.Parameters[1].Value = visibilityTimeout;
        cmd.Parameters[2].Value = qty;
        cmd.Parameters[3].Value = maxPollSeconds;
        cmd.Parameters[4].Value = pollIntervalMs;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await ReadMessages(reader, ct);
    }

    /// <summary>
    /// Reads messages with msg_id greater than the given offset directly from the queue table.
    /// Does NOT modify visibility timeout or delete/archive messages.
    /// Used for streaming/replay scenarios where multiple consumer groups independently track offsets.
    /// Note: Cannot use prepared statements because the table name is dynamic.
    /// </summary>
    public async Task<IReadOnlyList<PgmqMessage>> ReadFromOffsetAsync(
        string queueName, long afterMsgId, int qty = 1, CancellationToken ct = default)
    {
        var sql = $"SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message FROM pgmq.q_{queueName} WHERE msg_id > $1 ORDER BY msg_id LIMIT $2";
        await using var cmd = new NpgsqlCommand(sql, Connection);
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = afterMsgId });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = qty });

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await ReadMessages(reader, ct);
    }
}
