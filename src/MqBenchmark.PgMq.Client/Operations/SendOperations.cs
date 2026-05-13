using MqBenchmark.PgMq.Client.Models;
using Npgsql;
using NpgsqlTypes;

namespace MqBenchmark.PgMq.Client.Operations;

/// <summary>
/// Send operations: send single messages and batches, with optional delay.
/// All commands are lazy-prepared on first use (hot path).
/// </summary>
public sealed class SendOperations : PgmqOperationsBase, ISendOperations
{
    private readonly CommandSlot _sendSlot = Slot();
    private readonly CommandSlot _sendDelaySlot = Slot();
    private readonly CommandSlot _sendBatchSlot = Slot();
    private readonly CommandSlot _sendBatchDelaySlot = Slot();

    public SendOperations(NpgsqlConnection connection) : base(connection) { }

    /// <summary>
    /// Sends a single message. Returns the assigned message ID.
    /// </summary>
    public async Task<long> SendAsync(string queueName, byte[] payload, CancellationToken ct = default)
    {
        var cmd = await GetOrPrepareAsync(_sendSlot,
            "SELECT * FROM pgmq.send($1, to_jsonb($2::text))",
            c =>
            {
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text }); // queue_name
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text }); // base64 payload
            }, ct);

        cmd.Parameters[0].Value = queueName;
        cmd.Parameters[1].Value = EncodePayload(payload);
        var result = await cmd.ExecuteScalarAsync(ct);
        return (long)result!;
    }

    /// <summary>
    /// Sends a single message with an integer delay (seconds). Returns the assigned message ID.
    /// </summary>
    public async Task<long> SendAsync(string queueName, byte[] payload, int delaySeconds, CancellationToken ct = default)
    {
        if (delaySeconds <= 0)
            return await SendAsync(queueName, payload, ct);

        var cmd = await GetOrPrepareAsync(_sendDelaySlot,
            "SELECT * FROM pgmq.send($1, to_jsonb($2::text), $3)",
            c =>
            {
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });    // queue_name
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });    // base64 payload
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer }); // delay seconds
            }, ct);

        cmd.Parameters[0].Value = queueName;
        cmd.Parameters[1].Value = EncodePayload(payload);
        cmd.Parameters[2].Value = delaySeconds;
        var result = await cmd.ExecuteScalarAsync(ct);
        return (long)result!;
    }

    /// <summary>
    /// Sends a batch of messages. Returns an array of assigned message IDs.
    /// </summary>
    public async Task<long[]> SendBatchAsync(string queueName, IReadOnlyList<byte[]> payloads, CancellationToken ct = default)
    {
        var cmd = await GetOrPrepareAsync(_sendBatchSlot,
            "SELECT * FROM pgmq.send_batch($1, $2::jsonb[])",
            c =>
            {
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });                       // queue_name
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });  // jsonb[] as text[]
            }, ct);

        cmd.Parameters[0].Value = queueName;
        cmd.Parameters[1].Value = EncodePayloads(payloads);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await ReadIds(reader, ct);
    }

    /// <summary>
    /// Sends a batch of messages with an integer delay (seconds). Returns an array of assigned message IDs.
    /// </summary>
    public async Task<long[]> SendBatchAsync(string queueName, IReadOnlyList<byte[]> payloads, int delaySeconds, CancellationToken ct = default)
    {
        if (delaySeconds <= 0)
            return await SendBatchAsync(queueName, payloads, ct);

        var cmd = await GetOrPrepareAsync(_sendBatchDelaySlot,
            "SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3)",
            c =>
            {
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });                       // queue_name
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });  // jsonb[] as text[]
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer });                    // delay seconds
            }, ct);

        cmd.Parameters[0].Value = queueName;
        cmd.Parameters[1].Value = EncodePayloads(payloads);
        cmd.Parameters[2].Value = delaySeconds;

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await ReadIds(reader, ct);
    }

    private static string[] EncodePayloads(IReadOnlyList<byte[]> payloads)
    {
        var jsonArray = new string[payloads.Count];
        for (int i = 0; i < payloads.Count; i++)
        {
            jsonArray[i] = "\"" + Convert.ToBase64String(payloads[i]) + "\"";
        }
        return jsonArray;
    }
}
