using MqBenchmark.PgMq.Client.Models;
using Npgsql;
using NpgsqlTypes;

namespace MqBenchmark.PgMq.Client.Operations;

/// <summary>
/// Topic routing operations: bind/unbind queues to topic patterns, send via routing keys,
/// test routing, and list bindings. Send operations are lazy-prepared (hot-path for fan-out).
/// Management operations use ad-hoc commands.
/// </summary>
public sealed class TopicOperations : PgmqOperationsBase
{
    private readonly CommandSlot _sendTopicSlot = Slot();
    private readonly CommandSlot _sendTopicDelaySlot = Slot();
    private readonly CommandSlot _sendBatchTopicSlot = Slot();
    private readonly CommandSlot _sendBatchTopicDelaySlot = Slot();

    public TopicOperations(NpgsqlConnection connection) : base(connection) { }

    #region Management (ad-hoc)

    /// <summary>
    /// Binds a queue to a topic pattern. Messages sent with matching routing keys
    /// will be delivered to this queue.
    /// </summary>
    public async Task BindAsync(string pattern, string queueName, CancellationToken ct = default)
    {
        await using var cmd = new NpgsqlCommand("SELECT pgmq.bind_topic($1, $2)", Connection);
        cmd.Parameters.Add(new NpgsqlParameter { Value = pattern });
        cmd.Parameters.Add(new NpgsqlParameter { Value = queueName });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Unbinds a queue from a topic pattern.
    /// </summary>
    public async Task<bool> UnbindAsync(string pattern, string queueName, CancellationToken ct = default)
    {
        await using var cmd = new NpgsqlCommand("SELECT pgmq.unbind_topic($1, $2)", Connection);
        cmd.Parameters.Add(new NpgsqlParameter { Value = pattern });
        cmd.Parameters.Add(new NpgsqlParameter { Value = queueName });
        var result = await cmd.ExecuteScalarAsync(ct);
        return result is true;
    }

    /// <summary>
    /// Tests which queues a routing key would be delivered to.
    /// Returns (pattern, queue_name, compiled_regex) tuples.
    /// </summary>
    public async Task<IReadOnlyList<(string Pattern, string QueueName, string CompiledRegex)>> TestRoutingAsync(
        string routingKey, CancellationToken ct = default)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT pattern, queue_name, compiled_regex FROM pgmq.test_routing($1)", Connection);
        cmd.Parameters.Add(new NpgsqlParameter { Value = routingKey });

        var results = new List<(string, string, string)>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            results.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
        }
        return results;
    }

    /// <summary>
    /// Lists all topic bindings, optionally filtered by queue name.
    /// </summary>
    public async Task<IReadOnlyList<PgmqTopicBinding>> ListBindingsAsync(
        string? queueName = null, CancellationToken ct = default)
    {
        NpgsqlCommand cmd;
        if (queueName is not null)
        {
            cmd = new NpgsqlCommand(
                "SELECT pattern, queue_name, compiled_regex FROM pgmq.list_topic_bindings($1)", Connection);
            cmd.Parameters.Add(new NpgsqlParameter { Value = queueName });
        }
        else
        {
            cmd = new NpgsqlCommand(
                "SELECT pattern, queue_name, compiled_regex FROM pgmq.list_topic_bindings()", Connection);
        }

        await using (cmd)
        {
            var bindings = new List<PgmqTopicBinding>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                bindings.Add(new PgmqTopicBinding
                {
                    TopicName = reader.GetString(0),   // pattern
                    QueueName = reader.GetString(1),
                    RoutingKeyPattern = reader.GetString(2) // compiled_regex
                });
            }
            return bindings;
        }
    }

    #endregion

    #region Send via topic (lazy-prepared, hot path)

    /// <summary>
    /// Sends a single message to all queues matching the routing key.
    /// Returns the number of queues the message was delivered to.
    /// </summary>
    public async Task<int> SendAsync(string routingKey, byte[] payload, CancellationToken ct = default)
    {
        var cmd = await GetOrPrepareAsync(_sendTopicSlot,
            "SELECT pgmq.send_topic($1, to_jsonb($2::text))",
            c =>
            {
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text }); // routing_key
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text }); // base64 payload
            }, ct);

        cmd.Parameters[0].Value = routingKey;
        cmd.Parameters[1].Value = EncodePayload(payload);
        var result = await cmd.ExecuteScalarAsync(ct);
        return (int)result!;
    }

    /// <summary>
    /// Sends a single message with delay to all queues matching the routing key.
    /// Returns the number of queues the message was delivered to.
    /// </summary>
    public async Task<int> SendAsync(string routingKey, byte[] payload, int delaySeconds, CancellationToken ct = default)
    {
        if (delaySeconds <= 0)
            return await SendAsync(routingKey, payload, ct);

        var cmd = await GetOrPrepareAsync(_sendTopicDelaySlot,
            "SELECT pgmq.send_topic($1, to_jsonb($2::text), $3)",
            c =>
            {
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });    // routing_key
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });    // base64 payload
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer }); // delay seconds
            }, ct);

        cmd.Parameters[0].Value = routingKey;
        cmd.Parameters[1].Value = EncodePayload(payload);
        cmd.Parameters[2].Value = delaySeconds;
        var result = await cmd.ExecuteScalarAsync(ct);
        return (int)result!;
    }

    /// <summary>
    /// Sends a batch of messages to all queues matching the routing key.
    /// Returns (queue_name, msg_id) pairs for each delivery.
    /// </summary>
    public async Task<IReadOnlyList<(string QueueName, long MsgId)>> SendBatchAsync(
        string routingKey, IReadOnlyList<byte[]> payloads, CancellationToken ct = default)
    {
        var cmd = await GetOrPrepareAsync(_sendBatchTopicSlot,
            "SELECT queue_name, msg_id FROM pgmq.send_batch_topic($1, $2::jsonb[])",
            c =>
            {
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });                       // routing_key
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });  // jsonb[] as text[]
            }, ct);

        cmd.Parameters[0].Value = routingKey;
        cmd.Parameters[1].Value = EncodePayloads(payloads);

        return await ReadTopicBatchResults(cmd, ct);
    }

    /// <summary>
    /// Sends a batch of messages with delay to all queues matching the routing key.
    /// Returns (queue_name, msg_id) pairs for each delivery.
    /// </summary>
    public async Task<IReadOnlyList<(string QueueName, long MsgId)>> SendBatchAsync(
        string routingKey, IReadOnlyList<byte[]> payloads, int delaySeconds, CancellationToken ct = default)
    {
        if (delaySeconds <= 0)
            return await SendBatchAsync(routingKey, payloads, ct);

        var cmd = await GetOrPrepareAsync(_sendBatchTopicDelaySlot,
            "SELECT queue_name, msg_id FROM pgmq.send_batch_topic($1, $2::jsonb[], $3)",
            c =>
            {
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });                       // routing_key
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });  // jsonb[] as text[]
                c.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer });                    // delay seconds
            }, ct);

        cmd.Parameters[0].Value = routingKey;
        cmd.Parameters[1].Value = EncodePayloads(payloads);
        cmd.Parameters[2].Value = delaySeconds;

        return await ReadTopicBatchResults(cmd, ct);
    }

    #endregion

    #region Private helpers

    private static string[] EncodePayloads(IReadOnlyList<byte[]> payloads)
    {
        var jsonArray = new string[payloads.Count];
        for (int i = 0; i < payloads.Count; i++)
        {
            jsonArray[i] = "\"" + Convert.ToBase64String(payloads[i]) + "\"";
        }
        return jsonArray;
    }

    private static async Task<IReadOnlyList<(string QueueName, long MsgId)>> ReadTopicBatchResults(
        NpgsqlCommand cmd, CancellationToken ct)
    {
        var results = new List<(string, long)>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            results.Add((reader.GetString(0), reader.GetInt64(1)));
        }
        return results;
    }

    #endregion
}
