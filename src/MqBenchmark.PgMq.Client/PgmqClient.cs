using Npgsql;
using NpgsqlTypes;

namespace MqBenchmark.PgMq.Client;

/// <summary>
/// A performance-focused PGMQ client using a persistent Npgsql connection
/// with prepared statements for minimal per-operation overhead.
/// Each instance holds a single connection — create separate instances
/// for concurrent producer/consumer use.
/// </summary>
public class PgmqClient : IAsyncDisposable
{
    private readonly string _connectionString;
    private NpgsqlConnection? _connection;

    // Prepared commands — created once, reused with parameter rebinding
    private NpgsqlCommand? _sendCmd;
    private NpgsqlCommand? _sendBatchCmd;
    private NpgsqlCommand? _readCmd;
    private NpgsqlCommand? _readBatchCmd;
    private NpgsqlCommand? _popCmd;
    private NpgsqlCommand? _deleteCmd;
    private NpgsqlCommand? _deleteBatchCmd;
    private NpgsqlCommand? _archiveCmd;
    private NpgsqlCommand? _archiveBatchCmd;

    public PgmqClient(string connectionString)
    {
        _connectionString = connectionString;
    }

    /// <summary>
    /// Opens the persistent connection and prepares all reusable SQL commands.
    /// Must be called before any other operations.
    /// </summary>
    public async Task OpenAsync(CancellationToken ct = default)
    {
        _connection = new NpgsqlConnection(_connectionString);
        await _connection.OpenAsync(ct);

        // Ensure the pgmq extension is installed (idempotent).
        // Catch unique_violation (23505) in case another connection creates it concurrently.
        try
        {
            await using var ensureExt = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE", _connection);
            await ensureExt.ExecuteNonQueryAsync(ct);
        }
        catch (PostgresException ex) when (ex.SqlState == "23505")
        {
            // Extension was concurrently created by another connection — safe to continue
        }

        await PrepareCommandsAsync(ct);
    }

    public async ValueTask DisposeAsync()
    {
        _sendCmd?.Dispose();
        _sendBatchCmd?.Dispose();
        _readCmd?.Dispose();
        _readBatchCmd?.Dispose();
        _popCmd?.Dispose();
        _deleteCmd?.Dispose();
        _deleteBatchCmd?.Dispose();
        _archiveCmd?.Dispose();
        _archiveBatchCmd?.Dispose();

        if (_connection is not null)
        {
            await _connection.DisposeAsync();
        }
    }

    #region Queue Management

    /// <summary>
    /// Creates a PGMQ queue. Idempotent — does not error if the queue already exists.
    /// </summary>
    public async Task CreateQueueAsync(string queueName, bool unlogged = false, CancellationToken ct = default)
    {
        EnsureConnected();
        var fn = unlogged ? "pgmq.create_unlogged" : "pgmq.create";
        await using var cmd = new NpgsqlCommand($"SELECT {fn}($1)", _connection);
        cmd.Parameters.Add(new NpgsqlParameter { Value = queueName });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Drops a PGMQ queue and its associated tables.
    /// </summary>
    public async Task<bool> DropQueueAsync(string queueName, CancellationToken ct = default)
    {
        EnsureConnected();
        await using var cmd = new NpgsqlCommand("SELECT pgmq.drop_queue($1)", _connection);
        cmd.Parameters.Add(new NpgsqlParameter { Value = queueName });
        var result = await cmd.ExecuteScalarAsync(ct);
        return result is true;
    }

    /// <summary>
    /// Purges all messages from a queue using TRUNCATE.
    /// </summary>
    public async Task<long> PurgeQueueAsync(string queueName, CancellationToken ct = default)
    {
        EnsureConnected();
        await using var cmd = new NpgsqlCommand("SELECT pgmq.purge_queue($1)", _connection);
        cmd.Parameters.Add(new NpgsqlParameter { Value = queueName });
        var result = await cmd.ExecuteScalarAsync(ct);
        return result is long l ? l : 0;
    }

    #endregion

    #region Send

    /// <summary>
    /// Sends a single message to a queue. The payload is base64-encoded and stored as JSONB.
    /// Returns the message ID assigned by PGMQ.
    /// </summary>
    public async Task<long> SendAsync(string queueName, byte[] payload, CancellationToken ct = default)
    {
        EnsureConnected();
        var cmd = _sendCmd!;
        cmd.Parameters[0].Value = queueName;
        cmd.Parameters[1].Value = Convert.ToBase64String(payload);
        var result = await cmd.ExecuteScalarAsync(ct);
        return (long)result!;
    }

    /// <summary>
    /// Sends a batch of messages to a queue in a single SQL call.
    /// Returns an array of message IDs assigned by PGMQ.
    /// </summary>
    public async Task<long[]> SendBatchAsync(string queueName, IReadOnlyList<byte[]> payloads, CancellationToken ct = default)
    {
        EnsureConnected();
        var jsonArray = new string[payloads.Count];
        for (int i = 0; i < payloads.Count; i++)
        {
            jsonArray[i] = "\"" + Convert.ToBase64String(payloads[i]) + "\"";
        }

        var cmd = _sendBatchCmd!;
        cmd.Parameters[0].Value = queueName;
        cmd.Parameters[1].Value = jsonArray;

        var ids = new List<long>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            ids.Add(reader.GetInt64(0));
        }
        return ids.ToArray();
    }

    #endregion

    #region Read

    /// <summary>
    /// Reads a single message from a queue, making it invisible for the specified visibility timeout (seconds).
    /// Returns null if no messages are available.
    /// </summary>
    public async Task<PgmqMessage?> ReadAsync(string queueName, int visibilityTimeout, CancellationToken ct = default)
    {
        EnsureConnected();
        var cmd = _readCmd!;
        cmd.Parameters[0].Value = queueName;
        cmd.Parameters[1].Value = visibilityTimeout;
        
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return ReadMessage(reader);
        }
        return null;
    }

    /// <summary>
    /// Reads a batch of messages from a queue.
    /// </summary>
    public async Task<IReadOnlyList<PgmqMessage>> ReadBatchAsync(string queueName, int visibilityTimeout, int maxMessages, CancellationToken ct = default)
    {
        EnsureConnected();
        var cmd = _readBatchCmd!;
        cmd.Parameters[0].Value = queueName;
        cmd.Parameters[1].Value = visibilityTimeout;
        cmd.Parameters[2].Value = maxMessages;

        var messages = new List<PgmqMessage>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            messages.Add(ReadMessage(reader));
        }
        return messages;
    }

    #endregion

    #region Pop

    /// <summary>
    /// Atomically reads and deletes a single message from a queue (no visibility timeout).
    /// Returns null if no messages are available.
    /// </summary>
    public async Task<PgmqMessage?> PopAsync(string queueName, CancellationToken ct = default)
    {
        EnsureConnected();
        var cmd = _popCmd!;
        cmd.Parameters[0].Value = queueName;
        
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            return ReadMessage(reader);
        }
        return null;
    }

    #endregion

    #region Delete

    /// <summary>
    /// Deletes a single message from a queue by ID.
    /// </summary>
    public async Task<bool> DeleteAsync(string queueName, long msgId, CancellationToken ct = default)
    {
        EnsureConnected();
        var cmd = _deleteCmd!;
        cmd.Parameters[0].Value = queueName;
        cmd.Parameters[1].Value = msgId;
        var result = await cmd.ExecuteScalarAsync(ct);
        return result is true;
    }

    /// <summary>
    /// Deletes a batch of messages from a queue by IDs.
    /// Returns the number of messages successfully deleted.
    /// </summary>
    public async Task<int> DeleteBatchAsync(string queueName, long[] msgIds, CancellationToken ct = default)
    {
        EnsureConnected();
        var cmd = _deleteBatchCmd!;
        cmd.Parameters[0].Value = queueName;
        cmd.Parameters[1].Value = msgIds;

        int count = 0;
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            count++;
        }
        return count;
    }

    #endregion

    #region Archive

    /// <summary>
    /// Archives a single message (moves from queue table to archive table).
    /// </summary>
    public async Task<bool> ArchiveAsync(string queueName, long msgId, CancellationToken ct = default)
    {
        EnsureConnected();
        var cmd = _archiveCmd!;
        cmd.Parameters[0].Value = queueName;
        cmd.Parameters[1].Value = msgId;
        var result = await cmd.ExecuteScalarAsync(ct);
        return result is true;
    }

    /// <summary>
    /// Archives a batch of messages.
    /// Returns the number of messages successfully archived.
    /// </summary>
    public async Task<int> ArchiveBatchAsync(string queueName, long[] msgIds, CancellationToken ct = default)
    {
        EnsureConnected();
        var cmd = _archiveBatchCmd!;
        cmd.Parameters[0].Value = queueName;
        cmd.Parameters[1].Value = msgIds;

        int count = 0;
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            count++;
        }
        return count;
    }

    #endregion

    #region Private Helpers

    private void EnsureConnected()
    {
        if (_connection is null || _connection.State != System.Data.ConnectionState.Open)
        {
            throw new InvalidOperationException("PgmqClient is not connected. Call OpenAsync() first.");
        }
    }

    /// <summary>
    /// Reads a PgmqMessage from a data reader row.
    /// Expected columns: msg_id, read_ct, enqueued_at, vt, message, headers
    /// (headers is ignored — we only care about the payload).
    /// </summary>
    private static PgmqMessage ReadMessage(NpgsqlDataReader reader)
    {
        var msgId = reader.GetInt64(0);          // msg_id
        var readCount = reader.GetInt32(1);       // read_ct
        var enqueuedAt = reader.GetDateTime(2);   // enqueued_at
        // Column 3 is last_read_at for read(), but column order differs for pop()
        // We handle both: read() returns (msg_id, read_ct, enqueued_at, vt, message, headers)
        // pop() returns the same pgmq.message_record type
        var lastReadAt = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3);
        var vt = reader.GetDateTime(4);           // vt
        var messageJson = reader.GetString(5);    // message (JSONB)

        // The payload is stored as a base64-encoded JSON string: "\"<base64>\""
        // Strip the surrounding quotes if present, then decode
        var base64 = messageJson.Trim('"');
        var payload = Convert.FromBase64String(base64);

        return new PgmqMessage
        {
            MsgId = msgId,
            ReadCount = readCount,
            EnqueuedAt = enqueuedAt,
            LastReadAt = lastReadAt,
            Vt = vt,
            Payload = payload
        };
    }

    /// <summary>
    /// Prepares all reusable SQL commands on the persistent connection.
    /// Using positional parameters ($1, $2, ...) for maximum efficiency.
    /// </summary>
    private async Task PrepareCommandsAsync(CancellationToken ct)
    {
        // send: SELECT * FROM pgmq.send($1, to_jsonb($2::text))
        _sendCmd = new NpgsqlCommand("SELECT * FROM pgmq.send($1, to_jsonb($2::text))", _connection);
        _sendCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });    // $1: queue_name
        _sendCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });    // $2: base64 payload
        await _sendCmd.PrepareAsync(ct);

        // send_batch: SELECT * FROM pgmq.send_batch($1, $2::jsonb[])
        _sendBatchCmd = new NpgsqlCommand("SELECT * FROM pgmq.send_batch($1, $2::jsonb[])", _connection);
        _sendBatchCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });              // $1: queue_name
        _sendBatchCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text }); // $2: jsonb[] as text[]
        await _sendBatchCmd.PrepareAsync(ct);

        // read (single): SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message FROM pgmq.read($1, $2, 1)
        _readCmd = new NpgsqlCommand("SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message FROM pgmq.read($1, $2, 1)", _connection);
        _readCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });     // $1: queue_name
        _readCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer });  // $2: vt
        await _readCmd.PrepareAsync(ct);

        // read (batch): SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message FROM pgmq.read($1, $2, $3)
        _readBatchCmd = new NpgsqlCommand("SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message FROM pgmq.read($1, $2, $3)", _connection);
        _readBatchCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });     // $1: queue_name
        _readBatchCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer });  // $2: vt
        _readBatchCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer });  // $3: qty
        await _readBatchCmd.PrepareAsync(ct);

        // pop: SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message FROM pgmq.pop($1)
        _popCmd = new NpgsqlCommand("SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message FROM pgmq.pop($1)", _connection);
        _popCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });  // $1: queue_name
        await _popCmd.PrepareAsync(ct);

        // delete (single): SELECT pgmq.delete($1, $2)
        _deleteCmd = new NpgsqlCommand("SELECT pgmq.delete($1, $2)", _connection);
        _deleteCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });    // $1: queue_name
        _deleteCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint });  // $2: msg_id
        await _deleteCmd.PrepareAsync(ct);

        // delete (batch): SELECT * FROM pgmq.delete($1, $2)
        _deleteBatchCmd = new NpgsqlCommand("SELECT * FROM pgmq.delete($1, $2)", _connection);
        _deleteBatchCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });                  // $1: queue_name
        _deleteBatchCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint }); // $2: msg_ids[]
        await _deleteBatchCmd.PrepareAsync(ct);

        // archive (single): SELECT pgmq.archive($1, $2)
        _archiveCmd = new NpgsqlCommand("SELECT pgmq.archive($1, $2)", _connection);
        _archiveCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });    // $1: queue_name
        _archiveCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint });  // $2: msg_id
        await _archiveCmd.PrepareAsync(ct);

        // archive (batch): SELECT * FROM pgmq.archive($1, $2)
        _archiveBatchCmd = new NpgsqlCommand("SELECT * FROM pgmq.archive($1, $2)", _connection);
        _archiveBatchCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });                  // $1: queue_name
        _archiveBatchCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint }); // $2: msg_ids[]
        await _archiveBatchCmd.PrepareAsync(ct);
    }

    #endregion
}
