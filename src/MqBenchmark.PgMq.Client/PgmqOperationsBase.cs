using MqBenchmark.PgMq.Client.Models;
using Npgsql;

namespace MqBenchmark.PgMq.Client;

/// <summary>
/// Base class for all PGMQ operations groups. Provides shared access to
/// the persistent connection and lazy prepared-statement management.
/// </summary>
public abstract class PgmqOperationsBase : IAsyncDisposable
{
    protected readonly NpgsqlConnection Connection;
    private readonly List<NpgsqlCommand> _preparedCommands = new();

    protected PgmqOperationsBase(NpgsqlConnection connection)
    {
        Connection = connection;
    }

    /// <summary>
    /// Holds a lazily-prepared NpgsqlCommand. Passed by value to async methods
    /// (async methods cannot have ref parameters).
    /// </summary>
    protected sealed class CommandSlot
    {
        public volatile NpgsqlCommand? Command;
    }

    /// <summary>
    /// Creates a new empty command slot. Call from field initializers in subclasses.
    /// </summary>
    protected static CommandSlot Slot() => new();

    /// <summary>
    /// Lazily prepares a SQL command on first use. The command is tracked for disposal
    /// when the operations group is disposed.
    /// </summary>
    protected async Task<NpgsqlCommand> GetOrPrepareAsync(
        CommandSlot slot,
        string sql,
        Action<NpgsqlCommand> configureParams,
        CancellationToken ct)
    {
        if (slot.Command is not null)
            return slot.Command;

        var cmd = new NpgsqlCommand(sql, Connection);
        configureParams(cmd);
        await cmd.PrepareAsync(ct);

        // Simple assignment — we don't expect concurrent calls on the same operations instance.
        if (Interlocked.CompareExchange(ref slot.Command, cmd, null) is not null)
        {
            // Another call won the race — dispose the duplicate
            cmd.Dispose();
        }
        else
        {
            lock (_preparedCommands)
            {
                _preparedCommands.Add(cmd);
            }
        }

        return slot.Command;
    }

    /// <summary>
    /// Reads a single PgmqMessage from the current reader row.
    /// Expected column order: msg_id, read_ct, enqueued_at, last_read_at, vt, message
    /// </summary>
    protected static PgmqMessage ReadMessage(NpgsqlDataReader reader)
    {
        var msgId = reader.GetInt64(0);          // msg_id
        var readCount = reader.GetInt32(1);       // read_ct
        var enqueuedAt = reader.GetDateTime(2);   // enqueued_at
        var lastReadAt = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3); // last_read_at
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
    /// Reads multiple PgmqMessages from a reader until exhausted.
    /// </summary>
    protected static async Task<IReadOnlyList<PgmqMessage>> ReadMessages(NpgsqlDataReader reader, CancellationToken ct)
    {
        var messages = new List<PgmqMessage>();
        while (await reader.ReadAsync(ct))
        {
            messages.Add(ReadMessage(reader));
        }
        return messages;
    }

    /// <summary>
    /// Reads a column of long IDs from a reader until exhausted.
    /// Used by batch delete/archive which return affected IDs.
    /// </summary>
    protected static async Task<long[]> ReadIds(NpgsqlDataReader reader, CancellationToken ct)
    {
        var ids = new List<long>();
        while (await reader.ReadAsync(ct))
        {
            ids.Add(reader.GetInt64(0));
        }
        return ids.ToArray();
    }

    /// <summary>
    /// Encodes a byte[] payload as a base64 string for JSONB storage.
    /// </summary>
    protected static string EncodePayload(byte[] payload)
    {
        return Convert.ToBase64String(payload);
    }

    public ValueTask DisposeAsync()
    {
        lock (_preparedCommands)
        {
            foreach (var cmd in _preparedCommands)
            {
                cmd.Dispose();
            }
            _preparedCommands.Clear();
        }
        return ValueTask.CompletedTask;
    }
}
