using MqBenchmark.PgMq.Client.Operations;
using Npgsql;

namespace MqBenchmark.PgMq.Client;

/// <summary>
/// A performance-focused PGMQ client using a persistent Npgsql connection
/// with composition-based feature groups. Each operations group lazily prepares
/// its SQL commands on first use for minimal per-operation overhead.
///
/// Each instance holds a single connection — create separate instances
/// for concurrent producer/consumer use.
///
/// Usage:
///   var client = new PgmqClient(connectionString);
///   await client.OpenAsync();
///   await client.Queues.CreateAsync("my_queue");
///   await client.Send.SendAsync("my_queue", payload);
///   var messages = await client.Read.ReadAsync("my_queue", vt: 30);
///   await client.DisposeAsync();
/// </summary>
public sealed class PgmqClient : IPgmqClient
{
    private readonly string _connectionString;
    private NpgsqlConnection? _connection;

    // Feature-specific operations groups — initialized in OpenAsync
    public ISendOperations Send { get; private set; } = null!;
    public IReadOperations Read { get; private set; } = null!;
    public IPopOperations Pop { get; private set; } = null!;
    public IDeleteOperations Delete { get; private set; } = null!;
    public IArchiveOperations Archive { get; private set; } = null!;
    public IQueueOperations Queues { get; private set; } = null!;
    public ITopicOperations Topics { get; private set; } = null!;
    public INotifyOperations Notify { get; private set; } = null!;
    public MetricsOperations Metrics { get; private set; } = null!;

    public PgmqClient(string connectionString)
    {
        _connectionString = connectionString;
    }

    /// <summary>
    /// Opens the persistent connection, ensures the PGMQ extension is installed,
    /// and initializes all operations groups.
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

        // Initialize all operations groups with the shared connection
        Send = new SendOperations(_connection);
        Read = new ReadOperations(_connection);
        Pop = new PopOperations(_connection);
        Delete = new DeleteOperations(_connection);
        Archive = new ArchiveOperations(_connection);
        Queues = new QueueOperations(_connection);
        Topics = new TopicOperations(_connection);
        Notify = new NotifyOperations(_connection, _connectionString);
        Metrics = new MetricsOperations(_connection);
    }

    public async ValueTask DisposeAsync()
    {
        // Dispose all operations groups (releases prepared commands)
        if (Send is IAsyncDisposable s) await s.DisposeAsync();
        if (Read is IAsyncDisposable r) await r.DisposeAsync();
        if (Pop is IAsyncDisposable p) await p.DisposeAsync();
        if (Delete is IAsyncDisposable d) await d.DisposeAsync();
        if (Archive is IAsyncDisposable a) await a.DisposeAsync();
        if (Queues is IAsyncDisposable q) await q.DisposeAsync();
        if (Topics is IAsyncDisposable t) await t.DisposeAsync();
        if (Notify is IAsyncDisposable n) await n.DisposeAsync();
        if (Metrics is not null) await Metrics.DisposeAsync();

        // Close the shared connection
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }
    }
}
