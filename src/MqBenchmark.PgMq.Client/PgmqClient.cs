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
public sealed class PgmqClient : IAsyncDisposable
{
    private readonly string _connectionString;
    private NpgsqlConnection? _connection;

    // Feature-specific operations groups — initialized in OpenAsync
    public SendOperations Send { get; private set; } = null!;
    public ReadOperations Read { get; private set; } = null!;
    public PopOperations Pop { get; private set; } = null!;
    public DeleteOperations Delete { get; private set; } = null!;
    public ArchiveOperations Archive { get; private set; } = null!;
    public QueueOperations Queues { get; private set; } = null!;
    public TopicOperations Topics { get; private set; } = null!;
    public NotifyOperations Notify { get; private set; } = null!;
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
        if (Send is not null) await Send.DisposeAsync();
        if (Read is not null) await Read.DisposeAsync();
        if (Pop is not null) await Pop.DisposeAsync();
        if (Delete is not null) await Delete.DisposeAsync();
        if (Archive is not null) await Archive.DisposeAsync();
        if (Queues is not null) await Queues.DisposeAsync();
        if (Topics is not null) await Topics.DisposeAsync();
        if (Notify is not null) await Notify.DisposeAsync();
        await Metrics.DisposeAsync();

        // Close the shared connection
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }
    }
}
