using Npgsql;
using NpgsqlTypes;

namespace MqBenchmark.PgMq.Client.Operations;

public class ErrorCodes
{
    public const string TriggerAlreadyExistsSqlState = "42710";
}


/// <summary>
/// LISTEN/NOTIFY operations: enable/disable insert notifications on queues,
/// and create PgmqNotifyListener instances for event-driven consumption.
/// Management commands use ad-hoc (non-prepared) SQL.
/// </summary>
public sealed class NotifyOperations : PgmqOperationsBase
{
    private readonly string _connectionString;

    public NotifyOperations(NpgsqlConnection connection, string connectionString) : base(connection)
    {
        _connectionString = connectionString;
    }

    /// <summary>
    /// Enables LISTEN/NOTIFY insert notifications on a queue.
    /// The throttle interval prevents notification storms during bulk inserts.
    /// </summary>
    public async Task EnableAsync(string queueName, int throttleIntervalMs = 250, CancellationToken ct = default)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT pgmq.enable_notify_insert($1, $2)", Connection);
        cmd.Parameters.Add(new NpgsqlParameter { Value = queueName });
        cmd.Parameters.Add(new NpgsqlParameter<int> { TypedValue = throttleIntervalMs });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Disables LISTEN/NOTIFY insert notifications on a queue.
    /// </summary>
    public async Task DisableAsync(string queueName, CancellationToken ct = default)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT pgmq.disable_notify_insert($1)", Connection);
        cmd.Parameters.Add(new NpgsqlParameter { Value = queueName });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Creates a new PgmqNotifyListener with its own dedicated connection
    /// for LISTEN/WaitAsync. The caller is responsible for disposing the listener.
    /// </summary>
    public PgmqNotifyListener CreateListener(string queueName)
    {
        return new PgmqNotifyListener(_connectionString, queueName);
    }
}
