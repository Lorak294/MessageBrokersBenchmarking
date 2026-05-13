using Npgsql;

namespace MqBenchmark.PgMq.Client;

/// <summary>
/// Owns a dedicated PostgreSQL connection for LISTEN/NOTIFY on a PGMQ queue's
/// insert notification channel. The channel name follows PGMQ's convention:
/// "pgmq.q_{queue_name}.INSERT"
///
/// Usage pattern:
///   1. await listener.StartAsync(ct)       — opens connection, issues LISTEN
///   2. await listener.WaitAsync(timeout)   — blocks until a notification arrives or timeout
///   3. listener.DisposeAsync()             — closes the dedicated connection
///
/// The consumer should use a separate PgmqClient connection for read/delete operations.
/// A periodic fallback sweep is recommended to catch messages missed between
/// throttled notifications.
/// </summary>
public sealed class PgmqNotifyListener : IPgmqNotifyListener
{
    private readonly string _connectionString;
    private readonly string _channelName;
    private NpgsqlConnection? _connection;
    private volatile bool _notified;

    public PgmqNotifyListener(string connectionString, string queueName)
    {
        _connectionString = connectionString;
        _channelName = $"pgmq.q_{queueName}.INSERT";
    }

    /// <summary>
    /// Opens the dedicated connection and issues LISTEN on the queue's notification channel.
    /// </summary>
    public async Task StartAsync(CancellationToken ct = default)
    {
        _connection = new NpgsqlConnection(_connectionString);
        await _connection.OpenAsync(ct);

        _connection.Notification += OnNotification;

        await using var cmd = new NpgsqlCommand($"LISTEN \"{_channelName}\"", _connection);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Waits for a notification or until the timeout elapses.
    /// Returns true if a notification was received, false on timeout.
    /// Uses Npgsql's WaitAsync which efficiently waits on the socket.
    /// </summary>
    public async Task<bool> WaitAsync(TimeSpan timeout, CancellationToken ct = default)
    {
        if (_connection is null)
            throw new InvalidOperationException("Listener not started. Call StartAsync() first.");

        _notified = false;

        // WaitAsync blocks until a notification arrives on this connection or the timeout elapses.
        // It returns true if a notification was received.
        try
        {
            await _connection.WaitAsync(timeout, ct);
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        catch (NpgsqlException)
        {
            // Connection issue — caller should handle reconnection
            return false;
        }

        return _notified;
    }

    /// <summary>
    /// Waits for a notification or until the cancellation token is triggered.
    /// Returns true if a notification was received, false if cancelled.
    /// </summary>
    public async Task<bool> WaitAsync(CancellationToken ct)
    {
        if (_connection is null)
            throw new InvalidOperationException("Listener not started. Call StartAsync() first.");

        _notified = false;

        try
        {
            await _connection.WaitAsync(ct);
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        catch (NpgsqlException)
        {
            return false;
        }

        return _notified;
    }

    private void OnNotification(object sender, NpgsqlNotificationEventArgs e)
    {
        if (string.Equals(e.Channel, _channelName, StringComparison.OrdinalIgnoreCase))
        {
            _notified = true;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            _connection.Notification -= OnNotification;

            try
            {
                await using var cmd = new NpgsqlCommand($"UNLISTEN \"{_channelName}\"", _connection);
                await cmd.ExecuteNonQueryAsync();
            }
            catch
            {
                // Best-effort UNLISTEN — connection may already be closed
            }

            await _connection.DisposeAsync();
            _connection = null;
        }
    }
}
