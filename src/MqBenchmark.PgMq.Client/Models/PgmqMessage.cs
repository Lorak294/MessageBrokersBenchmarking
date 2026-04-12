namespace MqBenchmark.PgMq.Client.Models;

/// <summary>
/// Represents a message read from a PGMQ queue.
/// Maps to the pgmq.message_record composite type.
/// </summary>
public record PgmqMessage
{
    public required long MsgId { get; init; }
    public required int ReadCount { get; init; }
    public required DateTime EnqueuedAt { get; init; }
    public DateTime? LastReadAt { get; init; }
    public required DateTime Vt { get; init; }

    /// <summary>
    /// The raw message payload, decoded from the base64-encoded JSONB value stored in the queue.
    /// </summary>
    public required byte[] Payload { get; init; }

    /// <summary>
    /// Optional JSONB headers associated with the message. Null if no headers were set.
    /// </summary>
    public string? Headers { get; init; }
}
