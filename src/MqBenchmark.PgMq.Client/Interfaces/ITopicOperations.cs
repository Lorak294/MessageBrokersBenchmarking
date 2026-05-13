using MqBenchmark.PgMq.Client.Models;

namespace MqBenchmark.PgMq.Client;

public interface ITopicOperations
{
    Task BindAsync(string pattern, string queueName, CancellationToken ct = default);
    Task<bool> UnbindAsync(string pattern, string queueName, CancellationToken ct = default);
    Task<IReadOnlyList<(string Pattern, string QueueName, string CompiledRegex)>> TestRoutingAsync(string routingKey, CancellationToken ct = default);
    Task<IReadOnlyList<PgmqTopicBinding>> ListBindingsAsync(string? queueName = null, CancellationToken ct = default);
    Task<int> SendAsync(string routingKey, byte[] payload, CancellationToken ct = default);
    Task<int> SendAsync(string routingKey, byte[] payload, int delaySeconds, CancellationToken ct = default);
    Task<IReadOnlyList<(string QueueName, long MsgId)>> SendBatchAsync(string routingKey, IReadOnlyList<byte[]> payloads, CancellationToken ct = default);
    Task<IReadOnlyList<(string QueueName, long MsgId)>> SendBatchAsync(string routingKey, IReadOnlyList<byte[]> payloads, int delaySeconds, CancellationToken ct = default);
}
