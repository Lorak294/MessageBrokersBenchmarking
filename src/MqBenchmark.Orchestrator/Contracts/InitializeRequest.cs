using MqBenchmark.Core.Config;

namespace MqBenchmark.Orchestrator.Contracts;

public record InitializeRequest
{
    public int ProducersCount { get; set; }
    /// <summary>
    /// Array where each element represents the number of consumers in that group.
    /// E.g. [2, 3] means group 0 has 2 consumers, group 1 has 3 consumers.
    /// </summary>
    public required int[] ConsumerGroups { get; set; }
    public CommunicationMode CommunicationMode { get; set; } = CommunicationMode.PointToPoint;
    
    /// <summary>
    /// Total messages to produce. Used for PubSub and Streaming modes.
    /// For PointToPoint, derived from MessagesPerConsumerGroup if that is set, if not set, divided equally across groups.
    /// </summary>
    public int? MessageCount { get; set; }
    
    /// <summary>
    /// Per-group message counts for PointToPoint mode.
    /// If null in PointToPoint, MessageCount is divided equally across groups.
    /// Ignored for PubSub/Streaming.
    /// </summary>
    public int[]? MessagesPerConsumerGroup { get; set; }
    
    public int MessageSizeInBytes { get; set; }
    public int? SendFrequencyMps { get; set; }
    public required MqConfig MqConfig { get; set; }

    public int TotalConsumersCount => ConsumerGroups.Sum();
    
    /// <summary>
    /// Computes the effective total message count based on mode and configuration.
    /// </summary>
    public int GetTotalMessageCount()
    {
        if (CommunicationMode == CommunicationMode.PointToPoint)
        {
            if (MessagesPerConsumerGroup != null)
                return MessagesPerConsumerGroup.Sum();
            return MessageCount ?? 0;
        }
        return MessageCount ?? 0;
    }
    
    /// <summary>
    /// Gets the effective per-group message distribution for PointToPoint mode.
    /// </summary>
    public int[] GetMessagesPerGroup()
    {
        if (MessagesPerConsumerGroup != null)
            return MessagesPerConsumerGroup;
        
        // Equal distribution fallback
        var totalMessages = MessageCount ?? 0;
        var groupCount = ConsumerGroups.Length;
        var perGroup = totalMessages / groupCount;
        var remainder = totalMessages % groupCount;
        
        var result = new int[groupCount];
        for (int i = 0; i < groupCount; i++)
        {
            result[i] = perGroup + (i < remainder ? 1 : 0);
        }
        return result;
    }
}
