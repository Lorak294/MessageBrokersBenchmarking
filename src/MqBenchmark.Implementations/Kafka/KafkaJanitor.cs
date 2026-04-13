using Confluent.Kafka;
using Confluent.Kafka.Admin;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;

namespace MqBenchmark.Implementations.Kafka;

public class KafkaJanitor : IMqJanitor
{
    private IAdminClient? _adminClient;

    public async Task PrepareInfrastructureAsync(JanitorConfig config)
    {
        var kafkaConfig = config.MqConfig.ToKafkaConfig();

        _adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = kafkaConfig.BootstrapServers })
            .Build();

        switch (config.CommunicationMode)
        {
            case CommunicationMode.PointToPoint:
                // One topic per consumer group
                for (int i = 0; i < config.ConsumerGroups.Length; i++)
                {
                    var topicName = KafkaNaming.GroupTopic($"group_{i}");
                    await EnsureTopicClean(topicName, config.ConsumerGroups[i]);
                }
                break;

            case CommunicationMode.PubSub:
            case CommunicationMode.Streaming:
                // Single shared topic
                var maxConsumerGroupCont = config.ConsumerGroups.Max();
                await EnsureTopicClean(KafkaNaming.SharedTopic(), maxConsumerGroupCont);
                break;
        }
    }

    private async Task EnsureTopicClean(string topicName, int numPartitions)
    {
        try
        {
            await _adminClient!.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = topicName,
                    NumPartitions = numPartitions,
                    ReplicationFactor = 1
                }
            });
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // Topic exists — purge all records
            var metadata = _adminClient!.GetMetadata(topicName, TimeSpan.FromSeconds(10));
            var topic = metadata.Topics.First(t => t.Topic == topicName);

            var recordsToDelete = topic.Partitions.Select(p =>
                new TopicPartitionOffset(topicName, p.PartitionId, Offset.End)).ToList();

            await _adminClient.DeleteRecordsAsync(recordsToDelete);
        }
    }

    public void Dispose()
    {
        _adminClient?.Dispose();
    }
}
