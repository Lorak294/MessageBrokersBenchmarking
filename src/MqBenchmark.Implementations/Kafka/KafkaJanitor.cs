using Confluent.Kafka;
using Confluent.Kafka.Admin;
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

        var topicName = kafkaConfig.TopicName;

        try
        {
            // Create topic if it doesn't exist
            await _adminClient.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = topicName,
                    NumPartitions = kafkaConfig.NumPartitions,
                    ReplicationFactor = 1
                }
            });
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // Topic exists — purge all records
            var metadata = _adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
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
