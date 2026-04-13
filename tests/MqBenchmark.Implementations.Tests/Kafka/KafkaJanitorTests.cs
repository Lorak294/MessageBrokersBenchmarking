using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using MqBenchmark.Implementations.Kafka;
using NSubstitute;
using Xunit;

namespace MqBenchmark.Implementations.Tests.Kafka;

public class KafkaJanitorTests
{
    private readonly IAdminClient _mockAdmin;
    private readonly KafkaJanitor _sut;

    public KafkaJanitorTests()
    {
        _mockAdmin = Substitute.For<IAdminClient>();
        _sut = new KafkaJanitor(_mockAdmin);
    }

    private static MqConfig CreateMqConfig() => new()
    {
        Implementation = "Kafka",
        AdditionalSettings = new Dictionary<string, string>
        {
            ["bootstrapServers"] = "localhost:9092"
        }
    };

    [Fact]
    public async Task PrepareInfrastructure_PointToPoint_CreatesTopicPerGroup()
    {
        // Arrange
        var config = new JanitorConfig
        {
            MqConfig = CreateMqConfig(),
            CommunicationMode = CommunicationMode.PointToPoint,
            ConsumerGroups = [3, 2]
        };

        // Act
        await _sut.PrepareInfrastructureAsync(config);

        // Assert
        await _mockAdmin.Received(2).CreateTopicsAsync(
            Arg.Any<IEnumerable<TopicSpecification>>(), Arg.Any<CreateTopicsOptions>());
    }

    [Fact]
    public async Task PrepareInfrastructure_PubSub_CreatesSharedTopic()
    {
        // Arrange
        var config = new JanitorConfig
        {
            MqConfig = CreateMqConfig(),
            CommunicationMode = CommunicationMode.PubSub,
            ConsumerGroups = [3, 2]
        };

        // Act
        await _sut.PrepareInfrastructureAsync(config);

        // Assert
        await _mockAdmin.Received(1).CreateTopicsAsync(
            Arg.Any<IEnumerable<TopicSpecification>>(), Arg.Any<CreateTopicsOptions>());
    }

    [Fact]
    public async Task PrepareInfrastructure_Streaming_CreatesSharedTopic()
    {
        // Arrange
        var config = new JanitorConfig
        {
            MqConfig = CreateMqConfig(),
            CommunicationMode = CommunicationMode.Streaming,
            ConsumerGroups = [4]
        };

        // Act
        await _sut.PrepareInfrastructureAsync(config);

        // Assert
        await _mockAdmin.Received(1).CreateTopicsAsync(
            Arg.Any<IEnumerable<TopicSpecification>>(), Arg.Any<CreateTopicsOptions>());
    }
}
