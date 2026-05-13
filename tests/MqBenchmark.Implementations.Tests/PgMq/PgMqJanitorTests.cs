using FluentAssertions;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using MqBenchmark.Implementations.PgMq;
using MqBenchmark.PgMq.Client;
using NSubstitute;
using Xunit;

namespace MqBenchmark.Implementations.Tests.PgMq;

public class PgMqJanitorTests
{
    private readonly IPgmqClient _mockClient;
    private readonly IQueueOperations _mockQueues;
    private readonly ITopicOperations _mockTopics;
    private readonly PgMqJanitor _sut;

    public PgMqJanitorTests()
    {
        _mockClient = Substitute.For<IPgmqClient>();
        _mockQueues = Substitute.For<IQueueOperations>();
        _mockTopics = Substitute.For<ITopicOperations>();
        _mockClient.Queues.Returns(_mockQueues);
        _mockClient.Topics.Returns(_mockTopics);
        _sut = new PgMqJanitor(_mockClient);
    }

    private static MqConfig CreateMqConfig() => new()
    {
        Implementation = "PgMq",
        AdditionalSettings = new Dictionary<string, string>
        {
            ["connectionString"] = "Host=localhost;Database=test"
        }
    };

    [Fact]
    public async Task PrepareInfrastructure_PointToPoint_CreatesQueuesAndBindsTopics()
    {
        // Arrange
        var config = new JanitorConfig
        {
            MqConfig = CreateMqConfig(),
            CommunicationMode = CommunicationMode.PointToPoint,
            ConsumerGroups = [2, 3]
        };

        // Act
        await _sut.PrepareInfrastructureAsync(config);

        // Assert
        await _mockQueues.Received(2).CreateAsync(Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
        await _mockQueues.Received(2).PurgeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await _mockTopics.Received(1).BindAsync(
            PgMqNaming.GroupRoutingKey("group_0"),
            PgMqNaming.GroupQueue("group_0"),
            Arg.Any<CancellationToken>());
        await _mockTopics.Received(1).BindAsync(
            PgMqNaming.GroupRoutingKey("group_1"),
            PgMqNaming.GroupQueue("group_1"),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PrepareInfrastructure_PubSub_CreatesQueuesAndBindsBroadcast()
    {
        // Arrange
        var config = new JanitorConfig
        {
            MqConfig = CreateMqConfig(),
            CommunicationMode = CommunicationMode.PubSub,
            ConsumerGroups = [1, 1, 1]
        };

        // Act
        await _sut.PrepareInfrastructureAsync(config);

        // Assert
        await _mockQueues.Received(3).CreateAsync(Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
        await _mockQueues.Received(3).PurgeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await _mockTopics.Received(3).BindAsync(
            PgMqNaming.BroadcastRoutingKey(),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PrepareInfrastructure_Streaming_CreatesAndPurgesStreamQueue()
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
        await _mockQueues.Received(1).CreateAsync(
            PgMqNaming.StreamQueue(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
        await _mockQueues.Received(1).PurgeAsync(
            PgMqNaming.StreamQueue(), Arg.Any<CancellationToken>());
        await _mockTopics.DidNotReceive().BindAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }
}
