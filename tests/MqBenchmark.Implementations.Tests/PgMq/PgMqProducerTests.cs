using FluentAssertions;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using MqBenchmark.Implementations.PgMq;
using MqBenchmark.PgMq.Client;
using NSubstitute;
using Xunit;

namespace MqBenchmark.Implementations.Tests.PgMq;

public class PgMqProducerTests
{
    private readonly IPgmqClient _mockClient;
    private readonly ISendOperations _mockSend;
    private readonly ITopicOperations _mockTopics;
    private readonly IQueueOperations _mockQueues;
    private readonly PgMqProducer _sut;

    public PgMqProducerTests()
    {
        _mockClient = Substitute.For<IPgmqClient>();
        _mockSend = Substitute.For<ISendOperations>();
        _mockTopics = Substitute.For<ITopicOperations>();
        _mockQueues = Substitute.For<IQueueOperations>();
        _mockClient.Send.Returns(_mockSend);
        _mockClient.Topics.Returns(_mockTopics);
        _mockClient.Queues.Returns(_mockQueues);
        _sut = new PgMqProducer(_mockClient);
    }

    private static MqConfig CreateConfig(CommunicationMode mode, bool buffered = false) => new()
    {
        Implementation = "PgMq",
        CommunicationMode = mode,
        AdditionalSettings = new Dictionary<string, string>
        {
            ["connectionString"] = "Host=localhost;Database=test",
            ["useBufferedProducer"] = buffered.ToString().ToLower(),
            ["producerBatchSize"] = "3",
            ["producerLingerMs"] = "50"
        }
    };

    [Fact]
    public async Task SendAsync_PointToPoint_SendsToTopicRoutingKey()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PointToPoint));
        var message = Message.CreateMessage(32);

        // Act
        await _sut.SendAsync(message, "group_0");

        // Assert
        await _mockTopics.Received(1).SendAsync(
            PgMqNaming.GroupRoutingKey("group_0"),
            message.Payload,
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SendAsync_PubSub_SendsToBroadcastRoutingKey()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PubSub));
        var message = Message.CreateMessage(32);

        // Act
        await _sut.SendAsync(message);

        // Assert
        await _mockTopics.Received(1).SendAsync(
            PgMqNaming.BroadcastRoutingKey(),
            message.Payload,
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SendAsync_Streaming_SendsDirectToQueue()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.Streaming));
        var message = Message.CreateMessage(32);

        // Act
        await _sut.SendAsync(message);

        // Assert
        await _mockSend.Received(1).SendAsync(
            PgMqNaming.StreamQueue(),
            message.Payload,
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SendAsync_PointToPoint_NoTarget_Throws()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PointToPoint));
        var message = Message.CreateMessage(32);

        // Act
        var act = () => _sut.SendAsync(message);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*routing target*");
    }

    [Fact]
    public async Task SendAsync_Buffered_FlushesAtBatchSize()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PubSub, buffered: true));

        // Act - send 3 messages (batchSize = 3)
        for (int i = 0; i < 3; i++)
            await _sut.SendAsync(Message.CreateMessage(32));

        // Assert - batch should have been flushed via SendBatchAsync on topics
        await _mockTopics.Received(1).SendBatchAsync(
            PgMqNaming.BroadcastRoutingKey(),
            Arg.Is<IReadOnlyList<byte[]>>(list => list.Count == 3),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SendAsync_Buffered_FlushesOnTargetChange()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PointToPoint, buffered: true));

        // Act - send 2 to group_0, then 1 to group_1
        await _sut.SendAsync(Message.CreateMessage(32), "group_0");
        await _sut.SendAsync(Message.CreateMessage(32), "group_0");
        await _sut.SendAsync(Message.CreateMessage(32), "group_1");

        // Assert - first batch flushed due to target change (2 messages to group_0)
        await _mockTopics.Received(1).SendBatchAsync(
            PgMqNaming.GroupRoutingKey("group_0"),
            Arg.Is<IReadOnlyList<byte[]>>(list => list.Count == 2),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task DisposeAsync_Buffered_FlushesRemainingBuffer()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PubSub, buffered: true));
        await _sut.SendAsync(Message.CreateMessage(32));
        await _sut.SendAsync(Message.CreateMessage(32));

        // Act
        await _sut.DisposeAsync();

        // Assert
        await _mockTopics.Received(1).SendBatchAsync(
            PgMqNaming.BroadcastRoutingKey(),
            Arg.Is<IReadOnlyList<byte[]>>(list => list.Count == 2),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SendAsync_BeforeInitialize_Throws()
    {
        // Arrange
        var producer = new PgMqProducer();
        var message = Message.CreateMessage(32);

        // Act
        var act = () => producer.SendAsync(message);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*not initialized*");
    }
}
