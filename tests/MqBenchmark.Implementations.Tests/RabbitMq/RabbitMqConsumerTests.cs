using FluentAssertions;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using MqBenchmark.Implementations.RabbitMq;
using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Xunit;

namespace MqBenchmark.Implementations.Tests.RabbitMq;

public class RabbitMqConsumerTests
{
    private readonly IChannel _mockChannel;
    private readonly RabbitMqConsumer _sut;

    public RabbitMqConsumerTests()
    {
        _mockChannel = Substitute.For<IChannel>();
        _mockChannel.BasicConsumeAsync(
            Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<string>(),
            Arg.Any<bool>(), Arg.Any<bool>(),
            Arg.Any<IDictionary<string, object?>>(),
            Arg.Any<IAsyncBasicConsumer>(), Arg.Any<CancellationToken>())
            .Returns("consumer-tag-1");
        _sut = new RabbitMqConsumer(_mockChannel);
    }

    private static MqConfig CreateConfig(CommunicationMode mode, string groupName = "group_0") => new()
    {
        Implementation = "RabbitMQ",
        CommunicationMode = mode,
        ConsumerGroupName = groupName,
        AdditionalSettings = new Dictionary<string, string>
        {
            ["hostname"] = "localhost",
            ["port"] = "5672",
            ["username"] = "guest",
            ["password"] = "guest",
            ["prefetchCount"] = "50"
        }
    };

    [Fact]
    public async Task InitializeAsync_PointToPoint_DeclaresGroupQueue()
    {
        // Arrange & Act
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PointToPoint, "group_1"));

        // Assert
        await _mockChannel.Received(1).QueueDeclareAsync(
            RabbitMqNaming.GroupQueue("group_1"),
            Arg.Any<bool>(), Arg.Is(false), Arg.Is(false),
            Arg.Any<IDictionary<string, object?>>(),
            Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
        await _mockChannel.Received(1).BasicQosAsync(
            0, 50, false, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task InitializeAsync_Streaming_DeclaresStreamQueue()
    {
        // Arrange & Act
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.Streaming));

        // Assert
        await _mockChannel.Received(1).QueueDeclareAsync(
            RabbitMqNaming.StreamQueue(),
            Arg.Is(true), Arg.Is(false), Arg.Is(false),
            Arg.Is<IDictionary<string, object?>>(d => d.ContainsKey("x-queue-type")),
            Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SubscribeAsync_Streaming_SetsStreamOffsetFirst()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.Streaming));

        // Act
        await _sut.SubscribeAsync(_ => Task.CompletedTask);

        // Assert
        await _mockChannel.Received(1).BasicConsumeAsync(
            Arg.Any<string>(), false, Arg.Any<string>(),
            false, false,
            Arg.Is<IDictionary<string, object?>>(d =>
                d.ContainsKey("x-stream-offset") && (string)d["x-stream-offset"]! == "first"),
            Arg.Any<IAsyncBasicConsumer>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SubscribeAsync_NonStreaming_NoStreamOffset()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PubSub));

        // Act
        await _sut.SubscribeAsync(_ => Task.CompletedTask);

        // Assert
        await _mockChannel.Received(1).BasicConsumeAsync(
            Arg.Any<string>(), false, Arg.Any<string>(),
            false, false,
            Arg.Is<IDictionary<string, object?>>(d => d.Count == 0),
            Arg.Any<IAsyncBasicConsumer>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SubscribeAsync_BeforeInitialize_Throws()
    {
        // Arrange
        var consumer = new RabbitMqConsumer();

        // Act
        var act = () => consumer.SubscribeAsync(_ => Task.CompletedTask);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>();
    }
}
