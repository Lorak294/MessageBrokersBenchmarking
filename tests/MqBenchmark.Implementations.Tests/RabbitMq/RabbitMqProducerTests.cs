using FluentAssertions;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using MqBenchmark.Implementations.RabbitMq;
using NSubstitute;
using RabbitMQ.Client;
using Xunit;

namespace MqBenchmark.Implementations.Tests.RabbitMq;

public class RabbitMqProducerTests
{
    private readonly IChannel _mockChannel;
    private readonly RabbitMqProducer _sut;

    public RabbitMqProducerTests()
    {
        _mockChannel = Substitute.For<IChannel>();
        _sut = new RabbitMqProducer(_mockChannel);
    }

    private static MqConfig CreateConfig(CommunicationMode mode, bool durable = false) => new()
    {
        Implementation = "RabbitMQ",
        CommunicationMode = mode,
        AdditionalSettings = new Dictionary<string, string>
        {
            ["hostname"] = "localhost",
            ["port"] = "5672",
            ["username"] = "guest",
            ["password"] = "guest",
            ["durableMode"] = durable.ToString().ToLower(),
            ["publisherConfirms"] = "false"
        }
    };

    [Fact]
    public async Task InitializeAsync_PointToPoint_DeclaresTopicExchange()
    {
        // Arrange & Act
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PointToPoint));

        // Assert
        await _mockChannel.Received(1).ExchangeDeclareAsync(
            RabbitMqNaming.TopicExchange(),
            ExchangeType.Topic,
            Arg.Any<bool>(),
            Arg.Any<bool>(),
            Arg.Any<IDictionary<string, object?>>(),
            Arg.Any<bool>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task InitializeAsync_PubSub_DeclaresFanoutExchange()
    {
        // Arrange & Act
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PubSub));

        // Assert
        await _mockChannel.Received(1).ExchangeDeclareAsync(
            RabbitMqNaming.FanoutExchange(),
            ExchangeType.Fanout,
            Arg.Any<bool>(),
            Arg.Any<bool>(),
            Arg.Any<IDictionary<string, object?>>(),
            Arg.Any<bool>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task InitializeAsync_Streaming_DeclaresStreamQueue()
    {
        // Arrange & Act
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.Streaming));

        // Assert
        await _mockChannel.Received(1).QueueDeclareAsync(
            RabbitMqNaming.StreamQueue(),
            Arg.Is(true), // durable
            Arg.Is(false), // exclusive
            Arg.Is(false), // autoDelete
            Arg.Is<IDictionary<string, object?>>(d => d.ContainsKey("x-queue-type")),
            Arg.Any<bool>(), Arg.Any<bool>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SendAsync_PointToPoint_PublishesWithRoutingKey()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PointToPoint));
        var message = Message.CreateMessage(32);

        // Act
        await _sut.SendAsync(message, "group_0");

        // Assert
        await _mockChannel.Received(1).BasicPublishAsync(
            RabbitMqNaming.TopicExchange(),
            "group_0",
            Arg.Any<bool>(),
            Arg.Any<BasicProperties>(),
            Arg.Any<ReadOnlyMemory<byte>>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SendAsync_PubSub_PublishesWithEmptyRoutingKey()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PubSub));
        var message = Message.CreateMessage(32);

        // Act
        await _sut.SendAsync(message);

        // Assert
        await _mockChannel.Received(1).BasicPublishAsync(
            RabbitMqNaming.FanoutExchange(),
            string.Empty,
            Arg.Any<bool>(),
            Arg.Any<BasicProperties>(),
            Arg.Any<ReadOnlyMemory<byte>>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SendAsync_Streaming_PublishesWithQueueNameAsRoutingKey()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.Streaming));
        var message = Message.CreateMessage(32);

        // Act
        await _sut.SendAsync(message);

        // Assert
        await _mockChannel.Received(1).BasicPublishAsync(
            string.Empty, // default exchange
            RabbitMqNaming.StreamQueue(),
            Arg.Any<bool>(),
            Arg.Any<BasicProperties>(),
            Arg.Any<ReadOnlyMemory<byte>>(),
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
    public async Task SendAsync_BeforeInitialize_Throws()
    {
        // Arrange
        var producer = new RabbitMqProducer();
        var message = Message.CreateMessage(32);

        // Act
        var act = () => producer.SendAsync(message);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*not initialized*");
    }
}
