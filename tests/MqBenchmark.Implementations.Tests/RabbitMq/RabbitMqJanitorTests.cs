using FluentAssertions;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using MqBenchmark.Implementations.RabbitMq;
using NSubstitute;
using RabbitMQ.Client;
using Xunit;

namespace MqBenchmark.Implementations.Tests.RabbitMq;

public class RabbitMqJanitorTests
{
    private readonly IChannel _mockChannel;
    private readonly RabbitMqJanitor _sut;

    public RabbitMqJanitorTests()
    {
        _mockChannel = Substitute.For<IChannel>();
        _sut = new RabbitMqJanitor(_mockChannel);
    }

    private static MqConfig CreateMqConfig(bool durable = false) => new()
    {
        Implementation = "RabbitMQ",
        AdditionalSettings = new Dictionary<string, string>
        {
            ["hostname"] = "localhost",
            ["port"] = "5672",
            ["username"] = "guest",
            ["password"] = "guest",
            ["durableMode"] = durable.ToString().ToLower()
        }
    };

    [Fact]
    public async Task PrepareInfrastructure_PointToPoint_CreatesTopicExchangeAndBindsQueues()
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
        await _mockChannel.Received(1).ExchangeDeclareAsync(
            RabbitMqNaming.TopicExchange(),
            ExchangeType.Topic,
            Arg.Any<bool>(), Arg.Any<bool>(),
            Arg.Any<IDictionary<string, object?>>(),
            Arg.Any<bool>(), Arg.Any<CancellationToken>());

        // 2 groups -> 2 queue declares, 2 binds, 2 purges
        await _mockChannel.Received(2).QueueDeclareAsync(
            Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<bool>(),
            Arg.Any<IDictionary<string, object?>>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
        await _mockChannel.Received(2).QueueBindAsync(
            Arg.Any<string>(), RabbitMqNaming.TopicExchange(), Arg.Any<string>(),
            Arg.Any<IDictionary<string, object?>>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
        await _mockChannel.Received(2).QueuePurgeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PrepareInfrastructure_PubSub_CreatesFanoutExchangeAndBindsQueues()
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
        await _mockChannel.Received(1).ExchangeDeclareAsync(
            RabbitMqNaming.FanoutExchange(),
            ExchangeType.Fanout,
            Arg.Any<bool>(), Arg.Any<bool>(),
            Arg.Any<IDictionary<string, object?>>(),
            Arg.Any<bool>(), Arg.Any<CancellationToken>());

        // 3 groups
        await _mockChannel.Received(3).QueueDeclareAsync(
            Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<bool>(),
            Arg.Any<IDictionary<string, object?>>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
        await _mockChannel.Received(3).QueueBindAsync(
            Arg.Any<string>(), RabbitMqNaming.FanoutExchange(), string.Empty,
            Arg.Any<IDictionary<string, object?>>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PrepareInfrastructure_Streaming_DeletesAndRecreatesStreamQueue()
    {
        // Arrange
        var config = new JanitorConfig
        {
            MqConfig = CreateMqConfig(),
            CommunicationMode = CommunicationMode.Streaming,
            ConsumerGroups = [2]
        };

        // Act
        await _sut.PrepareInfrastructureAsync(config);

        // Assert
        await _mockChannel.Received(1).QueueDeleteAsync(
            RabbitMqNaming.StreamQueue(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
        await _mockChannel.Received(1).QueueDeclareAsync(
            RabbitMqNaming.StreamQueue(),
            Arg.Is(true), Arg.Is(false), Arg.Is(false),
            Arg.Is<IDictionary<string, object?>>(d => d.ContainsKey("x-queue-type")),
            Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }
}
