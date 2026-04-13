using Confluent.Kafka;
using FluentAssertions;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using MqBenchmark.Implementations.Kafka;
using NSubstitute;
using Xunit;

namespace MqBenchmark.Implementations.Tests.Kafka;

public class KafkaProducerTests
{
    private readonly IProducer<Null, byte[]> _mockProducer;
    private readonly KafkaProducer _sut;

    public KafkaProducerTests()
    {
        _mockProducer = Substitute.For<IProducer<Null, byte[]>>();
        _sut = new KafkaProducer(_mockProducer);
    }

    private static MqConfig CreateConfig(CommunicationMode mode, bool buffered = true) => new()
    {
        Implementation = "Kafka",
        CommunicationMode = mode,
        AdditionalSettings = new Dictionary<string, string>
        {
            ["bootstrapServers"] = "localhost:9092",
            ["useBufferedProducer"] = buffered.ToString().ToLower()
        }
    };

    [Fact]
    public async Task SendAsync_PointToPoint_ProducesToGroupTopic()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PointToPoint));
        var message = Message.CreateMessage(32);

        // Act
        await _sut.SendAsync(message, "group_0");

        // Assert
        _mockProducer.Received(1).Produce(
            KafkaNaming.GroupTopic("group_0"),
            Arg.Any<Message<Null, byte[]>>(),
            Arg.Any<Action<DeliveryReport<Null, byte[]>>>());
    }

    [Fact]
    public async Task SendAsync_PubSub_ProducesToSharedTopic()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PubSub));
        var message = Message.CreateMessage(32);

        // Act
        await _sut.SendAsync(message);

        // Assert
        _mockProducer.Received(1).Produce(
            KafkaNaming.SharedTopic(),
            Arg.Any<Message<Null, byte[]>>(),
            Arg.Any<Action<DeliveryReport<Null, byte[]>>>());
    }

    [Fact]
    public async Task SendAsync_Streaming_ProducesToSharedTopic()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.Streaming));
        var message = Message.CreateMessage(32);

        // Act
        await _sut.SendAsync(message);

        // Assert
        _mockProducer.Received(1).Produce(
            KafkaNaming.SharedTopic(),
            Arg.Any<Message<Null, byte[]>>(),
            Arg.Any<Action<DeliveryReport<Null, byte[]>>>());
    }

    [Fact]
    public async Task SendAsync_PointToPoint_WithoutRoutingTarget_Throws()
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
        var producer = new KafkaProducer();
        var message = Message.CreateMessage(32);

        // Act
        var act = () => producer.SendAsync(message, "group_0");

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*not initialized*");
    }

    [Fact]
    public async Task SendAsync_NonBufferedMode_UsesProduceAsync()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PubSub, buffered: false));
        var message = Message.CreateMessage(32);
        _mockProducer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<Null, byte[]>>(), Arg.Any<CancellationToken>())
            .Returns(new DeliveryResult<Null, byte[]>());

        // Act
        await _sut.SendAsync(message);

        // Assert
        await _mockProducer.Received(1).ProduceAsync(
            KafkaNaming.SharedTopic(),
            Arg.Any<Message<Null, byte[]>>(),
            Arg.Any<CancellationToken>());
        _mockProducer.DidNotReceive().Produce(
            Arg.Any<string>(),
            Arg.Any<Message<Null, byte[]>>(),
            Arg.Any<Action<DeliveryReport<Null, byte[]>>>());
    }

    [Fact]
    public async Task SendAsync_BufferedMode_QueueFull_FlushesAndRetries()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PubSub, buffered: true));
        var message = Message.CreateMessage(32);

        var callCount = 0;
        _mockProducer.When(p => p.Produce(Arg.Any<string>(), Arg.Any<Message<Null, byte[]>>(), Arg.Any<Action<DeliveryReport<Null, byte[]>>>()))
            .Do(_ =>
            {
                callCount++;
                if (callCount == 1)
                    throw new ProduceException<Null, byte[]>(new Error(ErrorCode.Local_QueueFull), new DeliveryResult<Null, byte[]>());
            });

        // Act
        await _sut.SendAsync(message);

        // Assert
        _mockProducer.Received(1).Flush(Arg.Any<TimeSpan>());
        _mockProducer.Received(2).Produce(
            Arg.Any<string>(),
            Arg.Any<Message<Null, byte[]>>(),
            Arg.Any<Action<DeliveryReport<Null, byte[]>>>());
    }

    [Fact]
    public async Task SendAsync_BufferedMode_PayloadPassedCorrectly()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PubSub));
        var message = Message.CreateMessage(64);

        // Act
        await _sut.SendAsync(message);

        // Assert
        _mockProducer.Received(1).Produce(
            Arg.Any<string>(),
            Arg.Is<Message<Null, byte[]>>(m => m.Value == message.Payload),
            Arg.Any<Action<DeliveryReport<Null, byte[]>>>());
    }
}
