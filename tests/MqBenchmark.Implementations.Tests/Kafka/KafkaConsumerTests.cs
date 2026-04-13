using Confluent.Kafka;
using FluentAssertions;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using MqBenchmark.Implementations.Kafka;
using NSubstitute;
using Xunit;

namespace MqBenchmark.Implementations.Tests.Kafka;

public class KafkaConsumerTests
{
    private readonly IConsumer<Null, byte[]> _mockConsumer;
    private readonly KafkaConsumer _sut;

    public KafkaConsumerTests()
    {
        _mockConsumer = Substitute.For<IConsumer<Null, byte[]>>();
        // Return non-empty assignment immediately to skip polling loop
        _mockConsumer.Assignment.Returns(new List<TopicPartition>
        {
            new("test", new Partition(0))
        });
        _sut = new KafkaConsumer(_mockConsumer);
    }

    private static MqConfig CreateConfig(CommunicationMode mode, string groupName = "group_0") => new()
    {
        Implementation = "Kafka",
        CommunicationMode = mode,
        ConsumerGroupName = groupName,
        AdditionalSettings = new Dictionary<string, string>
        {
            ["bootstrapServers"] = "localhost:9092"
        }
    };

    [Fact]
    public async Task InitializeAsync_PointToPoint_SubscribesToGroupTopic()
    {
        // Arrange & Act
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PointToPoint, "group_0"));

        // Assert
        _mockConsumer.Received(1).Subscribe(KafkaNaming.GroupTopic("group_0"));
    }

    [Fact]
    public async Task InitializeAsync_PubSub_SubscribesToSharedTopic()
    {
        // Arrange & Act
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PubSub, "group_1"));

        // Assert
        _mockConsumer.Received(1).Subscribe(KafkaNaming.SharedTopic());
    }

    [Fact]
    public async Task InitializeAsync_Streaming_SubscribesToSharedTopic()
    {
        // Arrange & Act
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.Streaming, "group_0"));

        // Assert
        _mockConsumer.Received(1).Subscribe(KafkaNaming.SharedTopic());
    }

    [Fact]
    public async Task SubscribeAsync_InvokesHandlerWithDeserializedMessage()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PubSub));
        var originalMessage = Message.CreateMessage(32);
        var receivedMessages = new List<Message>();

        var consumeResult = new ConsumeResult<Null, byte[]>
        {
            Message = new Message<Null, byte[]> { Value = originalMessage.Payload }
        };

        var callCount = 0;
        _mockConsumer.Consume(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callCount++;
                if (callCount == 1) return consumeResult;
                throw new OperationCanceledException();
            });

        // Act
        await _sut.SubscribeAsync(msg =>
        {
            receivedMessages.Add(msg);
            return Task.CompletedTask;
        });

        // Wait for the background task to process
        await Task.Delay(100);

        // Assert
        receivedMessages.Should().HaveCount(1);
        receivedMessages[0].Id.Should().Be(originalMessage.Id);
    }

    [Fact]
    public async Task SubscribeAsync_BeforeInitialize_Throws()
    {
        // Arrange
        var consumer = new KafkaConsumer();

        // Act
        var act = () => consumer.SubscribeAsync(_ => Task.CompletedTask);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>();
    }
}
