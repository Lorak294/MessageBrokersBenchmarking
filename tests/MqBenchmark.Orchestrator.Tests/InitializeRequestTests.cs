using FluentAssertions;
using MqBenchmark.Core.Config;
using MqBenchmark.Orchestrator.Contracts;
using Xunit;

namespace MqBenchmark.Orchestrator.Tests;

public class InitializeRequestTests
{
    [Fact]
    public void GetTotalMessageCount_WithMessagesPerConsumerGroup_ReturnsSum()
    {
        // Arrange
        var request = CreateRequest(messageCount: null, messagesPerGroup: [100, 200, 50]);

        // Act
        var result = request.GetTotalMessageCount();

        // Assert
        result.Should().Be(350);
    }

    [Fact]
    public void GetTotalMessageCount_WithoutMessagesPerConsumerGroup_ReturnsMessageCount()
    {
        // Arrange
        var request = CreateRequest(messageCount: 500, messagesPerGroup: null);

        // Act
        var result = request.GetTotalMessageCount();

        // Assert
        result.Should().Be(500);
    }

    [Fact]
    public void GetMessagesPerGroup_WithMessagesPerConsumerGroup_ReturnsSameArray()
    {
        // Arrange
        var expected = new[] { 10, 20, 30 };
        var request = CreateRequest(messageCount: null, messagesPerGroup: expected);

        // Act
        var result = request.GetMessagesPerGroup();

        // Assert
        result.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void GetMessagesPerGroup_EqualDistributionWithRemainder()
    {
        // Arrange — 10 messages across 3 groups = [4, 3, 3]
        var request = CreateRequest(messageCount: 10, messagesPerGroup: null, groupCount: 3);

        // Act
        var result = request.GetMessagesPerGroup();

        // Assert
        result.Should().HaveCount(3);
        result.Sum().Should().Be(10);
        result[0].Should().Be(4); // remainder goes to first
        result[1].Should().Be(3);
        result[2].Should().Be(3);
    }

    private static InitializeRequest CreateRequest(int? messageCount, int[]? messagesPerGroup, int groupCount = 3)
    {
        return new InitializeRequest
        {
            ProducersCount = 1,
            ConsumerGroups = Enumerable.Repeat(1, groupCount).ToArray(),
            CommunicationMode = CommunicationMode.PointToPoint,
            MessageCount = messageCount,
            MessagesPerConsumerGroup = messagesPerGroup,
            MessageSizeInBytes = 64,
            MqConfig = new MqConfig { Implementation = "Kafka" }
        };
    }
}
