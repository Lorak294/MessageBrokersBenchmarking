using FluentAssertions;
using MqBenchmark.Core.Config;
using MqBenchmark.Orchestrator.Contracts;
using MqBenchmark.Orchestrator.Tests.Helpers;
using Xunit;

namespace MqBenchmark.Orchestrator.Tests;

public class InitializeRequestValidatorTests
{
    [Fact]
    public void ValidRequest_ReturnsNoErrors()
    {
        // Arrange
        var request = TestData.ValidPointToPointRequest();

        // Act
        var errors = InitializeRequestValidator.Validate(request);

        // Assert
        errors.Should().BeEmpty();
    }

    [Fact]
    public void ValidPubSubRequest_ReturnsNoErrors()
    {
        // Arrange
        var request = TestData.ValidPubSubRequest();

        // Act
        var errors = InitializeRequestValidator.Validate(request);

        // Assert
        errors.Should().BeEmpty();
    }

    [Fact]
    public void InvalidCommunicationMode_ReturnsError()
    {
        // Arrange
        var request = TestData.ValidPointToPointRequest();
        request.CommunicationMode = (CommunicationMode)99;

        // Act
        var errors = InitializeRequestValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Contains("communicationMode"));
    }

    [Fact]
    public void MessageSizeBelow16_ReturnsError()
    {
        // Arrange
        var request = TestData.ValidPointToPointRequest();
        request.MessageSizeInBytes = 15;

        // Act
        var errors = InitializeRequestValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Contains("messageSizeInBytes") && e.Contains("16"));
    }

    [Fact]
    public void MessageSizeExactly16_NoError()
    {
        // Arrange
        var request = TestData.ValidPointToPointRequest();
        request.MessageSizeInBytes = 16;

        // Act
        var errors = InitializeRequestValidator.Validate(request);

        // Assert
        errors.Should().NotContain(e => e.Contains("messageSizeInBytes"));
    }

    [Fact]
    public void SendFrequencyZero_ReturnsError()
    {
        // Arrange
        var request = TestData.ValidPointToPointRequest();
        request.SendFrequencyMps = 0;

        // Act
        var errors = InitializeRequestValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Contains("sendFrequencyMps"));
    }

    [Fact]
    public void SendFrequencyNull_NoError()
    {
        // Arrange
        var request = TestData.ValidPointToPointRequest();
        request.SendFrequencyMps = null;

        // Act
        var errors = InitializeRequestValidator.Validate(request);

        // Assert
        errors.Should().NotContain(e => e.Contains("sendFrequencyMps"));
    }

    [Fact]
    public void ProducersCountZero_ReturnsError()
    {
        // Arrange
        var request = TestData.ValidPointToPointRequest();
        request.ProducersCount = 0;

        // Act
        var errors = InitializeRequestValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Contains("producersCount"));
    }

    [Fact]
    public void EmptyConsumerGroups_ReturnsError()
    {
        // Arrange
        var request = TestData.ValidPointToPointRequest();
        request.ConsumerGroups = [];

        // Act
        var errors = InitializeRequestValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Contains("consumerGroups") && e.Contains("non-empty"));
    }

    [Fact]
    public void NegativeValueInConsumerGroups_ReturnsError()
    {
        // Arrange
        var request = TestData.ValidPointToPointRequest();
        request.ConsumerGroups = [2, -1, 3];

        // Act
        var errors = InitializeRequestValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Contains("consumerGroups[1]"));
    }

    [Fact]
    public void MessagesPerConsumerGroup_LengthMismatch_ReturnsError()
    {
        // Arrange
        var request = TestData.ValidPointToPointRequest();
        request.ConsumerGroups = [2, 3];
        request.MessageCount = null;
        request.MessagesPerConsumerGroup = [100, 200, 300]; // 3 != 2

        // Act
        var errors = InitializeRequestValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Contains("messagesPerConsumerGroup length"));
    }

    [Fact]
    public void PubSub_WithoutMessageCount_ReturnsError()
    {
        // Arrange
        var request = TestData.ValidPubSubRequest();
        request.MessageCount = null;

        // Act
        var errors = InitializeRequestValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Contains("PubSub") && e.Contains("messageCount"));
    }

    [Fact]
    public void Streaming_WithoutMessageCount_ReturnsError()
    {
        // Arrange
        var request = TestData.ValidStreamingRequest();
        request.MessageCount = null;

        // Act
        var errors = InitializeRequestValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Contains("Streaming") && e.Contains("messageCount"));
    }

    [Fact]
    public void PointToPoint_BothMessageCountAndMessagesPerGroup_ReturnsError()
    {
        // Arrange
        var request = TestData.ValidPointToPointRequest();
        request.ConsumerGroups = [2, 3];
        request.MessageCount = 1000;
        request.MessagesPerConsumerGroup = [500, 500];

        // Act
        var errors = InitializeRequestValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Contains("either messageCount or messagesPerConsumerGroup"));
    }

    [Fact]
    public void PointToPoint_NeitherMessageCountNorMessagesPerGroup_ReturnsError()
    {
        // Arrange
        var request = TestData.ValidPointToPointRequest();
        request.MessageCount = null;
        request.MessagesPerConsumerGroup = null;

        // Act
        var errors = InitializeRequestValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Contains("either messageCount or messagesPerConsumerGroup must be set"));
    }

    [Fact]
    public void Streaming_RabbitMQ_MultipleConsumersPerGroup_ReturnsError()
    {
        // Arrange
        var request = TestData.ValidStreamingRequest();
        request.MqConfig = new MqConfig { Implementation = "RabbitMQ" };
        request.ConsumerGroups = [2, 1]; // group 0 has 2 consumers

        // Act
        var errors = InitializeRequestValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Contains("Streaming") && e.Contains("RabbitMQ"));
    }

    [Fact]
    public void MissingMqConfig_ReturnsError()
    {
        // Arrange
        var request = TestData.ValidPointToPointRequest();
        request.MqConfig = null!;

        // Act
        var errors = InitializeRequestValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Contains("mqConfig is required"));
    }

    [Fact]
    public void InvalidImplementation_ReturnsError()
    {
        // Arrange
        var request = TestData.ValidPointToPointRequest();
        request.MqConfig = new MqConfig { Implementation = "Redis" };

        // Act
        var errors = InitializeRequestValidator.Validate(request);

        // Assert
        errors.Should().Contain(e => e.Contains("must be one of"));
    }
}
