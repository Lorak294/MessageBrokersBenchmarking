using FluentAssertions;
using MqBenchmark.PgMq.Client;
using MqBenchmark.PgMq.Client.Operations;
using Xunit;

namespace MqBenchmark.PgMq.Client.Tests;

public class EncodePayloadTests
{
    [Fact]
    public void EncodePayload_SimpleBytes_ReturnsBase64()
    {
        // Arrange
        var payload = new byte[] { 1, 2, 3 };

        // Act
        var result = PgmqOperationsBase.EncodePayload(payload);

        // Assert
        result.Should().Be(Convert.ToBase64String(payload));
    }

    [Fact]
    public void EncodePayload_EmptyArray_ReturnsEmptyBase64()
    {
        // Arrange & Act
        var result = PgmqOperationsBase.EncodePayload(Array.Empty<byte>());

        // Assert
        result.Should().Be(string.Empty);
    }

    [Fact]
    public void EncodePayload_RoundTrips_WithReadMessage()
    {
        // Arrange
        var original = new byte[] { 42, 128, 255, 0, 1 };

        // Act
        var encoded = PgmqOperationsBase.EncodePayload(original);
        var decoded = Convert.FromBase64String(encoded);

        // Assert
        decoded.Should().BeEquivalentTo(original);
    }

    [Fact]
    public void EncodePayloads_SendOperations_WrapsEachInQuotes()
    {
        // Arrange
        var payloads = new List<byte[]>
        {
            new byte[] { 1, 2 },
            new byte[] { 3, 4, 5 }
        };

        // Act
        var result = SendOperations.EncodePayloads(payloads);

        // Assert
        result.Should().HaveCount(2);
        result[0].Should().Be("\"" + Convert.ToBase64String(payloads[0]) + "\"");
        result[1].Should().Be("\"" + Convert.ToBase64String(payloads[1]) + "\"");
    }

    [Fact]
    public void EncodePayloads_SendOperations_EmptyList_ReturnsEmptyArray()
    {
        // Arrange & Act
        var result = SendOperations.EncodePayloads(Array.Empty<byte[]>());

        // Assert
        result.Should().BeEmpty();
    }

    [Fact]
    public void EncodePayloads_TopicOperations_WrapsEachInQuotes()
    {
        // Arrange
        var payloads = new List<byte[]>
        {
            new byte[] { 10 },
            new byte[] { 20, 30 },
            new byte[] { 40, 50, 60 }
        };

        // Act
        var result = TopicOperations.EncodePayloads(payloads);

        // Assert
        result.Should().HaveCount(3);
        for (int i = 0; i < payloads.Count; i++)
        {
            result[i].Should().StartWith("\"").And.EndWith("\"");
            var inner = result[i][1..^1];
            Convert.FromBase64String(inner).Should().BeEquivalentTo(payloads[i]);
        }
    }

    [Fact]
    public void EncodePayloads_SingleItem_ReturnsArrayWithOneElement()
    {
        // Arrange
        var payloads = new List<byte[]> { new byte[] { 99 } };

        // Act
        var result = SendOperations.EncodePayloads(payloads);

        // Assert
        result.Should().HaveCount(1);
        result[0].Should().Be("\"" + Convert.ToBase64String(new byte[] { 99 }) + "\"");
    }
}
