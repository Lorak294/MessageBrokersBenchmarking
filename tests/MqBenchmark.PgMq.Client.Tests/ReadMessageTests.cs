using System.Data.Common;
using FluentAssertions;
using MqBenchmark.PgMq.Client;
using NSubstitute;
using Xunit;

namespace MqBenchmark.PgMq.Client.Tests;

public class ReadMessageTests
{
    [Fact]
    public void ReadMessage_NormalRow_DecodesBase64AndMapsAllFields()
    {
        // Arrange
        var payload = new byte[] { 1, 2, 3, 4, 5 };
        var base64 = Convert.ToBase64String(payload);
        var enqueuedAt = new DateTime(2025, 1, 15, 10, 30, 0, DateTimeKind.Utc);
        var lastReadAt = new DateTime(2025, 1, 15, 10, 31, 0, DateTimeKind.Utc);
        var vt = new DateTime(2025, 1, 15, 10, 32, 0, DateTimeKind.Utc);

        var reader = CreateMockReader(
            msgId: 42L,
            readCount: 3,
            enqueuedAt: enqueuedAt,
            lastReadAt: lastReadAt,
            vt: vt,
            messageJson: $"\"{base64}\"");

        // Act
        var msg = PgmqOperationsBase.ReadMessage(reader);

        // Assert
        msg.MsgId.Should().Be(42L);
        msg.ReadCount.Should().Be(3);
        msg.EnqueuedAt.Should().Be(enqueuedAt);
        msg.LastReadAt.Should().Be(lastReadAt);
        msg.Vt.Should().Be(vt);
        msg.Payload.Should().BeEquivalentTo(payload);
    }

    [Fact]
    public void ReadMessage_NullLastReadAt_MapsToNull()
    {
        // Arrange
        var payload = new byte[] { 10, 20 };
        var base64 = Convert.ToBase64String(payload);
        var enqueuedAt = new DateTime(2025, 3, 1, 0, 0, 0, DateTimeKind.Utc);
        var vt = new DateTime(2025, 3, 1, 0, 1, 0, DateTimeKind.Utc);

        var reader = CreateMockReader(
            msgId: 99L,
            readCount: 0,
            enqueuedAt: enqueuedAt,
            lastReadAt: null,
            vt: vt,
            messageJson: $"\"{base64}\"");

        // Act
        var msg = PgmqOperationsBase.ReadMessage(reader);

        // Assert
        msg.LastReadAt.Should().BeNull();
        msg.ReadCount.Should().Be(0);
        msg.Payload.Should().BeEquivalentTo(payload);
    }

    [Fact]
    public void ReadMessage_PayloadWithoutSurroundingQuotes_StillDecodes()
    {
        // Arrange
        var payload = new byte[] { 255, 128, 0 };
        var base64 = Convert.ToBase64String(payload); // no quotes
        var reader = CreateMockReader(
            msgId: 1L, readCount: 1,
            enqueuedAt: DateTime.UtcNow, lastReadAt: null,
            vt: DateTime.UtcNow, messageJson: base64);

        // Act
        var msg = PgmqOperationsBase.ReadMessage(reader);

        // Assert
        msg.Payload.Should().BeEquivalentTo(payload);
    }

    [Fact]
    public void ReadMessage_EmptyPayload_ReturnsEmptyByteArray()
    {
        // Arrange
        var base64 = Convert.ToBase64String(Array.Empty<byte>());
        var reader = CreateMockReader(
            msgId: 7L, readCount: 0,
            enqueuedAt: DateTime.UtcNow, lastReadAt: null,
            vt: DateTime.UtcNow, messageJson: $"\"{base64}\"");

        // Act
        var msg = PgmqOperationsBase.ReadMessage(reader);

        // Assert
        msg.Payload.Should().BeEmpty();
    }

    [Fact]
    public void ReadMessage_LargePayload_DecodesCorrectly()
    {
        // Arrange
        var payload = new byte[4096];
        Random.Shared.NextBytes(payload);
        var base64 = Convert.ToBase64String(payload);
        var reader = CreateMockReader(
            msgId: 100L, readCount: 5,
            enqueuedAt: DateTime.UtcNow, lastReadAt: DateTime.UtcNow,
            vt: DateTime.UtcNow, messageJson: $"\"{base64}\"");

        // Act
        var msg = PgmqOperationsBase.ReadMessage(reader);

        // Assert
        msg.Payload.Should().BeEquivalentTo(payload);
    }

    private static DbDataReader CreateMockReader(
        long msgId, int readCount, DateTime enqueuedAt,
        DateTime? lastReadAt, DateTime vt, string messageJson)
    {
        var reader = Substitute.For<DbDataReader>();
        reader.GetInt64(0).Returns(msgId);
        reader.GetInt32(1).Returns(readCount);
        reader.GetDateTime(2).Returns(enqueuedAt);
        reader.IsDBNull(3).Returns(lastReadAt is null);
        if (lastReadAt.HasValue)
            reader.GetDateTime(3).Returns(lastReadAt.Value);
        reader.GetDateTime(4).Returns(vt);
        reader.GetString(5).Returns(messageJson);
        return reader;
    }
}
