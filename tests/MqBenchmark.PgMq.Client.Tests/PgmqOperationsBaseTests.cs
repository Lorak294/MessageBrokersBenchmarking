using FluentAssertions;
using MqBenchmark.PgMq.Client;
using Npgsql;
using NSubstitute;
using Xunit;

namespace MqBenchmark.PgMq.Client.Tests;

public class PgmqOperationsBaseTests
{
    [Fact]
    public async Task DisposeAsync_WithNoPreparedCommands_CompletesSuccessfully()
    {
        // Arrange
        var connection = new NpgsqlConnection("Host=localhost");
        var ops = new TestableOperations(connection);

        // Act
        var act = async () => await ops.DisposeAsync();

        // Assert
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public void ReadMessage_WithDbNullLastReadAt_ReturnsNullLastReadAt()
    {
        // Arrange
        var reader = CreateMockReader(lastReadAtNull: true);

        // Act
        var result = PgmqOperationsBase.ReadMessage(reader);

        // Assert
        result.LastReadAt.Should().BeNull();
        result.MsgId.Should().Be(1L);
    }

    [Fact]
    public void ReadMessage_WithValidLastReadAt_ReturnsValue()
    {
        // Arrange
        var expectedTime = new DateTime(2025, 6, 15, 12, 0, 0, DateTimeKind.Utc);
        var reader = CreateMockReader(lastReadAtNull: false, lastReadAt: expectedTime);

        // Act
        var result = PgmqOperationsBase.ReadMessage(reader);

        // Assert
        result.LastReadAt.Should().Be(expectedTime);
    }

    [Fact]
    public void EncodePayload_And_ReadMessage_RoundTrip()
    {
        // Arrange
        var original = new byte[] { 0, 127, 255, 42, 99 };
        var encoded = PgmqOperationsBase.EncodePayload(original);
        var jsonInDb = $"\"{encoded}\""; // as stored in JSONB

        var reader = CreateMockReader(messageJson: jsonInDb);

        // Act
        var result = PgmqOperationsBase.ReadMessage(reader);

        // Assert
        result.Payload.Should().BeEquivalentTo(original);
    }

    private static System.Data.Common.DbDataReader CreateMockReader(
        bool lastReadAtNull = true,
        DateTime? lastReadAt = null,
        string? messageJson = null)
    {
        var payload = new byte[] { 1, 2, 3 };
        var base64 = Convert.ToBase64String(payload);
        var json = messageJson ?? $"\"{base64}\"";

        var reader = Substitute.For<System.Data.Common.DbDataReader>();
        reader.GetInt64(0).Returns(1L);
        reader.GetInt32(1).Returns(0);
        reader.GetDateTime(2).Returns(DateTime.UtcNow);
        reader.IsDBNull(3).Returns(lastReadAtNull);
        if (!lastReadAtNull && lastReadAt.HasValue)
            reader.GetDateTime(3).Returns(lastReadAt.Value);
        reader.GetDateTime(4).Returns(DateTime.UtcNow);
        reader.GetString(5).Returns(json);
        return reader;
    }
    
    private class TestableOperations : PgmqOperationsBase
    {
        public TestableOperations(NpgsqlConnection connection) : base(connection) { }
    }
}
