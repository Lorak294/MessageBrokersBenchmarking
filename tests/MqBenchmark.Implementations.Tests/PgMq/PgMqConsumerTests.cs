using FluentAssertions;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.MqImplementation;
using MqBenchmark.Implementations.PgMq;
using MqBenchmark.PgMq.Client;
using MqBenchmark.PgMq.Client.Models;
using NSubstitute;
using Xunit;

namespace MqBenchmark.Implementations.Tests.PgMq;

public class PgMqConsumerTests
{
    private readonly IPgmqClient _mockClient;
    private readonly IQueueOperations _mockQueues;
    private readonly IPopOperations _mockPop;
    private readonly IReadOperations _mockRead;
    private readonly IDeleteOperations _mockDelete;
    private readonly IArchiveOperations _mockArchive;
    private readonly PgMqConsumer _sut;

    public PgMqConsumerTests()
    {
        _mockClient = Substitute.For<IPgmqClient>();
        _mockQueues = Substitute.For<IQueueOperations>();
        _mockPop = Substitute.For<IPopOperations>();
        _mockRead = Substitute.For<IReadOperations>();
        _mockDelete = Substitute.For<IDeleteOperations>();
        _mockArchive = Substitute.For<IArchiveOperations>();
        _mockClient.Queues.Returns(_mockQueues);
        _mockClient.Pop.Returns(_mockPop);
        _mockClient.Read.Returns(_mockRead);
        _mockClient.Delete.Returns(_mockDelete);
        _mockClient.Archive.Returns(_mockArchive);
        _mockClient.Notify.Returns(Substitute.For<INotifyOperations>());
        _sut = new PgMqConsumer(_mockClient);
    }

    private static MqConfig CreateConfig(
        CommunicationMode mode,
        string groupName = "group_0",
        string readMode = "Delete",
        string consumerMode = "ClientPoll",
        bool usePop = true) => new()
    {
        Implementation = "PgMq",
        CommunicationMode = mode,
        ConsumerGroupName = groupName,
        AdditionalSettings = new Dictionary<string, string>
        {
            ["connectionString"] = "Host=localhost;Database=test",
            ["messageReadMode"] = readMode,
            ["consumerMode"] = consumerMode,
            ["usePop"] = usePop.ToString().ToLower(),
            ["pollIntervalMs"] = "1",
            ["consumerBatchSize"] = "1"
        }
    };

    private static PgmqMessage CreatePgmqMessage(long msgId)
    {
        var payload = Message.CreateMessage(32).Payload;
        return new PgmqMessage
        {
            MsgId = msgId,
            ReadCount = 1,
            EnqueuedAt = DateTime.UtcNow,
            Vt = DateTime.UtcNow.AddSeconds(30),
            Payload = payload
        };
    }

    [Fact]
    public async Task InitializeAsync_PointToPoint_CreatesGroupQueue()
    {
        // Arrange & Act
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PointToPoint, "group_2"));

        // Assert
        await _mockQueues.Received(1).CreateAsync(
            PgMqNaming.GroupQueue("group_2"), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task InitializeAsync_Streaming_CreatesStreamQueue()
    {
        // Arrange & Act
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.Streaming));

        // Assert
        await _mockQueues.Received(1).CreateAsync(
            PgMqNaming.StreamQueue(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SubscribeAsync_ClientPoll_Pop_InvokesHandler()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PointToPoint, usePop: true, readMode: "Delete"));
        var pgMsg = CreatePgmqMessage(1);
        var receivedMessages = new List<Message>();

        var callCount = 0;
        _mockPop.PopAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callCount++;
                if (callCount == 1) return new List<PgmqMessage> { pgMsg };
                return new List<PgmqMessage>();
            });

        // Act
        await _sut.SubscribeAsync(msg => { receivedMessages.Add(msg); return Task.CompletedTask; });
        await Task.Delay(100);

        // Assert
        receivedMessages.Should().HaveCountGreaterThanOrEqualTo(1);
        receivedMessages[0].Payload.Should().BeEquivalentTo(pgMsg.Payload);

        // Cleanup
        await _sut.DisposeAsync();
    }

    [Fact]
    public async Task SubscribeAsync_ClientPoll_ReadDelete_DeletesAfterProcessing()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PointToPoint, usePop: false, readMode: "Delete"));
        var pgMsg = CreatePgmqMessage(42);

        var callCount = 0;
        _mockRead.ReadAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callCount++;
                if (callCount == 1) return new List<PgmqMessage> { pgMsg };
                return new List<PgmqMessage>();
            });

        // Act
        await _sut.SubscribeAsync(_ => Task.CompletedTask);
        await Task.Delay(100);

        // Assert
        await _mockDelete.Received(1).DeleteBatchAsync(
            Arg.Any<string>(),
            Arg.Is<long[]>(ids => ids.Contains(42)),
            Arg.Any<CancellationToken>());

        // Cleanup
        await _sut.DisposeAsync();
    }

    [Fact]
    public async Task SubscribeAsync_ClientPoll_ReadArchive_ArchivesAfterProcessing()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.PointToPoint, usePop: false, readMode: "Archive"));
        var pgMsg = CreatePgmqMessage(99);

        var callCount = 0;
        _mockRead.ReadAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callCount++;
                if (callCount == 1) return new List<PgmqMessage> { pgMsg };
                return new List<PgmqMessage>();
            });

        // Act
        await _sut.SubscribeAsync(_ => Task.CompletedTask);
        await Task.Delay(100);

        // Assert
        await _mockArchive.Received(1).ArchiveBatchAsync(
            Arg.Any<string>(),
            Arg.Is<long[]>(ids => ids.Contains(99)),
            Arg.Any<CancellationToken>());

        // Cleanup
        await _sut.DisposeAsync();
    }

    [Fact]
    public async Task SubscribeAsync_Streaming_TracksOffset()
    {
        // Arrange
        await _sut.InitializeAsync(CreateConfig(CommunicationMode.Streaming));
        var msg1 = CreatePgmqMessage(10);
        var msg2 = CreatePgmqMessage(20);

        var callCount = 0;
        _mockRead.ReadFromOffsetAsync(Arg.Any<string>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callCount++;
                if (callCount == 1) return new List<PgmqMessage> { msg1, msg2 };
                return new List<PgmqMessage>();
            });

        // Act
        await _sut.SubscribeAsync(_ => Task.CompletedTask);
        await Task.Delay(100);

        // Assert - second call should use offset 20 (last processed msgId)
        await _mockRead.Received().ReadFromOffsetAsync(
            Arg.Any<string>(), 20, Arg.Any<int>(), Arg.Any<CancellationToken>());

        // Cleanup
        await _sut.DisposeAsync();
    }

    [Fact]
    public async Task SubscribeAsync_BeforeInitialize_Throws()
    {
        // Arrange
        var consumer = new PgMqConsumer();

        // Act
        var act = async () => await consumer.SubscribeAsync(_ => Task.CompletedTask);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>();
    }
}
