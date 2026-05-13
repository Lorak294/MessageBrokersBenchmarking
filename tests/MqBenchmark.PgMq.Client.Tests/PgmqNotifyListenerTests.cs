using FluentAssertions;
using MqBenchmark.PgMq.Client;
using Xunit;

namespace MqBenchmark.PgMq.Client.Tests;

public class PgmqNotifyListenerTests
{
    [Fact]
    public void HandleNotification_MatchingChannel_SetsNotified()
    {
        // Arrange
        var listener = new PgmqNotifyListener("Host=localhost", "test_queue");

        // Act
        listener.HandleNotification("pgmq.q_test_queue.INSERT");

        // Assert
        listener.IsNotified.Should().BeTrue();
    }

    [Fact]
    public void HandleNotification_NonMatchingChannel_DoesNotSetNotified()
    {
        // Arrange
        var listener = new PgmqNotifyListener("Host=localhost", "my_queue");

        // Act
        listener.HandleNotification("pgmq.q_other_queue.INSERT");

        // Assert
        listener.IsNotified.Should().BeFalse();
    }

    [Fact]
    public void HandleNotification_CaseInsensitiveMatch_SetsNotified()
    {
        // Arrange
        var listener = new PgmqNotifyListener("Host=localhost", "MyQueue");

        // Act
        listener.HandleNotification("PGMQ.Q_MYQUEUE.INSERT");

        // Assert
        listener.IsNotified.Should().BeTrue();
    }

    [Fact]
    public void HandleNotification_PartialMatch_DoesNotSetNotified()
    {
        // Arrange
        var listener = new PgmqNotifyListener("Host=localhost", "queue");

        // Act
        listener.HandleNotification("pgmq.q_queue.UPDATE"); // wrong suffix

        // Assert
        listener.IsNotified.Should().BeFalse();
    }

    [Fact]
    public void HandleNotification_EmptyChannel_DoesNotSetNotified()
    {
        // Arrange
        var listener = new PgmqNotifyListener("Host=localhost", "q");

        // Act
        listener.HandleNotification(string.Empty);

        // Assert
        listener.IsNotified.Should().BeFalse();
    }

    [Fact]
    public async Task WaitAsync_WithTimeout_BeforeStart_ThrowsInvalidOperationException()
    {
        // Arrange
        var listener = new PgmqNotifyListener("Host=localhost", "q");

        // Act
        var act = async () => await listener.WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*StartAsync*");
    }

    [Fact]
    public async Task WaitAsync_WithCancellationToken_BeforeStart_ThrowsInvalidOperationException()
    {
        // Arrange
        var listener = new PgmqNotifyListener("Host=localhost", "q");

        // Act
        var act = async () => await listener.WaitAsync(CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*StartAsync*");
    }

    [Fact]
    public void HandleNotification_MultipleQueues_OnlyMatchesOwn()
    {
        // Arrange
        var listener1 = new PgmqNotifyListener("Host=localhost", "alpha");
        var listener2 = new PgmqNotifyListener("Host=localhost", "beta");

        // Act
        listener1.HandleNotification("pgmq.q_beta.INSERT");
        listener2.HandleNotification("pgmq.q_alpha.INSERT");

        // Assert
        listener1.IsNotified.Should().BeFalse();
        listener2.IsNotified.Should().BeFalse();
    }

    [Fact]
    public void HandleNotification_CorrectChannel_AfterWrongOne_SetsNotified()
    {
        // Arrange
        var listener = new PgmqNotifyListener("Host=localhost", "target");

        // Act
        listener.HandleNotification("pgmq.q_wrong.INSERT");
        listener.HandleNotification("pgmq.q_target.INSERT");

        // Assert
        listener.IsNotified.Should().BeTrue();
    }

    [Fact]
    public async Task DisposeAsync_BeforeStart_DoesNotThrow()
    {
        // Arrange
        var listener = new PgmqNotifyListener("Host=localhost", "q");

        // Act
        var act = async () => await listener.DisposeAsync();

        // Assert
        await act.Should().NotThrowAsync();
    }
}
