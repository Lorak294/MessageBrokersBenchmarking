using FluentAssertions;
using Microsoft.Extensions.Logging;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.Metrics;
using MqBenchmark.Orchestrator.Services;
using NSubstitute;
using Xunit;

namespace MqBenchmark.Orchestrator.Tests;

public class TimestampAggregatorTests : IDisposable
{
    private readonly TimestampAggregator _aggregator;

    public TimestampAggregatorTests()
    {
        var logger = Substitute.For<ILogger<TimestampAggregator>>();
        _aggregator = new TimestampAggregator(logger);
    }

    public void Dispose()
    {
        // Clean up results directory created by constructor
        if (Directory.Exists(TimestampAggregator.ResultsDirectory))
            Directory.Delete(TimestampAggregator.ResultsDirectory, recursive: true);
    }

    [Fact]
    public void ComputeResults_SingleGroup_AllMatched_CorrectLatencies()
    {
        // Arrange
        var messageIds = Enumerable.Range(0, 5).Select(_ => Guid.NewGuid()).ToList();
        var baseTicks = DateTime.UtcNow.Ticks;
        var latencyTicks = TimeSpan.FromMilliseconds(10).Ticks;

        _aggregator.SubmitTimestamps(new WorkerTimestampData
        {
            WorkerId = Guid.NewGuid(),
            Role = WorkerConfig.Roles.Producer,
            Timestamps = messageIds.Select(id => new MessageTimestamp
            {
                MessageId = id,
                TimestampTicks = baseTicks
            }).ToList()
        });

        _aggregator.SubmitTimestamps(new WorkerTimestampData
        {
            WorkerId = Guid.NewGuid(),
            Role = WorkerConfig.Roles.Consumer,
            Timestamps = messageIds.Select(id => new MessageTimestamp
            {
                MessageId = id,
                TimestampTicks = baseTicks + latencyTicks
            }).ToList(),
            ConsumerGroupIndex = 0
        });

        _aggregator.SetExpectedMessagesPerGroup(null, 1);

        // Act
        var results = _aggregator.ComputeResults();

        // Assert
        results.TotalMessagesSent.Should().Be(5);
        results.PerGroupResults.Should().HaveCount(1);
        results.PerGroupResults[0].MessagesLost.Should().Be(0);
        results.PerGroupResults[0].AverageLatencyMs.Should().BeApproximately(10.0, 0.01);
    }

    [Fact]
    public void ComputeResults_MessagesLost_ReportsCorrectCount()
    {
        // Arrange
        var messageIds = Enumerable.Range(0, 10).Select(_ => Guid.NewGuid()).ToList();
        var baseTicks = DateTime.UtcNow.Ticks;

        _aggregator.SubmitTimestamps(new WorkerTimestampData
        {
            WorkerId = Guid.NewGuid(),
            Role = WorkerConfig.Roles.Producer,
            Timestamps = messageIds.Select(id => new MessageTimestamp
            {
                MessageId = id,
                TimestampTicks = baseTicks
            }).ToList()
        });

        _aggregator.SubmitTimestamps(new WorkerTimestampData
        {
            WorkerId = Guid.NewGuid(),
            Role = WorkerConfig.Roles.Consumer,
            Timestamps = messageIds.Take(7).Select(id => new MessageTimestamp
            {
                MessageId = id,
                TimestampTicks = baseTicks + TimeSpan.FromMilliseconds(5).Ticks
            }).ToList(),
            ConsumerGroupIndex = 0
        });

        _aggregator.SetExpectedMessagesPerGroup(null, 1);

        // Act
        var results = _aggregator.ComputeResults();

        // Assert
        results.PerGroupResults[0].MessagesLost.Should().Be(3);
    }

    [Fact]
    public void ComputeResults_MultipleGroups_WithExpectedPerGroup()
    {
        // Arrange
        var messageIds = Enumerable.Range(0, 10).Select(_ => Guid.NewGuid()).ToList();
        var baseTicks = DateTime.UtcNow.Ticks;

        _aggregator.SubmitTimestamps(new WorkerTimestampData
        {
            WorkerId = Guid.NewGuid(),
            Role = WorkerConfig.Roles.Producer,
            Timestamps = messageIds.Select(id => new MessageTimestamp
            {
                MessageId = id,
                TimestampTicks = baseTicks
            }).ToList()
        });

        _aggregator.SubmitTimestamps(new WorkerTimestampData
        {
            WorkerId = Guid.NewGuid(),
            Role = WorkerConfig.Roles.Consumer,
            Timestamps = messageIds.Take(4).Select(id => new MessageTimestamp
            {
                MessageId = id,
                TimestampTicks = baseTicks + TimeSpan.FromMilliseconds(5).Ticks
            }).ToList(),
            ConsumerGroupIndex = 0
        });

        _aggregator.SubmitTimestamps(new WorkerTimestampData
        {
            WorkerId = Guid.NewGuid(),
            Role = WorkerConfig.Roles.Consumer,
            Timestamps = messageIds.Skip(4).Take(3).Select(id => new MessageTimestamp
            {
                MessageId = id,
                TimestampTicks = baseTicks + TimeSpan.FromMilliseconds(8).Ticks
            }).ToList(),
            ConsumerGroupIndex = 1
        });

        _aggregator.SetExpectedMessagesPerGroup([6, 4], 2);

        // Act
        var results = _aggregator.ComputeResults();

        // Assert
        results.PerGroupResults.Should().HaveCount(2);
        results.PerGroupResults[0].MessagesLost.Should().Be(2); // expected 6, got 4
        results.PerGroupResults[1].MessagesLost.Should().Be(1); // expected 4, got 3
    }

    [Fact]
    public void ComputeResults_PubSubMode_AllGroupsExpectTotalSent()
    {
        // Arrange
        var messageIds = Enumerable.Range(0, 5).Select(_ => Guid.NewGuid()).ToList();
        var baseTicks = DateTime.UtcNow.Ticks;

        _aggregator.SubmitTimestamps(new WorkerTimestampData
        {
            WorkerId = Guid.NewGuid(),
            Role = WorkerConfig.Roles.Producer,
            Timestamps = messageIds.Select(id => new MessageTimestamp
            {
                MessageId = id,
                TimestampTicks = baseTicks
            }).ToList()
        });

        _aggregator.SubmitTimestamps(new WorkerTimestampData
        {
            WorkerId = Guid.NewGuid(),
            Role = WorkerConfig.Roles.Consumer,
            Timestamps = messageIds.Select(id => new MessageTimestamp
            {
                MessageId = id,
                TimestampTicks = baseTicks + TimeSpan.FromMilliseconds(3).Ticks
            }).ToList(),
            ConsumerGroupIndex = 0
        });

        _aggregator.SubmitTimestamps(new WorkerTimestampData
        {
            WorkerId = Guid.NewGuid(),
            Role = WorkerConfig.Roles.Consumer,
            Timestamps = messageIds.Take(3).Select(id => new MessageTimestamp
            {
                MessageId = id,
                TimestampTicks = baseTicks + TimeSpan.FromMilliseconds(4).Ticks
            }).ToList(),
            ConsumerGroupIndex = 1
        });

        _aggregator.SetExpectedMessagesPerGroup(null, 2); // PubSub: null = all expect total

        // Act
        var results = _aggregator.ComputeResults();

        // Assert
        results.PerGroupResults[0].MessagesLost.Should().Be(0);
        results.PerGroupResults[1].MessagesLost.Should().Be(2); // expected 5 (total sent), got 3
    }

    [Fact]
    public void SubmitTimestamps_SameWorker_MergesData()
    {
        // Arrange
        var workerId = Guid.NewGuid();

        // Act
        _aggregator.SubmitTimestamps(new WorkerTimestampData
        {
            WorkerId = workerId,
            Role = WorkerConfig.Roles.Producer,
            Timestamps = [new MessageTimestamp { MessageId = Guid.NewGuid(), TimestampTicks = 100 }]
        });

        _aggregator.SubmitTimestamps(new WorkerTimestampData
        {
            WorkerId = workerId,
            Role = WorkerConfig.Roles.Producer,
            Timestamps = [new MessageTimestamp { MessageId = Guid.NewGuid(), TimestampTicks = 200 }]
        });

        // Assert
        _aggregator.WorkerCount.Should().Be(1); // same worker, merged
    }

    [Fact]
    public void Reset_ClearsAllState()
    {
        // Arrange
        _aggregator.SubmitTimestamps(new WorkerTimestampData
        {
            WorkerId = Guid.NewGuid(),
            Role = WorkerConfig.Roles.Producer,
            Timestamps = [new MessageTimestamp { MessageId = Guid.NewGuid(), TimestampTicks = 100 }]
        });

        // Act
        _aggregator.Reset();

        // Assert
        _aggregator.WorkerCount.Should().Be(0);
    }
}
