using FluentAssertions;
using MqBenchmark.Core.Config;
using MqBenchmark.Core.Metrics;
using Xunit;

namespace MqBenchmark.Core.Tests;

public class TimestampBatchTransferTests
{
    [Fact]
    public void CompressAndDecompress_SingleBatch_RoundTripsCorrectly()
    {
        // Arrange
        var data = CreateTimestampData(10);

        // Act
        var batches = TimestampBatchTransfer.CompressBatches(data, batchSize: 100);

        // Assert
        batches.Should().HaveCount(1);
        var restored = TimestampBatchTransfer.DecompressBatch(batches[0]);
        restored.WorkerId.Should().Be(data.WorkerId);
        restored.Role.Should().Be(data.Role);
        restored.Timestamps.Should().HaveCount(10);
        restored.Timestamps.Select(t => t.MessageId).Should().BeEquivalentTo(data.Timestamps.Select(t => t.MessageId));
    }

    [Fact]
    public void CompressAndDecompress_MultipleBatches_SplitsCorrectlyAndAllRecoverable()
    {
        // Arrange
        var data = CreateTimestampData(12);

        // Act
        var batches = TimestampBatchTransfer.CompressBatches(data, batchSize: 5);

        // Assert
        batches.Should().HaveCount(3); // ceil(12/5) = 3
        batches.Select(b => b.BatchIndex).Should().BeEquivalentTo([0, 1, 2]);
        batches.Should().AllSatisfy(b => b.TotalBatches.Should().Be(3));

        var allRestored = batches.SelectMany(b => TimestampBatchTransfer.DecompressBatch(b).Timestamps).ToList();
        allRestored.Should().HaveCount(12);
        allRestored.Select(t => t.MessageId).Should().BeEquivalentTo(data.Timestamps.Select(t => t.MessageId));
    }

    [Fact]
    public void CompressAndDecompress_EmptyTimestamps_ProducesOneBatch()
    {
        // Arrange
        var data = CreateTimestampData(0);

        // Act
        var batches = TimestampBatchTransfer.CompressBatches(data, batchSize: 100);

        // Assert
        batches.Should().HaveCount(1);
        var restored = TimestampBatchTransfer.DecompressBatch(batches[0]);
        restored.Timestamps.Should().BeEmpty();
    }

    [Fact]
    public void CompressBatches_DefaultBatchSize_SplitsLargeData()
    {
        // Arrange
        var data = CreateTimestampData(12_000);

        // Act
        var batches = TimestampBatchTransfer.CompressBatches(data); // default 5000

        // Assert
        batches.Should().HaveCount(3); // ceil(12000/5000) = 3
    }

    private static WorkerTimestampData CreateTimestampData(int count)
    {
        return new WorkerTimestampData
        {
            WorkerId = Guid.NewGuid(),
            Role = WorkerConfig.Roles.Producer,
            Timestamps = Enumerable.Range(0, count).Select(_ => new MessageTimestamp
            {
                MessageId = Guid.NewGuid(),
                TimestampTicks = DateTime.UtcNow.Ticks
            }).ToList()
        };
    }
}
