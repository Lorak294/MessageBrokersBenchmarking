using FluentAssertions;
using MqBenchmark.Core.Config;
using Xunit;

namespace MqBenchmark.Worker.Tests;

public class RoundRobinRoutingIteratorTests
{
    [Fact]
    public void SingleTarget_ExhaustsCorrectly()
    {
        // Arrange
        var plan = new RoutingPlan
        {
            Targets = [new RoutingTarget { Target = "group_0", MessageCount = 3 }]
        };
        var iterator = new Worker.RoundRobinRoutingIterator(plan);

        // Act & Assert
        iterator.Next().Should().Be("group_0");
        iterator.Next().Should().Be("group_0");
        iterator.Next().Should().Be("group_0");
        // After exhaustion, falls back to first
        iterator.Next().Should().Be("group_0");
    }

    [Fact]
    public void MultipleTargets_RoundRobins()
    {
        // Arrange
        var plan = new RoutingPlan
        {
            Targets =
            [
                new RoutingTarget { Target = "group_0", MessageCount = 2 },
                new RoutingTarget { Target = "group_1", MessageCount = 2 }
            ]
        };
        var iterator = new Worker.RoundRobinRoutingIterator(plan);

        // Act & Assert
        iterator.Next().Should().Be("group_0");
        iterator.Next().Should().Be("group_1");
        iterator.Next().Should().Be("group_0");
        iterator.Next().Should().Be("group_1");
    }

    [Fact]
    public void AllExhausted_FallsBackToFirstTarget()
    {
        // Arrange
        var plan = new RoutingPlan
        {
            Targets =
            [
                new RoutingTarget { Target = "group_0", MessageCount = 1 },
                new RoutingTarget { Target = "group_1", MessageCount = 1 }
            ]
        };
        var iterator = new Worker.RoundRobinRoutingIterator(plan);

        // Act & Assert
        iterator.Next().Should().Be("group_0");
        iterator.Next().Should().Be("group_1");
        // Both exhausted
        iterator.Next().Should().Be("group_0"); // fallback
    }
}
