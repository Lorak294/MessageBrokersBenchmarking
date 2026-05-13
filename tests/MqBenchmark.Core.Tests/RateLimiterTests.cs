using System.Diagnostics;
using FluentAssertions;
using MqBenchmark.Core.Metrics;
using Xunit;

namespace MqBenchmark.Core.Tests;

public class RateLimiterTests
{
    [Fact]
    public async Task WaitAsync_AtGivenRate_TakesExpectedTime()
    {
        // Arrange
        const int rate = 100; // 100 msg/s
        const int calls = 20;
        var limiter = new RateLimiter(rate);

        // Act
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < calls; i++)
            await limiter.WaitAsync();
        sw.Stop();

        // Assert — 20 calls at 100/s should take ~200ms. Allow generous tolerance.
        sw.Elapsed.TotalMilliseconds.Should().BeGreaterThan(150);
        sw.Elapsed.TotalMilliseconds.Should().BeLessThan(400);
    }

    [Fact]
    public async Task WaitAsync_VeryHighRate_AddsNegligibleDelay()
    {
        // Arrange
        const int rate = 1_000_000;
        var limiter = new RateLimiter(rate);

        // Act
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < 100; i++)
            await limiter.WaitAsync();
        sw.Stop();

        // Assert
        sw.Elapsed.TotalMilliseconds.Should().BeLessThan(200);
    }
}
