using System.Diagnostics;

namespace MqBenchmark.Core.Metrics;

/// <summary>
/// Throttles operations to a target frequency using a hybrid approach:
/// sleeps for the bulk of the wait, then spin-waits for sub-millisecond precision.
/// Does not attempt to catch up after slow operations — resumes at normal pace.
/// Note: requires unrestricted CPU (no Docker CPU limits) for accurate timing.
/// </summary>
public class RateLimiter
{
    private readonly long _intervalTicks;
    private long _nextTimestamp;

    public RateLimiter(int messagesPerSecond)
    {
        _intervalTicks = Stopwatch.Frequency / messagesPerSecond;
        _nextTimestamp = Stopwatch.GetTimestamp();
    }

    public async Task WaitAsync()
    {
        var now = Stopwatch.GetTimestamp();
        var remaining = _nextTimestamp - now;

        if (remaining > 0)
        {
            var remainingMs = remaining * 1000 / Stopwatch.Frequency;
            if (remainingMs > 2)
                await Task.Delay((int)(remainingMs - 1));

            while (Stopwatch.GetTimestamp() < _nextTimestamp) { }
        }

        // If we fell behind, resume at normal pace from now
        var afterWait = Stopwatch.GetTimestamp();
        _nextTimestamp = afterWait > _nextTimestamp
            ? afterWait + _intervalTicks
            : _nextTimestamp + _intervalTicks;
    }
}
