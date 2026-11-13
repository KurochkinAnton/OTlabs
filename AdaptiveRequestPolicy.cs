using System;
using System.Collections.Generic;
using RRC.Abstractions;

namespace RRC.Student;

/// <summary>
/// Adapts to recent response times and backs off when the server is slow.
/// One instance is shared by all requests in a simulation.
/// </summary>
public sealed class AdaptiveRequestPolicy : IRequestPolicy
{
    private const int WindowSize = 128;
    private const int WarmupSamples = 16;
    private const int MaximumAttempts = 3;
    private const double InitialTimeoutMs = 1000;
    // A small safety floor costs nothing for on-time replies and protects
    // against abrupt changes after a run of very fast responses.
    private const double MinimumTimeoutMs = 200;
    private const double CongestionThreshold = 0.20;

    private readonly double[] latencies = new double[WindowSize];
    private readonly double[] sortedLatencies = new double[WindowSize];
    private readonly Dictionary<long, double> retryTimeouts = new();
    private int sampleCount;
    private int nextSample;
    private double smoothedLatencyMs;
    private double deviationMs;
    private double estimatedTimeoutMs = InitialTimeoutMs;
    private double recoveryTimeoutMs;
    private double firstAttemptTimeoutRate;

    public TimeSpan GetTimeout(RequestContext context, Random random)
    {
        // No retry remains after the final attempt. Giving up early cannot
        // reduce server work, and late replies would be discarded forever.
        // Let this attempt use the caller's horizon (the simulation deadline).
        if (context.Attempt >= MaximumAttempts)
            return TimeSpan.MaxValue;

        double timeoutMs = Math.Max(estimatedTimeoutMs, recoveryTimeoutMs);

        // A retry gets a larger budget; repeatedly using the expired timeout
        // would create more work without letting slower attempts finish.
        int backoff = Math.Clamp(context.Attempt - 1, 0, MaximumAttempts - 1);
        timeoutMs *= 1 << backoff;
        if (retryTimeouts.TryGetValue(context.RequestId, out double retryMinimumMs))
            timeoutMs = Math.Max(timeoutMs, retryMinimumMs);
        return Duration(timeoutMs);
    }

    public RetryDecision OnResult(RequestObservation observation, Random random)
    {
        double elapsedMs = observation.Elapsed.TotalMilliseconds;
        if (observation.Attempt == 1)
            firstAttemptTimeoutRate = (31.0 / 32) * firstAttemptTimeoutRate
                + (observation.Outcome == RequestOutcome.Timeout ? 1.0 / 32 : 0);

        if (observation.Outcome == RequestOutcome.Success)
        {
            retryTimeouts.Remove(observation.RequestId);
            RecordSuccess(elapsedMs);
            // Recover gradually so a few fast responses do not immediately
            // undo the response to a burst of timeouts.
            recoveryTimeoutMs *= firstAttemptTimeoutRate >= CongestionThreshold ? 0.999 : 0.98;
            if (firstAttemptTimeoutRate < 0.02 && sampleCount >= WarmupSamples)
                recoveryTimeoutMs = Math.Min(recoveryTimeoutMs, 4 * estimatedTimeoutMs);
            return RetryDecision.NoRetry();
        }

        // A timed-out duration is only a LOWER bound on server latency.
        // Do not add it to the successful-latency distribution as an exact RTT.
        // Use max, not repeated multiplication: simultaneous old timeouts
        // should not increase the shared timeout exponentially.
        // Only first attempts describe the fleet-wide timeout rate. A single
        // repeatedly stalled request must not escalate every other timeout.
        if (observation.Attempt == 1)
        {
            double recoveryMs = 4 * elapsedMs;
            if (firstAttemptTimeoutRate < CongestionThreshold)
                recoveryMs = Math.Min(recoveryMs, 2 * estimatedTimeoutMs);
            recoveryTimeoutMs = Math.Max(recoveryTimeoutMs, recoveryMs);
        }

        if (observation.Attempt >= MaximumAttempts)
        {
            retryTimeouts.Remove(observation.RequestId);
            return RetryDecision.NoRetry();
        }

        // A sparse workload may provide no other observations after a change
        // of latency scale. Expand the INDIVIDUAL budget substantially.
        retryTimeouts[observation.RequestId] = Math.Max(16 * elapsedMs, 4 * InitialTimeoutMs);

        // The original attempt still runs on the server. Backoff and jitter
        // spread retries out rather than amplifying a burst of server load.
        double delayMs = Math.Clamp(elapsedMs * 0.25, 25, 2000);
        if (firstAttemptTimeoutRate >= CongestionThreshold)
            delayMs = Math.Max(delayMs, Math.Max(elapsedMs, smoothedLatencyMs));
        int backoff = Math.Clamp(observation.Attempt - 1, 0, MaximumAttempts - 1);
        delayMs *= (1 << backoff) * (0.5 + random.NextDouble());
        return RetryDecision.RetryAfter(Duration(delayMs, 1));
    }

    private void RecordSuccess(double elapsedMs)
    {
        if (sampleCount == 0)
        {
            smoothedLatencyMs = elapsedMs;
            deviationMs = elapsedMs / 2;
        }
        else
        {
            // Measure deviation against the PREVIOUS smoothed value.
            deviationMs = 0.75 * deviationMs
                + 0.25 * Math.Abs(elapsedMs - smoothedLatencyMs);
            smoothedLatencyMs += 0.125 * (elapsedMs - smoothedLatencyMs);
        }

        latencies[nextSample] = elapsedMs;
        nextSample = (nextSample + 1) % WindowSize;
        sampleCount = Math.Min(sampleCount + 1, WindowSize);

        Array.Copy(latencies, sortedLatencies, sampleCount);
        Array.Sort(sortedLatencies, 0, sampleCount);
        int percentileIndex = (int)Math.Ceiling(sampleCount * 0.98) - 1;
        double tailBudgetMs = sortedLatencies[percentileIndex] * 2;
        double variationBudgetMs = smoothedLatencyMs + 8 * deviationMs;
        estimatedTimeoutMs = Math.Max(MinimumTimeoutMs, Math.Max(tailBudgetMs, variationBudgetMs));

        // Avoid learning an aggressive timeout from just a few fast replies
        // while the slower cold-start requests have not returned yet.
        if (sampleCount < WarmupSamples)
            estimatedTimeoutMs = Math.Max(estimatedTimeoutMs, InitialTimeoutMs);
    }

    private static TimeSpan Duration(double milliseconds, double minimumMs = MinimumTimeoutMs)
    {
        // Saturate before conversion: double millisecond values close to
        // TimeSpan.MaxValue can otherwise round above the largest Int64 tick.
        decimal ticks = (decimal)Math.Max(milliseconds, minimumMs) * TimeSpan.TicksPerMillisecond;
        return TimeSpan.FromTicks((long)Math.Min(ticks, long.MaxValue));
    }
}
