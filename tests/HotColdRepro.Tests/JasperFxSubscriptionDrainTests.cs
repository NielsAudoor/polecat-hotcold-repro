using System.Collections.Concurrent;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Shouldly;

namespace HotColdRepro.Tests;

/// <summary>
///     SubscriptionExecutionBase.StopAndDrainAsync is what the daemon calls when a node gives a subscription shard up,
///     because it lost the distribution lock or because the host is stopping. The daemon hands it a token bounded by
///     DaemonSettings.StopAndDrainTimeout.
/// </summary>
public class JasperFxSubscriptionDrainTests
{
    private static readonly TimeSpan Generous = TimeSpan.FromSeconds(5);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task stop_and_drain_honours_its_cancellation_token()
    {
        var execution = new GatedExecution();
        var agent = Substitute.For<ISubscriptionAgent>();

        await execution.EnqueueAsync(new EventPage(0), agent);
        await execution.Started.Reader.ReadAsync(Ct).AsTask().WaitAsync(Generous, Ct);

        using var drainTimeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        var stop = execution.StopAndDrainAsync(drainTimeout.Token);

        var returned = await Task.WhenAny(stop, Task.Delay(Generous, Ct)) == stop;
        execution.ReleaseAll();

        returned.ShouldBeTrue(
            "StopAndDrainAsync was handed a token that cancelled after 0.5 s and is still running 5 s later: it awaits " +
            "_executionBlock.WaitForCompletionAsync() without the token, so DaemonSettings.StopAndDrainTimeout never " +
            "bounds a subscription's drain.");
    }

    [Fact]
    public async Task stop_and_drain_does_not_execute_the_ranges_still_queued()
    {
        var execution = new GatedExecution();
        var agent = Substitute.For<ISubscriptionAgent>();

        await execution.EnqueueAsync(new EventPage(0), agent);
        await execution.EnqueueAsync(new EventPage(100), agent);
        await execution.EnqueueAsync(new EventPage(200), agent);
        await execution.Started.Reader.ReadAsync(Ct).AsTask().WaitAsync(Generous, Ct);

        var stop = execution.StopAndDrainAsync(CancellationToken.None);
        execution.ReleaseAll();
        await stop.WaitAsync(Generous, Ct);

        execution.Executed.ToArray().ShouldBe([0L],
            customMessage: "After StopAndDrainAsync was called the execution went on to run the ranges that were still " +
                           "queued: Block<T>.WaitForCompletionAsync executes every queued item. A node that lost its " +
                           "distribution lock keeps processing pages another node now owns, for as long as its backlog lasts.");
    }

    // Every range waits until released, so the first one is in flight when the drain starts
    private sealed class GatedExecution() : SubscriptionExecutionBase(Substitute.For<IEventDatabase>(),
        new ShardName("Gated"), NullLogger.Instance)
    {
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public System.Threading.Channels.Channel<long> Started { get; } =
            System.Threading.Channels.Channel.CreateUnbounded<long>();

        public ConcurrentQueue<long> Executed { get; } = new();

        public void ReleaseAll() => _released.TrySetResult();

        protected override async Task executeRangeAsync(IEventDatabase database, EventRange range,
            ShardExecutionMode mode, CancellationToken cancellationToken)
        {
            Executed.Enqueue(range.SequenceFloor);
            Started.Writer.TryWrite(range.SequenceFloor);

            await _released.Task.WaitAsync(cancellationToken);
        }
    }
}
