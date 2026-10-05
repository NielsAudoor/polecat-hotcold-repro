using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Threading.Channels;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polecat;
using Polecat.Subscriptions;
using Shouldly;

namespace HotColdRepro.Tests;

/// <summary>
///     The two defects end to end, through Polecat's public API: two HotCold nodes in one database, and one daemon
///     stopping a subscription that lags behind.
/// </summary>
[Collection(SqlServerCollection.Name)]
public class PolecatHotColdTests(SqlServer sql)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task only_one_hotcold_node_runs_the_subscription_after_the_lock_holders_sql_session_ends()
    {
        var database = await sql.CreateDatabaseAsync("hotcold");
        var ledger = new Ledger();
        await AppendAsync(database, 1);

        await using var appender = Appender.Start(database, eventsPerSave: 1, TimeSpan.FromMilliseconds(50));

        await using var nodeA = await HotColdNode.StartAsync(database, "nodeA", ledger);
        (await Eventually.TrueWithinAsync(() => ledger.PagesBy("nodeA") > 0, TimeSpan.FromSeconds(30)))
            .ShouldBeTrue("nodeA never started the subscription");

        await using var nodeB = await HotColdNode.StartAsync(database, "nodeB", ledger);
        var bothRunning = DateTimeOffset.UtcNow;
        await Task.Delay(TimeSpan.FromSeconds(3), Ct);
        ledger.NodesSince(bothRunning).ShouldBe(["nodeA"],
            customMessage: "before the kill only the lock holder should run the subscription");

        await SqlServer.KillLockSessionsAsync(database, applicationName: "nodeA");

        // Generous for a correct implementation: noticing the lost session, one coordinator poll, and one bounded drain
        await Task.Delay(TimeSpan.FromSeconds(15), Ct);
        var observedFrom = DateTimeOffset.UtcNow;
        await Task.Delay(TimeSpan.FromSeconds(10), Ct);
        var running = ledger.NodesSince(observedFrom);

        running.ShouldNotBeEmpty(
            "No node processed the subscription 15 to 25 s after nodeA's SQL session ended: nobody took the shard over.");

        running.Length.ShouldBe(1,
            $"Split brain: {string.Join(" and ", running)} processed the subscription 15 to 25 s after nodeA's SQL " +
            "session ended. nodeA never noticed that SQL Server released its sp_getapplock, and nodeB took the released " +
            "lock, so both nodes run the same shard and both write its progression.");
    }

    [Fact]
    public async Task stopping_a_subscription_that_lags_behind_is_bounded_by_StopAndDrainTimeout()
    {
        var database = await sql.CreateDatabaseAsync("drain");
        var subscription = new SlowSubscription();
        var drainTimeout = TimeSpan.FromSeconds(2);

        await AppendAsync(database, 1);

        await using var store = new DocumentStore(StoreOptionsFor(database, options =>
        {
            options.Projections.Subscribe(subscription);
            options.Projections.StopAndDrainTimeout = drainTimeout;
        }));

        using var daemon = await store.BuildProjectionDaemonAsync();

        await using (Appender.Start(database, eventsPerSave: 20, TimeSpan.FromMilliseconds(100)))
        {
            await daemon.StartAgentAsync("SlowSubscription:All", Ct);

            // Every high water mark update loads another page of 10 while the subscription needs 500 ms for one,
            // so the loader runs further and further ahead: a node under load whose subscription lags behind
            await Task.Delay(TimeSpan.FromSeconds(10), Ct);
        }

        // With the high water mark settled, the only load left is the one each completed page triggers, which
        // finishes long before the next page does. Stopping mid-page means no load is in flight.
        await Task.Delay(TimeSpan.FromSeconds(2), Ct);
        await subscription.NextPageStartedAsync();
        await Task.Delay(TimeSpan.FromMilliseconds(200), Ct);

        var pagesBefore = subscription.Pages;
        var stopwatch = Stopwatch.StartNew();
        await daemon.StopAgentAsync("SlowSubscription:All");
        stopwatch.Stop();
        var pagesAfterStop = subscription.Pages - pagesBefore;

        await daemon.StopAllAsync();

        stopwatch.Elapsed.ShouldBeLessThan(drainTimeout + TimeSpan.FromSeconds(1),
            string.Create(CultureInfo.InvariantCulture,
                $"StopAgentAsync took {stopwatch.Elapsed.TotalSeconds:F1} s with Projections.StopAndDrainTimeout = {drainTimeout.TotalSeconds} s, ") +
            $"and the subscription processed {pagesAfterStop} more pages after the stop was requested. The drain runs every " +
            "page the loader had already queued and never looks at its token, so a node that lost its lock keeps working " +
            "a shard another node owns, and a host shutdown can outlast its termination grace period.");

        pagesAfterStop.ShouldBeLessThanOrEqualTo(1,
            $"{pagesAfterStop} pages were processed after StopAgentAsync was called; only the page in flight should finish.");
    }

    internal static StoreOptions StoreOptionsFor(string connectionString, Action<StoreOptions>? configure = null)
    {
        var options = new StoreOptions
        {
            ConnectionString = connectionString,
            DatabaseSchemaName = "repro",
            AutoCreateSchemaObjects = AutoCreate.CreateOrUpdate,
        };

        options.Events.StreamIdentity = StreamIdentity.AsGuid;
        configure?.Invoke(options);
        return options;
    }

    internal static async Task AppendAsync(string database, int count)
    {
        await using var store = new DocumentStore(StoreOptionsFor(database));
        await using var session = store.LightweightSession();

        for (var i = 0; i < count; i++)
        {
            session.Events.StartStream(Guid.NewGuid(), new Pinged(i));
        }

        await session.SaveChangesAsync();
    }
}

public sealed record Pinged(int Number);

/// <summary>
///     Appends events in the background until disposed, which keeps the high water mark moving the way production load does.
/// </summary>
public sealed class Appender : IAsyncDisposable
{
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _running;

    private Appender(string database, int eventsPerSave, TimeSpan interval) =>
        _running = Task.Run(() => RunAsync(database, eventsPerSave, interval, _stop.Token));

    public static Appender Start(string database, int eventsPerSave, TimeSpan interval) => new(database, eventsPerSave, interval);

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        await _running;
        _stop.Dispose();
    }

    private static async Task RunAsync(string database, int eventsPerSave, TimeSpan interval, CancellationToken token)
    {
        await using var store = new DocumentStore(PolecatHotColdTests.StoreOptionsFor(database));
        var number = 0;

        while (!token.IsCancellationRequested)
        {
            await using var session = store.LightweightSession();
            for (var i = 0; i < eventsPerSave; i++)
            {
                session.Events.StartStream(Guid.NewGuid(), new Pinged(number++));
            }

            await session.SaveChangesAsync(CancellationToken.None);

            try
            {
                await Task.Delay(interval, token);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }
}

public sealed class Ledger
{
    private readonly ConcurrentQueue<(string Node, DateTimeOffset At)> _pages = new();

    public void Record(string node) => _pages.Enqueue((node, DateTimeOffset.UtcNow));

    public int PagesBy(string node) => _pages.Count(x => x.Node == node);

    public string[] NodesSince(DateTimeOffset since) =>
        _pages.Where(x => x.At >= since).Select(x => x.Node).Distinct().Order().ToArray();
}

public sealed class LedgerSubscription : SubscriptionBase
{
    private readonly string _node;
    private readonly Ledger _ledger;

    public LedgerSubscription(string node, Ledger ledger)
    {
        _node = node;
        _ledger = ledger;
        IncludeType(typeof(Pinged));
    }

    public override Task<IChangeListener> ProcessEventsAsync(EventRange page, ISubscriptionController controller,
        IDocumentOperations operations, CancellationToken cancellationToken)
    {
        if (page.Events.Count > 0)
        {
            _ledger.Record(_node);
        }

        return Task.FromResult<IChangeListener>(NullChangeListener.Instance);
    }
}

public sealed class SlowSubscription : SubscriptionBase
{
    private readonly Channel<int> _started = Channel.CreateUnbounded<int>();
    private int _pages;

    public SlowSubscription()
    {
        IncludeType(typeof(Pinged));
        Options.BatchSize = 10;
    }

    public int Pages => Volatile.Read(ref _pages);

    public async Task NextPageStartedAsync()
    {
        while (_started.Reader.TryRead(out _))
        {
        }

        await _started.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
    }

    public override async Task<IChangeListener> ProcessEventsAsync(EventRange page, ISubscriptionController controller,
        IDocumentOperations operations, CancellationToken cancellationToken)
    {
        _started.Writer.TryWrite(page.Events.Count);
        await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
        Interlocked.Increment(ref _pages);
        return NullChangeListener.Instance;
    }
}

public sealed class HotColdNode : IAsyncDisposable
{
    private readonly IHost _host;

    private HotColdNode(IHost host) => _host = host;

    public static async Task<HotColdNode> StartAsync(string database, string node, Ledger ledger)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();

        builder.Services.AddPolecat(_ => PolecatHotColdTests.StoreOptionsFor(SqlServer.As(database, node), options =>
            {
                options.Projections.Subscribe(new LedgerSubscription(node, ledger));
                options.DaemonSettings.LeadershipPollingTime = 1000;
            }))
            .AddProjectionCoordinator(DaemonMode.HotCold);

        var host = builder.Build();
        await host.StartAsync();
        return new HotColdNode(host);
    }

    public async ValueTask DisposeAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await _host.StopAsync(timeout.Token);
        _host.Dispose();
    }
}
