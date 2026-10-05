using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Weasel.SqlServer;

namespace HotColdRepro.Tests;

/// <summary>
///     Weasel.SqlServer.AdvisoryLock is what Polecat's HotCold ProjectionCoordinator elects shard owners with: one
///     session-owned sp_getapplock per shard, all on one long-lived SqlConnection per node.
/// </summary>
[Collection(SqlServerCollection.Name)]
public class WeaselSqlServerAdvisoryLockTests(SqlServer sql)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task a_node_holding_a_lock_stops_reporting_it_once_sql_server_ends_its_session()
    {
        var database = await sql.CreateDatabaseAsync("lock_holder");
        await using var nodeA = Node(database, "nodeA");
        await using var nodeB = Node(database, "nodeB");

        (await nodeA.TryAttainLockAsync(4201, Ct)).ShouldBeTrue();

        await SqlServer.KillLockSessionsAsync(database, lockId: 4201);

        (await nodeB.TryAttainLockAsync(4201, Ct)).ShouldBeTrue(
            "SQL Server should have released lock 4201 with nodeA's session");

        (await Eventually.TrueWithinAsync(() => !nodeA.HasLock(4201), TimeSpan.FromSeconds(20))).ShouldBeTrue(
            "Split brain: 20 s after nodeA's session ended, nodeA.HasLock(4201) is still true while nodeB holds lock 4201. " +
            "A node that holds every lock it wants never sends another command on its lock connection, so " +
            "SqlConnection.State stays Open and nothing ever tells it the server released the lock.");
    }

    [Fact]
    public async Task a_node_that_reconnects_does_not_keep_reporting_a_lock_it_lost_with_its_old_session()
    {
        var database = await sql.CreateDatabaseAsync("lock_poller");
        await using var nodeA = Node(database, "nodeA");
        await using var nodeB = Node(database, "nodeB");

        (await nodeA.TryAttainLockAsync(4301, Ct)).ShouldBeTrue();

        await SqlServer.KillLockSessionsAsync(database, lockId: 4301);

        // What the coordinator does every polling cycle for a shard it does not own yet
        await AttainEventuallyAsync(nodeA, 4302);

        (await nodeB.TryAttainLockAsync(4301, Ct)).ShouldBeTrue(
            "SQL Server should have released lock 4301 with nodeA's session");

        nodeA.HasLock(4301).ShouldBeFalse(
            "Split brain: after reconnecting, nodeA.HasLock(4301) is still true although that lock died with its " +
            "previous session and nodeB holds it now. TryAttainLockAsync disposes and nulls the broken connection " +
            "but never clears the list of locks that were held on it, and HasLock answers from that list.");
    }

    private static AdvisoryLock Node(string database, string node) =>
        new(() => new SqlConnection(SqlServer.As(database, node)), NullLogger.Instance, node);

    private static async Task AttainEventuallyAsync(AdvisoryLock theLock, int lockId)
    {
        for (var attempt = 0; attempt < 3; attempt++)
        {
            try
            {
                if (await theLock.TryAttainLockAsync(lockId, Ct)) return;
            }
            catch (SqlException)
            {
                // The first attempt after the kill is the one that discovers the dead connection
            }
        }

        throw new Exception($"Lock {lockId} was never attained after the session was killed");
    }
}
