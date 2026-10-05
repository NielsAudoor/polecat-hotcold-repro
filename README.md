# Polecat HotCold: split brain after a lost SQL session

A runnable reproduction of two defects behind duplicate processing in Polecat's HotCold projection coordinator:

1. **Weasel**: `Weasel.SqlServer.AdvisoryLock` keeps reporting locks that died with its SQL session. Once the lock
   holder's session ends, a second node takes the lock while the first keeps running the shard, indefinitely.
2. **JasperFx**: `SubscriptionExecutionBase.StopAndDrainAsync` executes the whole queued backlog and ignores its
   `StopAndDrainTimeout` token. A node that gives a lagging subscription up keeps processing it for as long as its
   backlog lasts, which stretches the overlap after a lock loss and slows every graceful shutdown.

## Run it

Requirements: the .NET 10 SDK and Docker.

```bash
dotnet test
```

Testcontainers starts `mcr.microsoft.com/mssql/server:2025-latest` (Polecat needs SQL Server 2025). The run takes
about two minutes. The JasperFx tests do not need a database, so
`dotnet test --filter "FullyQualifiedName~JasperFx"` runs them without Docker.

To use a different image, set `REPRO_SQLSERVER_IMAGE`. To use an existing SQL Server 2025 or Azure SQL instead of a
container, set `REPRO_SQLSERVER` to a connection string for a login that is `sysadmin`, or that has
`CREATE ANY DATABASE`, `ALTER ANY CONNECTION` (for `KILL`) and `VIEW SERVER STATE` (to find the lock holders). Each
test creates its own database, and the run drops them again.

## What fails

Every test asserts the **correct** behaviour, so on the current releases (Polecat 5.36.0, Weasel 9.41.0,
JasperFx 2.80.2) all six fail, each with a message explaining why.

| Test | Owner | What happens on the current releases |
|---|---|---|
| `WeaselSqlServerAdvisoryLockTests.a_node_holding_a_lock_stops_reporting_it_once_sql_server_ends_its_session` | Weasel | 20 s after its session is killed, node A still reports `HasLock` while node B holds the lock. |
| `WeaselSqlServerAdvisoryLockTests.a_node_that_reconnects_does_not_keep_reporting_a_lock_it_lost_with_its_old_session` | Weasel | After reconnecting to poll for another lock, node A reports a lock that died with its old session and that node B now holds. |
| `JasperFxSubscriptionDrainTests.stop_and_drain_honours_its_cancellation_token` | JasperFx | `StopAndDrainAsync` is still running 5 s after its token was cancelled. |
| `JasperFxSubscriptionDrainTests.stop_and_drain_does_not_execute_the_ranges_still_queued` | JasperFx | After the stop, the queued ranges are executed too. |
| `PolecatHotColdTests.only_one_hotcold_node_runs_the_subscription_after_the_lock_holders_sql_session_ends` | Weasel, end to end | Two HotCold nodes in one database. After the lock holder's session is killed, both nodes keep processing the subscription. |
| `PolecatHotColdTests.stopping_a_subscription_that_lags_behind_is_bounded_by_StopAndDrainTimeout` | JasperFx, end to end | With `Projections.StopAndDrainTimeout = 2 s`, `StopAgentAsync` takes several seconds and processes every page the loader had queued. |

## How it shows up in production

A service running several replicas with `AddProjectionCoordinator(DaemonMode.HotCold)` against Azure SQL. When the
lock holder's session goes away (a failover, a gateway reconfiguration, a dropped TCP connection), the logs show,
on the other replicas only:

```
fail: Polecat.Events.Daemon.Coordination.ProjectionCoordinator
      Error trying to attain a lock for set OutboxPublisher:All and lock id 372425480. Will retry later
      Microsoft.Data.SqlClient.SqlException: The connection is broken and recovery is not possible.
      The connection is marked by the server as unrecoverable.
```

and on shutdown:

```
fail: Weasel.SqlServer.AdvisoryLock
      Error trying to dispose of advisory locks for database Polecat
      Microsoft.Data.SqlClient.SqlException: Cannot release the application lock (Database Principal: 'public',
      Resource: '372425480') because it is not currently held.
```

What the logs do not show is that the subscription is now running on two or three nodes at once. Polecat writes
progression without a floor check, so nothing throws: every page is simply processed more than once. In a
three-replica load test with three killed lock sessions, 22,000 outbox events were processed by more than one node
within three minutes, and the subscription was still running on all three replicas when they were stopped.

## The two defects

### 1. `Weasel.SqlServer.AdvisoryLock`: locks outlive their session

Every lock is a session-owned `sp_getapplock` on one shared `SqlConnection`. When SQL Server ends that session it
releases all of the locks, but `AdvisoryLock` does not notice:

- **A node that holds every lock it wants never sends another command on the connection.** `HasLock` rejects only a
  `Closed` connection, and `SqlConnection.State` stays `Open` until the next I/O, so it answers `true` for as long as
  the process lives. Another node takes the released lock and both run the shard.
- **A node that polls for a lock it lacks** gets a `SqlException` on the dead connection. On the next call
  `TryAttainLockAsync` disposes and nulls the broken connection, but never clears its list of held locks. Whether
  `_conn` is null or a fresh connection opened for another lock, `HasLock` answers from that stale list, so the node
  reports locks that died with the old session.

### 2. `JasperFx.Events`: a subscription's drain is unbounded

`DaemonSettings.StopAndDrainTimeout` (jasperfx#564) documents the drain as waiting for a shard "to gracefully finish
its in-flight page and flush its progression", and says that "exceeding this bound cancels the drain mid-flight".
For subscriptions, neither holds. `SubscriptionExecutionBase.StopAndDrainAsync(token)` awaits
`_executionBlock.WaitForCompletionAsync()` without the token. `Block<T>.WaitForCompletionAsync()` completes the
block, waits for the item in flight, then **executes every queued item itself**. `StopAndDrainTimeoutTests` covers
#564 with a substituted execution (`GatedDrainExecution`), so the real `SubscriptionExecutionBase` path was never
under test.

A subscription that lags behind under load has many pages queued, so:

- a node that lost its distribution lock keeps processing pages another node already owns, for as long as its backlog
  lasts (34 s in the three-replica load test), and
- a graceful shutdown waits for the whole backlog (27 s in that load test), close to a container platform's 30 s
  termination grace period.

## Verifying a fix

`verify-fixes.sh` packs local checkouts of Weasel and JasperFx, with a version that ranks just above the release they
are based on, into a private feed and package cache. It then reruns the same tests against them:

```bash
bash verify-fixes.sh <path to a weasel checkout> <path to a jasperfx checkout>
```

Use Git Bash or WSL on Windows. The checkouts must be based on at least the releases Polecat 5.36.0 depends on
(Weasel 9.41.0, JasperFx 2.80.2); the script checks this.

Proposed fixes, both based on those releases:

- Weasel: [NielsAudoor/weasel `sqlserver-advisory-lock-session-loss`](https://github.com/NielsAudoor/weasel/tree/sqlserver-advisory-lock-session-loss).
  Locks are dropped together with their connection. `HasLock` needs a live connection. A default-on monitor probes
  `APPLOCK_MODE` while locks are held. An acquire whose connection died retries once on a fresh one, and returns
  `false` rather than throwing when the database cannot be reached. The lock connection is not pooled, so disposing
  it always ends the session.
- JasperFx: [NielsAudoor/jasperfx `subscription-drain-honours-timeout`](https://github.com/NielsAudoor/jasperfx/tree/subscription-drain-honours-timeout).
  The drain finishes the page in flight, skips the queued ones and honours its token. A range that completes after a
  timed-out drain no longer reports itself to the agent as completed (`MarkSuccessAsync`), so a stopped shard is not
  published as running again.

With both branches all six tests pass.

## Related, not covered here

- Polecat's `RecordProgressionOperation` writes progression without a floor check, so two nodes running one shard
  never fail loudly; Marten raises `ProgressionProgressOutOfOrderException` in the same situation.
- `ProjectionExecution` and `GroupedProjectionExecution` share the drain pattern of defect 2, so async projections
  have the same unbounded drain.
- `StoreOptions.DaemonSettings.StopAndDrainTimeout` compiles and is silently ignored: Polecat's daemon reads
  `StoreOptions.Projections.StopAndDrainTimeout`.
