# polecat-hotcold-repro

Reproduces duplicate processing with Polecat's HotCold projection coordinator on SQL Server. We hit it load testing a
service with several replicas on Azure SQL. There are two bugs:

- Weasel: `Weasel.SqlServer.AdvisoryLock` keeps reporting locks after SQL Server has ended the session that held
  them. Another node takes the lock and both run the shard.
- JasperFx: `SubscriptionExecutionBase.StopAndDrainAsync` ignores its token and runs the whole queued backlog, so a
  node that lost a subscription keeps processing it.

## Run

Needs the .NET 10 SDK and Docker.

```bash
dotnet test
```

Testcontainers starts `mcr.microsoft.com/mssql/server:2025-latest`, and a run takes about two minutes. The JasperFx
tests don't need a database: `dotnet test --filter "FullyQualifiedName~JasperFx"`.

`REPRO_SQLSERVER_IMAGE` overrides the image. `REPRO_SQLSERVER` takes a connection string to use an existing server
instead; the login needs sysadmin, or `CREATE ANY DATABASE`, `ALTER ANY CONNECTION` and `VIEW SERVER STATE`.

## Tests

The tests assert the correct behaviour, so all six fail on Polecat 5.36.0, Weasel 9.41.0 and JasperFx 2.80.2.

| Test | Bug | Fails because |
|---|---|---|
| `a_node_holding_a_lock_stops_reporting_it_once_sql_server_ends_its_session` | Weasel | 20 s after its session was killed, node A still reports `HasLock` while node B holds the lock |
| `a_node_that_reconnects_does_not_keep_reporting_a_lock_it_lost_with_its_old_session` | Weasel | after reconnecting, node A still reports a lock from its old session |
| `stop_and_drain_honours_its_cancellation_token` | JasperFx | `StopAndDrainAsync` is still running 5 s after its token was cancelled |
| `stop_and_drain_does_not_execute_the_ranges_still_queued` | JasperFx | the queued ranges run after the stop |
| `only_one_hotcold_node_runs_the_subscription_after_the_lock_holders_sql_session_ends` | Weasel, via Polecat | both nodes keep processing the subscription |
| `stopping_a_subscription_that_lags_behind_is_bounded_by_StopAndDrainTimeout` | JasperFx, via Polecat | `StopAgentAsync` takes well over the 2 s `StopAndDrainTimeout` and processes every queued page |

## Cause

### Weasel

Every lock is a session-scoped `sp_getapplock` on one shared `SqlConnection`. When SQL Server ends that session it
releases all of them, but `AdvisoryLock` doesn't notice:

- A node that already holds every lock it wants never uses the connection again. `SqlConnection.State` stays `Open`
  until the next I/O, so `HasLock` keeps returning true.
- A node polling for another lock hits the broken connection and reconnects, but never clears its list of held locks.

### JasperFx

`StopAndDrainAsync(token)` awaits `Block<T>.WaitForCompletionAsync()` without the token, and that method executes
every queued item. `StopAndDrainTimeoutTests` uses a substitute execution, so this path isn't tested.

## In production

When the lock holder's session drops (failover, gateway reconfiguration, a dropped TCP connection), the other
replicas log:

```
fail: Polecat.Events.Daemon.Coordination.ProjectionCoordinator
      Error trying to attain a lock for set OutboxPublisher:All and lock id 372425480. Will retry later
      Microsoft.Data.SqlClient.SqlException: The connection is broken and recovery is not possible.
```

Polecat writes progression without a floor check, so the double processing itself never throws. In a three-replica
load test, 22,000 outbox events were processed more than once within three minutes. A lagging node kept draining for
34 s after it lost its lock, and a graceful shutdown took 27 s, against a 30 s termination grace period.

## Verifying a fix

```bash
bash verify-fixes.sh <weasel checkout> <jasperfx checkout>
```

This packs both checkouts into a local feed, versioned just above the release they're based on, and reruns the
tests. On Windows, use Git Bash or WSL. The checkouts must be based on Weasel 9.41.0 and JasperFx 2.80.2 or later.

Proposed fixes; all six tests pass with both:

- Weasel, [`sqlserver-advisory-lock-session-loss`](https://github.com/NielsAudoor/weasel/tree/sqlserver-advisory-lock-session-loss):
  locks are dropped with their session, `HasLock` needs a live connection, and while locks are held a probe checks
  `APPLOCK_MODE` every 5 s. Polling for a lock no longer waits, and taking a lock that's already held doesn't stack it.
- JasperFx, [`subscription-drain-honours-timeout`](https://github.com/NielsAudoor/jasperfx/tree/subscription-drain-honours-timeout):
  the drain finishes the range in flight, skips the queued ones and honours its token. A range that finishes after a
  timed-out drain no longer marks progress.

## Not covered

- Polecat's progression has no floor check; Marten throws `ProgressionProgressOutOfOrderException` in the same case.
- `ProjectionExecution` and `GroupedProjectionExecution` drain the same way, so async projections have the same
  problem.
- Polecat ignores `DaemonSettings.StopAndDrainTimeout`; it reads `Projections.StopAndDrainTimeout`.
