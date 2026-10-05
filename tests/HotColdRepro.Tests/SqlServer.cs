using Microsoft.Data.SqlClient;
using Shouldly;
using Testcontainers.MsSql;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace HotColdRepro.Tests;

[CollectionDefinition(Name)]
public sealed class SqlServerCollection : ICollectionFixture<SqlServer>
{
    public const string Name = "SQL Server";
}

/// <summary>
///     One SQL Server 2025 for the database tests, from Testcontainers unless REPRO_SQLSERVER points at an existing
///     server. Every test gets its own database, so application locks never cross tests.
/// </summary>
public sealed class SqlServer : IAsyncLifetime
{
    public const string DefaultImage = "mcr.microsoft.com/mssql/server:2025-latest";

    private readonly List<string> _databases = new();
    private MsSqlContainer? _container;
    private string _server = "";

    public async ValueTask InitializeAsync()
    {
        var existing = Environment.GetEnvironmentVariable("REPRO_SQLSERVER");
        if (!string.IsNullOrWhiteSpace(existing))
        {
            _server = existing;
            return;
        }

        _container = new MsSqlBuilder(Environment.GetEnvironmentVariable("REPRO_SQLSERVER_IMAGE") ?? DefaultImage)
            .Build();

        await _container.StartAsync();
        _server = _container.GetConnectionString();
    }

    public async ValueTask DisposeAsync()
    {
        if (_container != null)
        {
            await _container.DisposeAsync();
            return;
        }

        SqlConnection.ClearAllPools();
        await using var conn = new SqlConnection(_server);
        await conn.OpenAsync();

        foreach (var database in _databases)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}];";
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public async Task<string> CreateDatabaseAsync(string name)
    {
        var database = $"{name}_{Guid.NewGuid():N}";

        await using var conn = new SqlConnection(_server);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"CREATE DATABASE [{database}]";
        await cmd.ExecuteNonQueryAsync();
        _databases.Add(database);

        return new SqlConnectionStringBuilder(_server) { InitialCatalog = database }.ConnectionString;
    }

    public static string As(string connectionString, string applicationName) =>
        new SqlConnectionStringBuilder(connectionString) { ApplicationName = applicationName }.ConnectionString;

    /// <summary>
    ///     Ends the sessions holding application locks in the database, the way an Azure SQL failover, a gateway
    ///     reconfiguration or a dropped TCP connection does. SQL Server releases a session's locks with it.
    /// </summary>
    public static async Task KillLockSessionsAsync(string connectionString, int? lockId = null, string? applicationName = null)
    {
        var sessions = await LockSessionsAsync(connectionString, lockId, applicationName);
        sessions.ShouldNotBeEmpty("no session holds the application lock that was meant to be killed");

        await using var conn = new SqlConnection(NonPooled(connectionString));
        await conn.OpenAsync();

        foreach (var session in sessions)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"KILL {session}";
            await cmd.ExecuteNonQueryAsync();
        }

        (await Eventually.TrueWithinAsync(
                () => LockSessionsAsync(connectionString, lockId, applicationName).GetAwaiter().GetResult().Length == 0,
                TimeSpan.FromSeconds(10)))
            .ShouldBeTrue("the killed sessions still hold their application locks");
    }

    private static async Task<short[]> LockSessionsAsync(string connectionString, int? lockId, string? applicationName)
    {
        await using var conn = new SqlConnection(NonPooled(connectionString));
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          SELECT DISTINCT l.request_session_id
                          FROM sys.dm_tran_locks l
                          JOIN sys.dm_exec_sessions s ON s.session_id = l.request_session_id
                          WHERE l.resource_type = 'APPLICATION'
                            AND l.resource_database_id = DB_ID()
                            AND l.request_status = 'GRANT'
                            AND (@lock IS NULL OR CHARINDEX(':[' + @lock + ']:', l.resource_description) > 0)
                            AND (@app IS NULL OR s.program_name = @app)
                          """;
        cmd.Parameters.AddWithValue("@lock", (object?)lockId?.ToString() ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@app", (object?)applicationName ?? DBNull.Value);

        var sessions = new List<short>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            sessions.Add((short)reader.GetInt32(0));
        }

        return sessions.ToArray();
    }

    private static string NonPooled(string connectionString) =>
        new SqlConnectionStringBuilder(connectionString) { Pooling = false, ApplicationName = "repro-observer" }.ConnectionString;
}

public static class Eventually
{
    public static async Task<bool> TrueWithinAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return true;
            await Task.Delay(100);
        }

        return condition();
    }
}
