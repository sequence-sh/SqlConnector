using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;
using CSharpFunctionalExtensions;
using MySqlConnector;
using Npgsql;
using Reductech.EDR.Connectors.Sql.Steps;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Connectors;
using Reductech.EDR.Core.Internal.Errors;

namespace Reductech.EDR.Connectors.Sql
{

public sealed class ConnectorInjection : IConnectorInjection
{
    /// <inheritdoc />
    public Result<IReadOnlyCollection<(string Name, object Context)>, IErrorBuilder>
        TryGetInjectedContexts(SCLSettings settings)
    {
        var list = new List<(string Name, object Context)>()
        {
            (DbConnectionFactory.DbConnectionName, DbConnectionFactory.Instance)
        };

        return list;
    }
}

public class DbConnectionFactory : IDbConnectionFactory
{
    public const string DbConnectionName = "DbConnection";

    private DbConnectionFactory() { }
    public static IDbConnectionFactory Instance { get; } = new DbConnectionFactory();

    /// <inheritdoc />
    public IDbConnection GetDatabaseConnection(DatabaseConnectionMetadata databaseConnection)
    {
        var connectionString = databaseConnection.ConnectionString;

        return databaseConnection.DatabaseType switch
        {
            DatabaseType.SQLite   => new SQLiteConnection(connectionString),
            DatabaseType.MsSql    => new SqlConnection(connectionString),
            DatabaseType.Postgres => new NpgsqlConnection(connectionString),
            DatabaseType.MySql    => new MySqlConnection(connectionString),
            DatabaseType.MariaDb  => new MySqlConnection(connectionString),
            _ => throw new ArgumentOutOfRangeException(
                nameof(databaseConnection.DatabaseType),
                databaseConnection,
                null
            )
        };
    }
}

}
