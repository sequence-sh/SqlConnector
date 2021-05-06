using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;
using MySqlConnector;
using Npgsql;

namespace Reductech.EDR.Connectors.Sql
{

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
