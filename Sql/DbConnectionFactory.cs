using System.Data;
using System.Data.SQLite;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;

namespace Reductech.Sequence.Connectors.Sql;

/// <inheritdoc />
public class DbConnectionFactory : IDbConnectionFactory
{
    /// <summary>
    /// Name of the database connection.
    /// </summary>
    public const string DbConnectionName = "DbConnection";

    private DbConnectionFactory() { }

    /// <summary>
    /// THE instance
    /// </summary>
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
