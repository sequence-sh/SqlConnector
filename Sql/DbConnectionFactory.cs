using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;

namespace Reductech.EDR.Connectors.Sql
{

public class DbConnectionFactory : IDbConnectionFactory
{
    private DbConnectionFactory() { }
    public static IDbConnectionFactory Instance { get; } = new DbConnectionFactory();

    /// <inheritdoc />
    public IDbConnection GetDatabaseConnection(DatabaseType databaseType, string connectionString)
    {
        return databaseType switch
        {
            DatabaseType.SqlLite => new SQLiteConnection(connectionString),
            DatabaseType.Sql => new SqlConnection(connectionString),
            _ => throw new ArgumentOutOfRangeException(nameof(databaseType), databaseType, null)
        };
    }
}

}
