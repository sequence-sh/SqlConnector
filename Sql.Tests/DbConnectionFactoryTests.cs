using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;
using FluentAssertions;
using MySqlConnector;
using Npgsql;
using Xunit.Abstractions;

namespace Reductech.EDR.Connectors.Sql.Tests
{

[AutoTheory.UseTestOutputHelper]
public partial class DbConnectionFactoryTests
{
    [AutoTheory.GenerateTheory("TestGetDatabaseConnection")]
    public IEnumerable<ConnectionFactoryTestCase> TestCases
    {
        get
        {
            yield return new ConnectionFactoryTestCase(
                "Server=Server;Database=Database;Integrated Security=true;",
                DatabaseType.MsSql,
                typeof(SqlConnection)
            );

            yield return new ConnectionFactoryTestCase(
                "Server=Server;Port=1234;Database=Database;Uid=UserName;Pwd=Password;",
                DatabaseType.MySql,
                typeof(MySqlConnection)
            );

            yield return new ConnectionFactoryTestCase(
                "Server=Server;Port=1234;Database=Database;Uid=UserName;Pwd=Password;",
                DatabaseType.MariaDb,
                typeof(MySqlConnection)
            );

            yield return new ConnectionFactoryTestCase(
                "Server=Server;Port=1234;Database=Database;Uid=UserName;Pwd=Password;",
                DatabaseType.Postgres,
                typeof(NpgsqlConnection)
            );

            yield return new ConnectionFactoryTestCase(
                "Data Source=InMemorySample;Mode=Memory;Cache=Shared",
                DatabaseType.SQLite,
                typeof(SQLiteConnection)
            );
        }
    }

    public record ConnectionFactoryTestCase(
        string ConnectionString,
        DatabaseType DatabaseType,
        Type ExpectedConnectionType) : AutoTheory.ITestInstance
    {
        /// <inheritdoc />
        public void Run(ITestOutputHelper testOutputHelper)
        {
            var connection = new DatabaseConnectionMetadata()
            {
                ConnectionString = ConnectionString, DatabaseType = DatabaseType
            };

            var result = DbConnectionFactory.Instance.GetDatabaseConnection(connection);

            result.Should().BeOfType(ExpectedConnectionType);
            result.ConnectionString.Should().Be(ConnectionString);
        }

        /// <inheritdoc />
        public string Name => DatabaseType.ToString();
    }
}

}
