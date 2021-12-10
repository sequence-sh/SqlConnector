using System.Data;
using Moq;

namespace Reductech.EDR.Connectors.Sql.Tests;

public static class DbMockHelper
{
    public static Mock<IDbConnectionFactory> SetupConnectionFactoryForQuery(
        MockRepository repository,
        DatabaseType databaseType,
        string connectionString,
        string expectedQuery,
        DataTable dataTable)
    {
        var dataReader = new DataTableReader(dataTable);

        var factory =
            SetupFactory(
                repository,
                databaseType,
                connectionString,
                expectedQuery,
                cm => cm.Setup(x => x.ExecuteReader()).Returns(dataReader)
            );

        return factory;
    }

    public static Mock<IDbConnectionFactory> SetupConnectionFactoryErrorForQuery(
        MockRepository repository,
        DatabaseType databaseType,
        string connectionString,
        string expectedQuery,
        Exception exception)
    {
        var factory =
            SetupFactory(
                repository,
                databaseType,
                connectionString,
                expectedQuery,
                cm => cm.Setup(x => x.ExecuteReader()).Throws(exception)
            );

        return factory;
    }

    public static Mock<IDbConnectionFactory> SetupConnectionFactoryErrorForCommand(
        MockRepository repository,
        DatabaseType databaseType,
        string connectionString,
        string expectedQuery,
        Exception exception)
    {
        var factory =
            SetupFactory(
                repository,
                databaseType,
                connectionString,
                expectedQuery,
                cm => cm.Setup(x => x.ExecuteNonQuery()).Throws(exception)
            );

        return factory;
    }

    public static Mock<IDbConnectionFactory> SetupConnectionFactoryForCommand(
        MockRepository repository,
        DatabaseType databaseType,
        string connectionString,
        string expectedQuery,
        int rowsAffected)
    {
        var factory =
            SetupFactory(
                repository,
                databaseType,
                connectionString,
                expectedQuery,
                cm => cm.Setup(x => x.ExecuteNonQuery()).Returns(rowsAffected)
            );

        return factory;
    }

    public static Mock<IDbConnectionFactory> SetupConnectionFactoryForScalarQuery(
        MockRepository repository,
        DatabaseType databaseType,
        string connectionString,
        string expectedQuery,
        string result)
    {
        var factory =
            SetupFactory(
                repository,
                databaseType,
                connectionString,
                expectedQuery,
                cm => cm.Setup(x => x.ExecuteScalar()).Returns(result)
            );

        return factory;
    }

    public static Mock<IDbConnectionFactory> SetupFactoryLoose(
        MockRepository repository,
        DatabaseType databaseType,
        string connectionString)
    {
        var factory    = repository.Create<IDbConnectionFactory>();
        var connection = repository.Create<IDbConnection>();
        var command    = repository.Create<IDbCommand>();

        command.Setup(x => x.Dispose());
        connection.Setup(x => x.Dispose());

        return factory;
    }

    private static Mock<IDbConnectionFactory> SetupFactory(
        MockRepository repository,
        DatabaseType databaseType,
        string connectionString,
        string expectedQuery,
        Action<Mock<IDbCommand>> setupCommand)
    {
        var factory    = repository.Create<IDbConnectionFactory>();
        var connection = repository.Create<IDbConnection>();
        var command    = repository.Create<IDbCommand>();

        factory.Setup(
                f => f.GetDatabaseConnection(
                    new DatabaseConnectionMetadata()
                    {
                        ConnectionString = connectionString, DatabaseType = databaseType
                    }
                )
            )
            .Returns(connection.Object);

        connection.Setup(f => f.CreateCommand()).Returns(command.Object);

        var query = Core.TestHarness.SpaceCompressor.CompressSpaces(expectedQuery);

        connection.Setup(x => x.Open());

        command.SetupSet<string>(
            x => x.CommandText =
                It.Is<string>(y => Core.TestHarness.SpaceCompressor.CompressSpaces(y).Equals(query))
        );

        command.Setup(x => x.Dispose());
        connection.Setup(x => x.Dispose());

        setupCommand(command);

        command.Setup(x => x.Dispose());
        connection.Setup(x => x.Dispose());

        return factory;
    }
}
