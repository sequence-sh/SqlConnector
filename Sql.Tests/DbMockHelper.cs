using System.Data;
using Moq;

namespace Reductech.EDR.Connectors.Sql.Tests
{

public static class DbMockHelper
{
    public static Mock<IDbConnectionFactory> SetupConnectionFactory(
        MockRepository repository,
        DatabaseType databaseType,
        string connectionString,
        string expectedQuery,
        DataTable dataTable)
    {
        var factory    = repository.Create<IDbConnectionFactory>();
        var connection = repository.Create<IDbConnection>();
        var command    = repository.Create<IDbCommand>();

        var dataReader = new DataTableReader(dataTable);

        factory.Setup(f => f.GetDatabaseConnection(databaseType, connectionString))
            .Returns(connection.Object);

        connection.Setup(f => f.CreateCommand()).Returns(command.Object);

        connection.Setup(x => x.Open());
        command.SetupSet<string>(x => x.CommandText = expectedQuery);

        command.Setup(x => x.ExecuteReader()).Returns(dataReader);
        command.Setup(x => x.Dispose());
        connection.Setup(x => x.Dispose());

        return factory;
    }
}

}
