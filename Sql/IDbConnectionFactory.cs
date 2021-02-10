using System.Data;

namespace Reductech.EDR.Connectors.Sql
{

public interface IDbConnectionFactory
{
    public IDbConnection GetDatabaseConnection(DatabaseType databaseType, string connectionString);
}

}
