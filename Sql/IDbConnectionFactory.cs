using System.Data;
using Reductech.EDR.Connectors.Sql.Steps;

namespace Reductech.EDR.Connectors.Sql
{

public interface IDbConnectionFactory
{
    public IDbConnection GetDatabaseConnection(DatabaseConnectionMetadata connectionMetadata);
}

}
