using System.Data;

namespace Reductech.EDR.Connectors.Sql
{

/// <summary>
/// A way of creating / retrieving database connections based on
/// connection metadata.
/// </summary>
public interface IDbConnectionFactory
{
    /// <summary>
    /// Create or retrieve a database connection based on the supplied connection metadata.
    /// </summary>
    public IDbConnection GetDatabaseConnection(DatabaseConnectionMetadata connectionMetadata);
}

}
