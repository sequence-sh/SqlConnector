namespace Reductech.EDR.Connectors.Sql;

/// <summary>
/// The type of database to connect to
/// </summary>
public enum DatabaseType
{
    /// <summary>
    /// SQLite
    /// </summary>
    SQLite,

    /// <summary>
    /// MsSql
    /// </summary>
    MsSql,

    /// <summary>
    /// Postgres
    /// </summary>
    Postgres,

    /// <summary>
    /// MySql
    /// </summary>
    MySql,

    /// <summary>
    /// MariaDb
    /// </summary>
    MariaDb
}
