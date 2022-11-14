using Sequence.Core.Connectors;
using Sequence.Core.Internal.Errors;

namespace Sequence.Connectors.Sql;

/// <inheritdoc />
public sealed class ConnectorInjection : IConnectorInjection
{
    /// <inheritdoc />
    public Result<IReadOnlyCollection<(string Name, object Context)>, IErrorBuilder>
        TryGetInjectedContexts()
    {
        var list = new List<(string Name, object Context)>()
        {
            (DbConnectionFactory.DbConnectionName, DbConnectionFactory.Instance)
        };

        return list;
    }
}
