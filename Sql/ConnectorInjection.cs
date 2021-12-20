using Reductech.Sequence.Core.Connectors;
using Reductech.Sequence.Core.Internal.Errors;

namespace Reductech.Sequence.Connectors.Sql;

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
