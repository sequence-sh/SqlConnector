using System.Collections.Generic;
using CSharpFunctionalExtensions;
using Reductech.EDR.Core.Connectors;
using Reductech.EDR.Core.Internal.Errors;

namespace Reductech.EDR.Connectors.Sql
{

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

}
