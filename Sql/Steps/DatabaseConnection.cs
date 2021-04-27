using System;
using System.Threading.Tasks;
using CSharpFunctionalExtensions;
using Newtonsoft.Json;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Internal.Errors;
using Reductech.EDR.Core.Util;
using Entity = Reductech.EDR.Core.Entity;

namespace Reductech.EDR.Connectors.Sql.Steps
{

[Serializable]
public class DatabaseConnection : IEntityConvertible
{
    public const string DatabaseConnectionKey = "ReductechDatabaseConnection";

    [JsonProperty] public string ConnectionString { get; set; } = null!;

    [JsonProperty] public DatabaseType DatabaseType { get; set; }

    public static async Task<Result<Unit, IError>> TrySetConnection(
        Entity dbConnectionEntity,
        IStateMonad stateMonad,
        IStep step)
    {
        var dbConnectionConversionResult =
            EntityConversionHelpers.TryCreateFromEntity<DatabaseConnection>(dbConnectionEntity);

        if (dbConnectionConversionResult.IsFailure)
            return dbConnectionConversionResult.ConvertFailure<Unit>()
                .MapError(x => x.WithLocation(step));

        var result = await stateMonad.SetVariableAsync(
            new VariableName(DatabaseConnectionKey),
            dbConnectionEntity,
            true,
            step
        );

        return result;
    }

    public static Result<DatabaseConnection, IErrorBuilder>
        TryGetConnection(IStateMonad stateMonad)
    {
        var vR = stateMonad.GetVariable<Entity>(new VariableName(DatabaseConnectionKey))
            .Bind(EntityConversionHelpers.TryCreateFromEntity<DatabaseConnection>);

        return vR;
    }
}

}
