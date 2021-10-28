using System;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using CSharpFunctionalExtensions;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Internal.Errors;
using Entity = Reductech.EDR.Core.Entity;

namespace Reductech.EDR.Connectors.Sql
{

[Serializable]
public class DatabaseConnectionMetadata : IEntityConvertible
{
    public const string DatabaseConnectionKey1 = "ReductechDatabaseConnection";

    public static readonly VariableName DatabaseConnectionVariableName =
        new(DatabaseConnectionKey1);

    [JsonPropertyName("ConnectionString")] public string ConnectionString { get; set; } = null!;

    [JsonPropertyName("DatabaseType")] public DatabaseType DatabaseType { get; set; }

    public static async Task<Result<DatabaseConnectionMetadata, IError>> TrySetConnection(
        Entity dbConnectionEntity,
        IStateMonad stateMonad,
        IStep step)
    {
        var dbConnectionConversionResult =
            EntityConversionHelpers.TryCreateFromEntity<DatabaseConnectionMetadata>(
                dbConnectionEntity
            );

        if (dbConnectionConversionResult.IsFailure)
            return dbConnectionConversionResult.ConvertFailure<DatabaseConnectionMetadata>()
                .MapError(x => x.WithLocation(step));

        var result = await stateMonad.SetVariableAsync(
            DatabaseConnectionVariableName,
            dbConnectionEntity,
            true,
            step
        );

        if (result.IsFailure)
            return result.ConvertFailure<DatabaseConnectionMetadata>();

        return dbConnectionConversionResult.Value;
    }

    /// <summary>
    /// Get or create a SQL Connection
    /// </summary>
    public static async Task<Result<DatabaseConnectionMetadata, IError>> GetOrCreate(
        IStep<Entity>? connection,
        IStateMonad stateMonad,
        IStep callingStep,
        CancellationToken cancellationToken)
    {
        if (connection is null)
        {
            //Try to use pre-existing connection
            var v = stateMonad
                .GetVariable<Entity>(DatabaseConnectionVariableName)
                .Bind(EntityConversionHelpers.TryCreateFromEntity<DatabaseConnectionMetadata>)
                .MapError(x => x.WithLocation(callingStep));

            return v;
        }

        //Use the given connection

        var connectionR = await connection.Run(stateMonad, cancellationToken)
            .Bind(x => TrySetConnection(x, stateMonad, callingStep));

        return connectionR;
    }

    /// <summary>
    /// Gets a connection from the State Monad
    /// </summary>
    public static Result<DatabaseConnectionMetadata, IErrorBuilder>
        TryGetConnection(IStateMonad stateMonad)
    {
        var vR = stateMonad.GetVariable<Entity>(DatabaseConnectionVariableName)
            .Bind(EntityConversionHelpers.TryCreateFromEntity<DatabaseConnectionMetadata>);

        return vR;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj)
    {
        return obj is DatabaseConnectionMetadata metadata &&
               ConnectionString == metadata.ConnectionString &&
               DatabaseType == metadata.DatabaseType;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(ConnectionString, DatabaseType);
    }
}

}
