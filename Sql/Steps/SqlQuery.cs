using System;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using CSharpFunctionalExtensions;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Attributes;
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Internal.Errors;
using Entity = Reductech.EDR.Core.Entity;

namespace Reductech.EDR.Connectors.Sql.Steps
{

/// <summary>
/// Executes a SQL query and returns the result as an entity stream.
/// </summary>
public sealed class SqlQuery : CompoundStep<Array<Entity>>
{
    /// <inheritdoc />
    protected override async Task<Result<Array<Entity>, IError>> Run(
        IStateMonad stateMonad,
        CancellationToken cancellationToken)
    {
        var query = await
            Query.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (query.IsFailure)
            return query.ConvertFailure<Array<Entity>>();

        var connectionString =
            await ConnectionString.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (connectionString.IsFailure)
            return connectionString.ConvertFailure<Array<Entity>>();

        var databaseType = await DatabaseType.Run(stateMonad, cancellationToken);

        if (databaseType.IsFailure)
            return databaseType.ConvertFailure<Array<Entity>>();

        var factory = stateMonad.ExternalContext
            .TryGetContext<IDbConnectionFactory>(DbConnectionFactory.DbConnectionName);

        if (factory.IsFailure)
            return factory.MapError(x => x.WithLocation(this)).ConvertFailure<Array<Entity>>();

        IDbConnection conn = factory.Value.GetDatabaseConnection(
            databaseType.Value,
            connectionString.Value
        );

        conn.Open();

        var command = conn.CreateCommand();
        command.CommandText = query.Value;

        IDataReader dbReader;

        try
        {
            dbReader = command.ExecuteReader();
        }
        catch (Exception e)
        {
            command.Dispose();
            conn.Dispose();

            return Result.Failure<Array<Entity>, IError>(
                ErrorCode_Sql.SqlError.ToErrorBuilder(e.Message).WithLocation(this)
            );
        }

        var array = Extensions.GetEntityEnumerable(dbReader, command, conn, cancellationToken)
            .ToSCLArray();

        return array;
    }

    /// <summary>
    /// The Connection String
    /// </summary>
    [StepProperty(order: 1)]
    [Required]
    public IStep<StringStream> ConnectionString { get; set; } = null!;

    /// <summary>
    /// The Sql query to run
    /// </summary>
    [StepProperty(order: 2)]
    [Required]
    [Alias("Sql")]
    public IStep<StringStream> Query { get; set; } = null!;

    /// <summary>
    /// The Database Type to connect to
    /// </summary>
    [StepProperty(3)]
    [DefaultValueExplanation("Sql")]
    [Alias("DB")]
    public IStep<DatabaseType> DatabaseType { get; set; } =
        new EnumConstant<DatabaseType>(Sql.DatabaseType.MsSql);

    /// <inheritdoc />
    public override IStepFactory StepFactory => new SimpleStepFactory<SqlQuery, Array<Entity>>();
}

}
