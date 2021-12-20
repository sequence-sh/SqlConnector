using System.Data;
using Reductech.Sequence.Core.Internal.Errors;
using Entity = Reductech.Sequence.Core.Entity;

namespace Reductech.Sequence.Connectors.Sql.Steps;

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

        var databaseConnectionMetadata = await DatabaseConnectionMetadata.GetOrCreate(
            Connection,
            stateMonad,
            this,
            cancellationToken
        );

        if (databaseConnectionMetadata.IsFailure)
            return databaseConnectionMetadata.ConvertFailure<Array<Entity>>();

        var factory = stateMonad.ExternalContext
            .TryGetContext<IDbConnectionFactory>(DbConnectionFactory.DbConnectionName);

        if (factory.IsFailure)
            return factory.MapError(x => x.WithLocation(this)).ConvertFailure<Array<Entity>>();

        IDbConnection conn = factory.Value.GetDatabaseConnection(databaseConnectionMetadata.Value);

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
    /// The Sql query to run
    /// </summary>
    [StepProperty(order: 1)]
    [Required]
    [Alias("Sql")]
    public IStep<StringStream> Query { get; set; } = null!;

    /// <summary>
    /// The Connection String
    /// </summary>
    [StepProperty(order: 2)]
    [DefaultValueExplanation("The Most Recent Connection")]
    public IStep<Entity>? Connection { get; set; } = null;

    /// <inheritdoc />
    public override IStepFactory StepFactory => new SimpleStepFactory<SqlQuery, Array<Entity>>();
}
