using System.Data;
using Sequence.Core.Internal.Errors;
using Entity = Sequence.Core.Entity;

namespace Sequence.Connectors.Sql.Steps;

/// <summary>
/// Executes a Sql command
/// </summary>
public sealed class SqlCommand : CompoundStep<Unit>
{
    /// <inheritdoc />
    protected override async ValueTask<Result<Unit, IError>> Run(
        IStateMonad stateMonad,
        CancellationToken cancellationToken)
    {
        var command = await
            Command.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (command.IsFailure)
            return command.ConvertFailure<Unit>();

        var factory = stateMonad.ExternalContext
            .TryGetContext<IDbConnectionFactory>(DbConnectionFactory.DbConnectionName);

        if (factory.IsFailure)
            return factory.MapError(x => x.WithLocation(this)).ConvertFailure<Unit>();

        var databaseConnectionMetadata = await DatabaseConnectionMetadata.GetOrCreate(
            Connection,
            stateMonad,
            this,
            cancellationToken
        );

        if (databaseConnectionMetadata.IsFailure)
            return databaseConnectionMetadata.ConvertFailure<Unit>();

        using IDbConnection conn =
            factory.Value.GetDatabaseConnection(databaseConnectionMetadata.Value);

        conn.Open();

        using var dbCommand = conn.CreateCommand();
        dbCommand.CommandText = command.Value;

        int rowsAffected;

        try
        {
            rowsAffected = dbCommand.ExecuteNonQuery();
        }
        catch (Exception e)
        {
            return Result.Failure<Unit, IError>(
                ErrorCode_Sql.SqlError.ToErrorBuilder(e.Message).WithLocation(this)
            );
        }

        LogSituationSql.CommandExecuted.Log(stateMonad, this, rowsAffected);

        return Unit.Default;
    }

    /// <summary>
    /// The Sql command to run
    /// </summary>
    [StepProperty(order: 1)]
    [Required]
    [Alias("SQL")]
    public IStep<StringStream> Command { get; set; } = null!;

    /// <summary>
    /// The Connection String
    /// </summary>
    [StepProperty(order: 2)]
    [DefaultValueExplanation("The Most Recent Connection")]
    public IStep<Entity>? Connection { get; set; } = null;

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } = new SimpleStepFactory<SqlCommand, Unit>();
}
