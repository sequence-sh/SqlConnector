using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CSharpFunctionalExtensions;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Attributes;
using Reductech.EDR.Core.Entities;
using Reductech.EDR.Core.Enums;
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Internal.Errors;
using Reductech.EDR.Core.Util;
using Entity = Reductech.EDR.Core.Entity;

namespace Reductech.EDR.Connectors.Sql.Steps
{

/// <summary>
/// Create a SQL table from a given schema
/// </summary>
public sealed class SqlCreateTable : CompoundStep<Unit>
{
    /// <inheritdoc />
    protected override async Task<Result<Unit, IError>> Run(
        IStateMonad stateMonad,
        CancellationToken cancellationToken)
    {
        var entity =
            await Schema.Run(stateMonad, cancellationToken);

        if (entity.IsFailure)
            return entity.ConvertFailure<Unit>();

        var databaseConnectionMetadata = await DatabaseConnectionMetadata.GetOrCreate(
            Connection,
            stateMonad,
            this,
            cancellationToken
        );

        if (databaseConnectionMetadata.IsFailure)
            return databaseConnectionMetadata.ConvertFailure<Unit>();

        var schema = EntityConversionHelpers.TryCreateFromEntity<Schema>(entity.Value)
            .MapError(x => x.WithLocation(this));

        if (schema.IsFailure)
            return schema.ConvertFailure<Unit>();

        var factory = stateMonad.ExternalContext
            .TryGetContext<IDbConnectionFactory>(DbConnectionFactory.DbConnectionName);

        if (factory.IsFailure)
            return factory.MapError(x => x.WithLocation(this)).ConvertFailure<Unit>();

        using IDbConnection conn =
            factory.Value.GetDatabaseConnection(databaseConnectionMetadata.Value);

        conn.Open();

        using var dbCommand = conn.CreateCommand();

        var setCommandResult = SetCommand(
            schema.Value,
            dbCommand,
            databaseConnectionMetadata.Value.DatabaseType
        );

        if (setCommandResult.IsFailure)
            return setCommandResult.MapError(x => x.WithLocation(this)).ConvertFailure<Unit>();

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
    /// The table to create a schema from
    /// </summary>
    [StepProperty(order: 1)]
    [Required]
    public IStep<Entity> Schema { get; set; } = null!;

    /// <summary>
    /// The Connection String
    /// </summary>
    [StepProperty(order: 2)]
    [DefaultValueExplanation("The Most Recent Connection")]
    public IStep<Entity>? Connection { get; set; } = null;

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<SqlCreateTable, Unit>();

    private static bool ShouldQuoteNames(DatabaseType databaseType)
    {
        return databaseType switch
        {
            DatabaseType.SQLite => true,
            DatabaseType.MsSql => true,
            DatabaseType.Postgres => true,
            DatabaseType.MySql => false,
            DatabaseType.MariaDb => false,
            _ => throw new ArgumentOutOfRangeException(nameof(databaseType), databaseType, null)
        };
    }

    private static Result<Unit, IErrorBuilder> SetCommand(
        Schema schema,
        IDbCommand command,
        DatabaseType databaseType)
    {
        var sb     = new StringBuilder();
        var errors = new List<IErrorBuilder>();

        if (schema.ExtraProperties == ExtraPropertyBehavior.Allow)
            errors.Add(
                ErrorCode_Sql.CouldNotCreateTable.ToErrorBuilder(
                    $"Schema has {nameof(schema.ExtraProperties)} set to 'Allow'"
                )
            );

        var quoteNames = ShouldQuoteNames(databaseType);

        var tableName = Extensions.CheckSqlObjectName(schema.Name);

        if (tableName.IsFailure)
            errors.Add(tableName.Error);
        else
            sb.AppendLine($"CREATE TABLE {Extensions.MaybeQuote(tableName.Value, quoteNames)} (");

        var index = 0;

        foreach (var (column, schemaProperty) in schema.Properties)
        {
            var dataType     = TypeConversion.TryGetDataType(schemaProperty.Type, databaseType);
            var multiplicity = TryGetMultiplicityString(schemaProperty.Multiplicity);

            if (dataType.IsFailure)
                errors.Add(dataType.Error);

            if (multiplicity.IsFailure)
                errors.Add(multiplicity.Error);

            if (dataType.IsSuccess && multiplicity.IsSuccess)
            {
                if (index > 0)
                    sb.Append(',');

                var columnName = Extensions.CheckSqlObjectName(column);

                if (columnName.IsFailure)
                    errors.Add(columnName.Error);
                else
                    sb.AppendLine(
                        $"{Extensions.MaybeQuote(columnName.Value, quoteNames)} {dataType.Value} {multiplicity.Value}"
                    );

                index++;
            }
        }

        sb.AppendLine(")");

        command.CommandText = sb.ToString();

        if (errors.Any())
            return Result.Failure<Unit, IErrorBuilder>(ErrorBuilderList.Combine(errors));

        return Unit.Default;

        static Result<string, IErrorBuilder> TryGetMultiplicityString(Multiplicity multiplicity)
        {
            return multiplicity switch
            {
                Multiplicity.Any => ErrorCode_Sql.CouldNotCreateTable.ToErrorBuilder(
                    $"Sql does not support Multiplicity '{multiplicity}'"
                ),
                Multiplicity.AtLeastOne => ErrorCode_Sql.CouldNotCreateTable.ToErrorBuilder(
                    $"Sql does not support Multiplicity '{multiplicity}'"
                ),
                Multiplicity.ExactlyOne => "NOT NULL",
                Multiplicity.UpToOne    => "NULL",
                _ => throw new ArgumentOutOfRangeException(
                    nameof(multiplicity),
                    multiplicity,
                    null
                )
            };
        }
    }
}

}
