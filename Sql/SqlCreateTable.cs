using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CSharpFunctionalExtensions;
using Microsoft.SqlServer.Management.SqlParser.Metadata;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Attributes;
using Reductech.EDR.Core.Entities;
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Internal.Errors;
using Reductech.EDR.Core.Internal.Logging;
using Reductech.EDR.Core.Util;
using Entity = Reductech.EDR.Core.Entity;

namespace Reductech.EDR.Connectors.Sql
{

/// <summary>
/// Create a sql table from a given schema
/// </summary>
public sealed class SqlCreateTable : CompoundStep<Unit>
{
    /// <inheritdoc />
    protected override async Task<Result<Unit, IError>> Run(
        IStateMonad stateMonad,
        CancellationToken cancellationToken)
    {
        var connectionString =
            await ConnectionString.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (connectionString.IsFailure)
            return connectionString.ConvertFailure<Unit>();

        var entity =
            await Entity.Run(stateMonad, cancellationToken);

        if (entity.IsFailure)
            return entity.ConvertFailure<Unit>();

        var databaseType = await DatabaseType.Run(stateMonad, cancellationToken);

        if (databaseType.IsFailure)
            return databaseType.ConvertFailure<Unit>();

        var schema = Schema.TryCreateFromEntity(entity.Value).MapError(x => x.WithLocation(this));

        if (schema.IsFailure)
            return schema.ConvertFailure<Unit>();

        var factory = stateMonad.ExternalContext
            .TryGetContext<IDbConnectionFactory>(DbConnectionFactory.DbConnectionName);

        if (factory.IsFailure)
            return factory.MapError(x => x.WithLocation(this)).ConvertFailure<Unit>();

        using IDbConnection conn = factory.Value.GetDatabaseConnection(
            databaseType.Value,
            connectionString.Value
        );

        conn.Open();

        using var dbCommand = conn.CreateCommand();

        var setCommandResult = SetCommand(schema.Value, dbCommand);

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

        stateMonad.Logger.LogSituation(
            LogSituationSql.CommandExecuted,
            new object[] { rowsAffected }
        );

        return Unit.Default;
    }

    private Result<Unit, IErrorBuilder> SetCommand(Schema schema, IDbCommand command)
    {
        var sb = new StringBuilder();

        var errors = new List<IErrorBuilder>();

        sb.AppendLine($"CREATE TABLE {schema.Name} (");

        if (schema.AllowExtraProperties)
            errors.Add(
                ErrorCode_Sql.CouldNotCreateTable.ToErrorBuilder(
                    $"Schema has {nameof(schema.AllowExtraProperties)} set to true"
                )
            );

        var index = 0;

        foreach (var (column, schemaProperty) in schema.Properties)
        {
            var dataType     = TryGetDataType(schemaProperty.Type);
            var multiplicity = TryGetMultiplicityString(schemaProperty.Multiplicity);

            if (dataType.IsFailure)
                errors.Add(dataType.Error);

            if (multiplicity.IsFailure)
                errors.Add(multiplicity.Error);

            if (dataType.IsSuccess && multiplicity.IsSuccess)
            {
                if (index > 0)
                    sb.Append(",");

                sb.AppendLine(
                    $"{column} {dataType.Value.ToString().ToUpperInvariant()} {multiplicity.Value}"
                );

                index++;
            }
        }

        sb.AppendLine(")");

        command.CommandText = sb.ToString();

        if (errors.Any())
            return Result.Failure<Unit, IErrorBuilder>(ErrorBuilderList.Combine(errors));

        return Unit.Default;

        static Result<SqlDataType, IErrorBuilder> TryGetDataType(
            SchemaPropertyType schemaPropertyType)
        {
            return schemaPropertyType switch
            {
                SchemaPropertyType.String  => SqlDataType.NText,
                SchemaPropertyType.Integer => SqlDataType.Int,
                SchemaPropertyType.Double  => SqlDataType.Float,
                SchemaPropertyType.Enum    => SqlDataType.NText,
                SchemaPropertyType.Bool    => SqlDataType.Bit,
                SchemaPropertyType.Date    => SqlDataType.DateTime2,
                SchemaPropertyType.Entity => ErrorCode_Sql.CouldNotCreateTable.ToErrorBuilder(
                    $"Sql does not support nested entities"
                ),
                _ => throw new ArgumentOutOfRangeException(
                    nameof(schemaPropertyType),
                    schemaPropertyType,
                    null
                )
            };
        }

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
                Multiplicity.UpToOne => "NULL",
                _ => throw new ArgumentOutOfRangeException(nameof(multiplicity), multiplicity, null)
            };
        }
    }

    /// <summary>
    /// The Connection String
    /// </summary>
    [StepProperty(order: 1)]
    [Required]
    public IStep<StringStream> ConnectionString { get; set; } = null!;

    /// <summary>
    /// The table to create a schema from
    /// </summary>
    [StepProperty(order: 2)]
    [Required]
    public IStep<Entity> Entity { get; set; } = null!;

    [StepProperty(3)]
    [DefaultValueExplanation("Sql")]
    [Alias("DB")]
    public IStep<DatabaseType> DatabaseType { get; set; } =
        new EnumConstant<DatabaseType>(Sql.DatabaseType.MsSql);

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<SqlCreateTable, Unit>();
}

}
