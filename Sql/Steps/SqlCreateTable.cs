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
using MySqlConnector;
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
        var connectionString =
            await ConnectionString.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (connectionString.IsFailure)
            return connectionString.ConvertFailure<Unit>();

        var entity =
            await Schema.Run(stateMonad, cancellationToken);

        if (entity.IsFailure)
            return entity.ConvertFailure<Unit>();

        var databaseType = await DatabaseType.Run(stateMonad, cancellationToken);

        if (databaseType.IsFailure)
            return databaseType.ConvertFailure<Unit>();

        var schema = EntityConversionHelpers.TryCreateFromEntity<Schema>(entity.Value)
            .MapError(x => x.WithLocation(this));

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

        var setCommandResult = SetCommand(schema.Value, dbCommand, databaseType.Value);

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

    private static bool ShouldQuoteNames(DatabaseType databaseType)
    {
        return databaseType switch
        {
            Sql.DatabaseType.SQLite => true,
            Sql.DatabaseType.MsSql => true,
            Sql.DatabaseType.Postgres => true,
            Sql.DatabaseType.MySql => false,
            Sql.DatabaseType.MariaDb => false,
            _ => throw new ArgumentOutOfRangeException(nameof(databaseType), databaseType, null)
        };
    }

    private static Result<Unit, IErrorBuilder> SetCommand(
        Schema schema,
        IDbCommand command,
        DatabaseType databaseType)
    {
        var sb = new StringBuilder();

        var errors = new List<IErrorBuilder>();

        var quoteNames = ShouldQuoteNames(databaseType);

        var tableName = Extensions.CheckSqlObjectName(schema.Name);

        if (tableName.IsFailure)
            errors.Add(tableName.Error);
        else if (quoteNames)
            sb.AppendLine($"CREATE TABLE \"{tableName.Value}\" (");
        else
            sb.AppendLine($"CREATE TABLE {tableName.Value} (");

        if (schema.ExtraProperties == ExtraPropertyBehavior.Allow)
            errors.Add(
                ErrorCode_Sql.CouldNotCreateTable.ToErrorBuilder(
                    $"Schema has {nameof(schema.ExtraProperties)} set to 'Allow'"
                )
            );

        var index = 0;

        foreach (var (column, schemaProperty) in schema.Properties)
        {
            var dataType     = TryGetDataType(schemaProperty.Type, databaseType);
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
                else if (quoteNames)
                    sb.AppendLine($"\"{columnName.Value}\" {dataType.Value} {multiplicity.Value}");
                else
                    sb.AppendLine($"{columnName.Value} {dataType.Value} {multiplicity.Value}");

                index++;
            }
        }

        sb.AppendLine(")");

        command.CommandText = sb.ToString();

        if (errors.Any())
            return Result.Failure<Unit, IErrorBuilder>(ErrorBuilderList.Combine(errors));

        return Unit.Default;

        static Result<string, IErrorBuilder> TryGetDataType(
            SCLType schemaPropertyType,
            DatabaseType databaseType)
        {
            switch (databaseType)
            {
                case Sql.DatabaseType.Postgres:
                {
                    return schemaPropertyType switch
                    {
                        SCLType.String  => "text",
                        SCLType.Integer => "integer",
                        SCLType.Double  => "double precision",
                        SCLType.Enum    => "text",
                        SCLType.Bool    => "boolean",
                        SCLType.Date    => "date",
                        SCLType.Entity => ErrorCode_Sql.CouldNotCreateTable
                            .ToErrorBuilder($"Sql does not support nested entities"),
                        _ => throw new ArgumentOutOfRangeException(
                            nameof(schemaPropertyType),
                            schemaPropertyType,
                            null
                        )
                    };
                }

                case Sql.DatabaseType.SQLite:
                case Sql.DatabaseType.MySql:
                {
                    Result<SqlDataType, IErrorBuilder> sqlDbType = schemaPropertyType switch
                    {
                        SCLType.String  => SqlDataType.NText,
                        SCLType.Integer => SqlDataType.Int,
                        SCLType.Double  => SqlDataType.Float,
                        SCLType.Enum    => SqlDataType.NText,
                        SCLType.Bool    => SqlDataType.Bit,
                        SCLType.Date    => SqlDataType.DateTime2,
                        SCLType.Entity => ErrorCode_Sql.CouldNotCreateTable
                            .ToErrorBuilder($"Sql does not support nested entities"),
                        _ => throw new ArgumentOutOfRangeException(
                            nameof(schemaPropertyType),
                            schemaPropertyType,
                            null
                        )
                    };

                    return sqlDbType.Map(x => x.ToString().ToUpperInvariant());
                }
                case Sql.DatabaseType.MsSql:
                case Sql.DatabaseType.MariaDb:
                {
                    Result<MySqlDbType, IErrorBuilder> sqlDbType = schemaPropertyType switch
                    {
                        SCLType.String  => MySqlDbType.Text,
                        SCLType.Integer => MySqlDbType.Int32,
                        SCLType.Double  => MySqlDbType.Float,
                        SCLType.Enum    => MySqlDbType.Text,
                        SCLType.Bool    => MySqlDbType.Bit,
                        SCLType.Date    => MySqlDbType.DateTime,
                        SCLType.Entity => ErrorCode_Sql.CouldNotCreateTable
                            .ToErrorBuilder($"Sql does not support nested entities"),
                        _ => throw new ArgumentOutOfRangeException(
                            nameof(schemaPropertyType),
                            schemaPropertyType,
                            null
                        )
                    };

                    return sqlDbType.Map(
                        x => x == MySqlDbType.Int32 ? "INT" : x.ToString().ToUpperInvariant()
                    );
                }
                default:
                    throw new ArgumentOutOfRangeException(nameof(databaseType), databaseType, null);
            }
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
                Multiplicity.UpToOne    => "NULL",
                _ => throw new ArgumentOutOfRangeException(
                    nameof(multiplicity),
                    multiplicity,
                    null
                )
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
    public IStep<Entity> Schema { get; set; } = null!;

    /// <summary>
    /// The Database Type to connect to
    /// </summary>
    [StepProperty(3)]
    [DefaultValueExplanation("SQL")]
    [Alias("DB")]
    public IStep<DatabaseType> DatabaseType { get; set; } =
        new EnumConstant<DatabaseType>(Sql.DatabaseType.MsSql);

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<SqlCreateTable, Unit>();
}

}
