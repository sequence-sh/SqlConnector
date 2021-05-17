using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CSharpFunctionalExtensions;
using MoreLinq;
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
/// Inserts data into a SQL table
/// </summary>
public sealed class SqlInsert : CompoundStep<Unit>
{
    /// <inheritdoc />
    protected override async Task<Result<Unit, IError>> Run(
        IStateMonad stateMonad,
        CancellationToken cancellationToken)
    {
        var entities = await
            Entities.Run(stateMonad, cancellationToken);

        if (entities.IsFailure)
            return entities.ConvertFailure<Unit>();

        var databaseConnectionMetadata = await DatabaseConnectionMetadata.GetOrCreate(
            Connection,
            stateMonad,
            this,
            cancellationToken
        );

        if (databaseConnectionMetadata.IsFailure)
            return databaseConnectionMetadata.ConvertFailure<Unit>();

        string? postgresSchema = null;

        if (PostgresSchema is not null)
        {
            var s = await PostgresSchema.Run(stateMonad, cancellationToken)
                .Map(x => x.GetStringAsync())
                .Bind(x => Extensions.CheckSqlObjectName(x).MapError(e => e.WithLocation(this)));

            if (s.IsFailure)
                return s.ConvertFailure<Unit>();

            postgresSchema = s.Value;
        }

        var schema = await Schema.Run(stateMonad, cancellationToken)
            .Bind(
                x => EntityConversionHelpers.TryCreateFromEntity<Schema>(x)
                    .MapError(e => e.WithLocation(this))
            );

        if (schema.IsFailure)
            return schema.ConvertFailure<Unit>();

        var factory = stateMonad.ExternalContext
            .TryGetContext<IDbConnectionFactory>(DbConnectionFactory.DbConnectionName);

        if (factory.IsFailure)
            return factory.MapError(x => x.WithLocation(this)).ConvertFailure<Unit>();

        var elements = await entities.Value.GetElementsAsync(cancellationToken);

        if (elements.IsFailure)
            return elements.ConvertFailure<Unit>();

        IDbConnection conn = factory.Value.GetDatabaseConnection(databaseConnectionMetadata.Value);

        conn.Open();
        const int maxQueryParameters = 2099;
        var       batchSize          = maxQueryParameters / schema.Value.Properties.Count;

        var batches = elements.Value.Batch(batchSize);

        var shouldQuoteFieldNames =
            ShouldQuoteFieldNames(databaseConnectionMetadata.Value.DatabaseType);

        foreach (var batch in batches)
        {
            var commandDataResult = GetCommandData(
                schema.Value,
                postgresSchema,
                shouldQuoteFieldNames,
                batch,
                stateMonad
            );

            if (commandDataResult.IsFailure)
                return commandDataResult.MapError(x => x.WithLocation(this)).ConvertFailure<Unit>();

            using var dbCommand = conn.CreateCommand();

            commandDataResult.Value.SetCommand(dbCommand);

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
        }

        return Unit.Default;
    }

    /// <summary>
    /// The entities to insert
    /// </summary>
    [StepProperty(order: 1)]
    [Required]
    [Alias("Sql")]
    public IStep<Array<Entity>> Entities { get; set; } = null!;

    /// <summary>
    /// The schema that the data must match
    /// </summary>
    [StepProperty(order: 2)]
    [Required]
    public IStep<Entity> Schema { get; set; } = null!;

    /// <summary>
    /// The schema this table belongs to, if postgres
    /// </summary>
    [StepProperty(3)]
    [DefaultValueExplanation("No schema")]
    public IStep<StringStream>? PostgresSchema { get; set; } = null;

    /// <summary>
    /// The Connection String
    /// </summary>
    [StepProperty(order: 4)]
    [DefaultValueExplanation("The Most Recent Connection")]
    public IStep<Entity>? Connection { get; set; } = null;

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<SqlInsert, Unit>();

    private static bool ShouldQuoteFieldNames(DatabaseType databaseType)
    {
        return databaseType switch
        {
            DatabaseType.SQLite => false,
            DatabaseType.MsSql => false,
            DatabaseType.Postgres => true,
            DatabaseType.MySql => false,
            DatabaseType.MariaDb => false,
            _ => throw new ArgumentOutOfRangeException(nameof(databaseType), databaseType, null)
        };
    }

    public record CommandData(
        string CommandText,
        IReadOnlyList<(string Parameter, object Value, DbType DbType)> Parameters)
    {
        public void SetCommand(IDbCommand command)
        {
            command.CommandText = CommandText;

            foreach (var (parameter, value, dbType) in Parameters)
            {
                command.AddParameter(parameter, value, dbType);
            }
        }
    }

    private Result<CommandData, IErrorBuilder> GetCommandData(
        Schema schema,
        string? postgresSchemaName,
        bool quoteFieldNames,
        IEnumerable<Entity> entities,
        IStateMonad stateMonad)
    {
        var stringBuilder = new StringBuilder();
        var errors        = new List<IErrorBuilder>();

        var tableName = Extensions.CheckSqlObjectName(schema.Name);

        if (tableName.IsFailure)
            return tableName.ConvertFailure<CommandData>();

        if (string.IsNullOrWhiteSpace(postgresSchemaName))
            stringBuilder.Append($"INSERT INTO {tableName.Value} (");
        else
            stringBuilder.Append($"INSERT INTO {postgresSchemaName}.\"{tableName.Value}\" (");

        var first = true;

        foreach (var (name, _) in schema.Properties)
        {
            if (!first)
                stringBuilder.Append(", ");

            var columnName = Extensions.CheckSqlObjectName(name);

            if (columnName.IsFailure)
                errors.Add(columnName.Error);
            else
            {
                stringBuilder.Append($"{Extensions.MaybeQuote(columnName.Value, quoteFieldNames)}");
                first = false;
            }
        }

        stringBuilder.AppendLine(")");
        stringBuilder.Append("VALUES");
        var i = 0;

        var parameters = new List<(string Parameter, object Value, DbType DbType)>();

        foreach (var entity in entities)
        {
            if (i > 0)
            {
                stringBuilder.Append(", ");
            }

            var entity2 = schema.ApplyToEntity(entity, this, stateMonad, ErrorBehavior.Fail);

            if (entity2.IsFailure)
            {
                errors.Add(entity2.Error);
                continue;
            }

            if (entity2.Value.HasNoValue) { continue; }

            stringBuilder.Append('(');
            first = true;

            foreach (var (name, _) in schema.Properties)
            {
                if (!first)
                    stringBuilder.Append(", ");

                var ev = entity2.Value.Value.TryGetValue(name);

                var val = GetValue(ev, name);

                var valueKey = $"v{i}";

                if (val.IsFailure)
                    errors.Add(val.Error);
                else
                    parameters.Add((valueKey!, val.Value.o!, val.Value.dbType));

                stringBuilder.Append($"@{valueKey}");
                first = false;
                i++;
            }

            stringBuilder.AppendLine(")");
        }

        if (errors.Any())
        {
            var list = ErrorBuilderList.Combine(errors);
            return Result.Failure<CommandData, IErrorBuilder>(list);
        }

        return new CommandData(stringBuilder.ToString(), parameters);

        static Result<(object? o, DbType dbType), IErrorBuilder> GetValue(
            Maybe<EntityValue> entityValue,
            string columnName)
        {
            if (entityValue.HasNoValue)
                return (null, DbType.String);

            return entityValue.Value switch
            {
                EntityValue.Boolean boolean => (boolean.Value, DbType.Boolean),
                EntityValue.Date date       => (date.Value, DbType.DateTime2),
                EntityValue.Double d        => (d.Value, DbType.Double),
                EntityValue.EnumerationValue enumerationValue => (
                    enumerationValue.Value.ToString(), DbType.String),
                EntityValue.Integer integer => (integer.Value, DbType.Int32),
                EntityValue.NestedEntity => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                    nameof(Entity),
                    columnName
                ),
                EntityValue.NestedList => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                    "Array",
                    columnName
                ),
                EntityValue.Null     => (null, DbType.String),
                EntityValue.String s => (s.Value, DbType.String),
                _                    => throw new ArgumentOutOfRangeException()
            };
        }
    }
}

}
