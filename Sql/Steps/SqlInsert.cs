﻿using System;
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
/// Inserts data into a SQL table
/// </summary>
public sealed class SqlInsert : CompoundStep<Unit>
{
    /// <inheritdoc />
    protected override async Task<Result<Unit, IError>> Run(
        IStateMonad stateMonad,
        CancellationToken cancellationToken)
    {
        var connectionString =
            await ConnectionString.Run(stateMonad, cancellationToken)
                .Map(x => x.GetStringAsync());

        if (connectionString.IsFailure)
            return connectionString.ConvertFailure<Unit>();

        var entities = await
            Entities.Run(stateMonad, cancellationToken);

        if (entities.IsFailure)
            return entities.ConvertFailure<Unit>();

        var databaseType = await DatabaseType.Run(stateMonad, cancellationToken);

        if (databaseType.IsFailure)
            return databaseType.ConvertFailure<Unit>();

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

        IDbConnection conn = factory.Value.GetDatabaseConnection(
            databaseType.Value,
            connectionString.Value
        );

        conn.Open();
        const int maxQueryParameters = 2099;
        var       batchSize          = maxQueryParameters / schema.Value.Properties.Count;

        var batches = MoreLinq.MoreEnumerable.Batch(elements.Value, batchSize);

        var shouldQuoteFieldNames = ShouldQuoteFieldNames(databaseType.Value);

        foreach (var batch in batches)
        {
            using var dbCommand = conn.CreateCommand();

            var setCommandResult = SetCommand(
                schema.Value,
                postgresSchema,
                shouldQuoteFieldNames,
                batch,
                dbCommand,
                stateMonad
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
        }

        return Unit.Default;
    }

    private static bool ShouldQuoteFieldNames(DatabaseType databaseType)
    {
        return databaseType switch
        {
            Sql.DatabaseType.SQLite => false,
            Sql.DatabaseType.MsSql => false,
            Sql.DatabaseType.Postgres => true,
            Sql.DatabaseType.MySql => false,
            Sql.DatabaseType.MariaDb => false,
            _ => throw new ArgumentOutOfRangeException(nameof(databaseType), databaseType, null)
        };
    }

    private Result<Unit, IErrorBuilder> SetCommand(
        Schema schema,
        string? postgresSchemaName,
        bool quoteFieldNames,
        IEnumerable<Entity> entities,
        IDbCommand command,
        IStateMonad stateMonad)
    {
        var stringBuilder = new StringBuilder();
        var errors        = new List<IErrorBuilder>();

        var tableName = Extensions.CheckSqlObjectName(schema.Name);

        if (tableName.IsFailure)
            return tableName.ConvertFailure<Unit>();

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
                if (quoteFieldNames)
                    stringBuilder.Append($"\"{columnName.Value}\"");
                else
                    stringBuilder.Append($"{columnName.Value}");

                first = false;
            }
        }

        stringBuilder.AppendLine(")");
        stringBuilder.Append("VALUES");
        var i = 0;

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
                    command.AddParameter(valueKey, val.Value.o, val.Value.dbType);

                stringBuilder.Append($"@{valueKey}");
                first = false;
                i++;
            }

            stringBuilder.AppendLine(")");
        }

        command.CommandText = stringBuilder.ToString();

        if (errors.Any())
        {
            var list = ErrorBuilderList.Combine(errors);
            return Result.Failure<Unit, IErrorBuilder>(list);
        }

        return Unit.Default;

        static Result<(object? o, DbType dbType), IErrorBuilder> GetValue(
            Maybe<EntityValue> entityValue,
            string columnName)
        {
            if (entityValue.HasNoValue)
                return (null, DbType.String);

            var v = entityValue.Value.Match<Result<(object? o, DbType dbType), IErrorBuilder>>(
                _ => (null, DbType.String),
                s => (s, DbType.String),
                i => (i, DbType.Int32),
                d => (d, DbType.Double),
                b => (b, DbType.Boolean),
                e => (e.Value, DbType.String),
                dt => (dt, DbType.DateTime2),
                _ => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                    nameof(Entity),
                    columnName
                ),
                _ => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder("Array", columnName)
            );

            return v;
        }
    }

    /// <summary>
    /// The Connection String
    /// </summary>
    [StepProperty(order: 1)]
    [Required]
    public IStep<StringStream> ConnectionString { get; set; } = null!;

    /// <summary>
    /// The entities to insert
    /// </summary>
    [StepProperty(order: 2)]
    [Required]
    [Alias("Sql")]
    public IStep<Array<Entity>> Entities { get; set; } = null!;

    /// <summary>
    /// The schema that the data must match
    /// </summary>
    [StepProperty(order: 3)]
    [Required]
    public IStep<Entity> Schema { get; set; } = null!;

    /// <summary>
    /// The Database Type to connect to
    /// </summary>
    [StepProperty(4)]
    [DefaultValueExplanation("Sql")]
    [Alias("DB")]
    public IStep<DatabaseType> DatabaseType { get; set; } =
        new EnumConstant<DatabaseType>(Sql.DatabaseType.MsSql);

    /// <summary>
    /// The schema this table belongs to, if postgres
    /// </summary>
    [StepProperty(5)]
    [DefaultValueExplanation("No schema")]
    public IStep<StringStream>? PostgresSchema { get; set; } = null;

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<SqlInsert, Unit>();
}

}
