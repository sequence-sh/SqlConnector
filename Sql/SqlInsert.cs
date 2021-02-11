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
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Internal.Errors;
using Reductech.EDR.Core.Internal.Logging;
using Reductech.EDR.Core.Util;
using Entity = Reductech.EDR.Core.Entity;

namespace Reductech.EDR.Connectors.Sql
{

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

        var table = await Table.Run(stateMonad, cancellationToken)
            .Map(x => x.GetStringAsync());

        if (table.IsFailure)
            return table.ConvertFailure<Unit>();

        var databaseType = await DatabaseType.Run(stateMonad, cancellationToken);

        if (databaseType.IsFailure)
            return databaseType.ConvertFailure<Unit>();

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

        foreach (var entity in elements.Value)
        {
            using var dbCommand = conn.CreateCommand();

            var setCommandResult = SetCommand(entity, table.Value, dbCommand);

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
        }

        return Unit.Default;
    }

    private Result<Unit, IErrorBuilder> SetCommand(
        Entity entity,
        string tableName,
        IDbCommand command)
    {
        var insertStringBuilder = new StringBuilder();
        var valuesBuilder       = new StringBuilder();
        var errors              = new List<IErrorBuilder>();

        //const string tableNameKey = "TableName";
        //insertStringBuilder.Append($"INSERT INTO @{tableNameKey} (");
        insertStringBuilder.Append($"INSERT INTO {tableName} (");
        //command.AddParameter(tableNameKey, tableName, DbType.String);
        valuesBuilder.Append("VALUES (");

        var i = 0;

        foreach (var property in entity)
        {
            if (i > 0)
            {
                insertStringBuilder.Append(", ");
                valuesBuilder.Append(", ");
            }

            insertStringBuilder.Append($"{property.Name}");
            //var columnKey = $"Column{i}";
            //insertStringBuilder.Append($"@{columnKey}");
            //command.AddParameter(columnKey, property.Name, DbType.String);

            var valueKey = $"Value{i}";
            valuesBuilder.Append($"@{valueKey}");

            var val = GetValue(property.BestValue, property.Name);

            if (val.IsFailure)
                errors.Add(val.Error);
            else
                command.AddParameter(valueKey, val.Value.o, val.Value.dbType);

            i++;
        }

        insertStringBuilder.Append(")");
        valuesBuilder.Append(");");

        command.CommandText = $"{insertStringBuilder} {valuesBuilder}";

        if (errors.Any())
        {
            var list = ErrorBuilderList.Combine(errors);
            return Result.Failure<Unit, IErrorBuilder>(list);
        }

        return Unit.Default;

        static Result<(object? o, DbType dbType), IErrorBuilder> GetValue(
            EntityValue entityValue,
            string columnName)
        {
            var v = entityValue.Match<Result<(object? o, DbType dbType), IErrorBuilder>>(
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
    /// The table to insert into
    /// </summary>
    [StepProperty(order: 3)]
    [Required]
    [Alias("Sql")]
    public IStep<StringStream> Table { get; set; } = null!;

    [StepProperty(4)]
    [DefaultValueExplanation("Sql")]
    [Alias("DB")]
    public IStep<DatabaseType> DatabaseType { get; set; } =
        new EnumConstant<DatabaseType>(Sql.DatabaseType.MsSql);

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<SqlInsert, Unit>();
}

}
