using System.Data;
using System.Linq;
using System.Text;
using Json.Schema;
using Reductech.Sequence.Core.Internal.Errors;
using Entity = Reductech.Sequence.Core.Entity;

namespace Reductech.Sequence.Connectors.Sql.Steps;

/// <summary>
/// Create a SQL table from a given schema
/// </summary>
public sealed class SqlCreateTable : CompoundStep<Unit>
{
    /// <inheritdoc />
    protected override async ValueTask<Result<Unit, IError>> Run(
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

        var schema =
            EntityConversionHelpers.TryCreateFromEntity<JsonSchema>(entity.Value)
                .MapError(x => x.WithLocation(this));

        if (schema.IsFailure)
            return schema.ConvertFailure<Unit>();

        var factory = stateMonad.ExternalContext
            .TryGetContext<IDbConnectionFactory>(DbConnectionFactory.DbConnectionName);

        if (factory.IsFailure)
            return factory.MapError(x => x.WithLocation(this)).ConvertFailure<Unit>();

        var commandTextResult = GetCommandText(
            schema.Value,
            databaseConnectionMetadata.Value.DatabaseType
        );

        if (commandTextResult.IsFailure)
            return commandTextResult.MapError(x => x.WithLocation(this)).ConvertFailure<Unit>();

        using IDbConnection conn =
            factory.Value.GetDatabaseConnection(databaseConnectionMetadata.Value);

        conn.Open();

        using var dbCommand = conn.CreateCommand();

        dbCommand.CommandText = commandTextResult.Value;

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

    internal static Result<string, IErrorBuilder> GetCommandText(
        JsonSchema schema,
        DatabaseType databaseType)
    {
        var sb     = new StringBuilder();
        var errors = new List<IErrorBuilder>();

        if (schema.Keywords is null)
            errors.Add(ErrorCode_Sql.CouldNotCreateTable.ToErrorBuilder($"Schema has no keywords"));
        else
        {
            var quoteNames = ShouldQuoteNames(databaseType);

            var title = schema.Keywords.OfType<TitleKeyword>()
                .Select(x => x.Value)
                .FirstOrDefault();

            if (title is null)
                errors.Add(
                    ErrorCode_Sql.CouldNotCreateTable.ToErrorBuilder($"Schema has no keywords")
                );
            else
            {
                var tableName = Extensions.CheckSqlObjectName(title);

                if (tableName.IsFailure)
                    errors.Add(tableName.Error);
                else
                    sb.AppendLine(
                        $"CREATE TABLE {Extensions.MaybeQuote(tableName.Value, quoteNames)} ("
                    );

                var index = 0;

                var schemaProperties = schema.Keywords.OfType<PropertiesKeyword>()
                    .SelectMany(x => x.Properties);

                var requiredProperties = schema.Keywords.OfType<RequiredKeyword>()
                    .SelectMany(x => x.Properties)
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                foreach (var (column, propertySchema) in schemaProperties)
                {
                    var dataType     = TypeConversion.TryGetDataType(propertySchema, databaseType);
                    var multiplicity = requiredProperties.Contains(column) ? "NOT NULL" : "NULL";

                    if (dataType.IsFailure)
                        errors.Add(dataType.Error);

                    if (dataType.IsSuccess)
                    {
                        if (index > 0)
                            sb.Append(',');

                        var columnName = Extensions.CheckSqlObjectName(column);

                        if (columnName.IsFailure)
                            errors.Add(columnName.Error);
                        else
                            sb.AppendLine(
                                $"{Extensions.MaybeQuote(columnName.Value, quoteNames)} {dataType.Value} {multiplicity}"
                            );

                        index++;
                    }
                }

                sb.AppendLine(")");
            }
        }

        if (errors.Any())
            return Result.Failure<string, IErrorBuilder>(ErrorBuilderList.Combine(errors));

        return sb.ToString();

        static bool ShouldQuoteNames(DatabaseType databaseType)
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
    }
}
