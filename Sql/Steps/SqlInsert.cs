using System.Data;
using System.Linq;
using System.Text;
using Json.Schema;
using MoreLinq;
using Reductech.EDR.Core.Entities;
using Reductech.EDR.Core.Internal.Errors;
using Entity = Reductech.EDR.Core.Entity;

namespace Reductech.EDR.Connectors.Sql.Steps;

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
        var stuff = await stateMonad.RunStepsAsync(
            Entities.WrapArray(),
            Schema.WrapStep(StepMaps.ConvertToSchema(Schema)),
            PostgresSchema.WrapNullable(StepMaps.String()),
            cancellationToken
        );

        if (stuff.IsFailure)
            return stuff.ConvertFailure<Unit>();

        var (elements, schema, postgresSchemaString) = stuff.Value;

        var databaseConnectionMetadata = await DatabaseConnectionMetadata.GetOrCreate(
            Connection,
            stateMonad,
            this,
            cancellationToken
        );

        if (databaseConnectionMetadata.IsFailure)
            return databaseConnectionMetadata.ConvertFailure<Unit>();

        string? postgresSchema = null;

        if (postgresSchemaString.HasValue)
        {
            var psSchemaResult =
                Extensions.CheckSqlObjectName(postgresSchemaString.GetValueOrThrow())
                    .MapError(e => e.WithLocation(this));

            if (psSchemaResult.IsFailure)
                return psSchemaResult.ConvertFailure<Unit>();

            postgresSchema = psSchemaResult.Value;
        }

        var factory = stateMonad.ExternalContext
            .TryGetContext<IDbConnectionFactory>(DbConnectionFactory.DbConnectionName);

        if (factory.IsFailure)
            return factory.MapError(x => x.WithLocation(this)).ConvertFailure<Unit>();

        IDbConnection conn = factory.Value.GetDatabaseConnection(databaseConnectionMetadata.Value);

        conn.Open();
        const int maxQueryParameters = 2099;

        var propertiesCount = schema.Keywords?.OfType<PropertiesKeyword>()
            .SelectMany(x => x.Properties)
            .Count() ?? 0;

        var batchSize = maxQueryParameters / propertiesCount;

        var batches = elements.Batch(batchSize);

        var shouldQuoteFieldNames =
            ShouldQuoteFieldNames(databaseConnectionMetadata.Value.DatabaseType);

        foreach (var batch in batches)
        {
            var commandDataResult = GetCommandData(
                schema,
                postgresSchema,
                shouldQuoteFieldNames,
                batch
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

    /// <summary>
    /// The command and parameters used for inserting table rows
    /// </summary>
    public record CommandData(
        string CommandText,
        IReadOnlyList<(string Parameter, object Value, DbType DbType)> Parameters)
    {
        /// <summary>
        /// Create a command from the CommandText and Parameters
        /// </summary>
        public void SetCommand(IDbCommand command)
        {
            command.CommandText = CommandText;

            foreach (var (parameter, value, dbType) in Parameters)
            {
                command.AddParameter(parameter, value, dbType);
            }
        }
    }

    private static Result<CommandData, IErrorBuilder> GetCommandData(
        JsonSchema schema,
        string? postgresSchemaName,
        bool quoteFieldNames,
        IEnumerable<Entity> entities)
    {
        var stringBuilder = new StringBuilder();
        var errors        = new List<IErrorBuilder>();

        if (schema.Keywords is null)
            return ErrorCode_Sql.CouldNotCreateTable.ToErrorBuilder("Schema keywords is null");

        var schemaTitle = schema.Keywords.OfType<TitleKeyword>()
            .Select(x => x.Value)
            .DefaultIfEmpty("")
            .First();

        var tableName = Extensions.CheckSqlObjectName(schemaTitle);

        if (tableName.IsFailure)
            return tableName.ConvertFailure<CommandData>();

        if (string.IsNullOrWhiteSpace(postgresSchemaName))
            stringBuilder.Append($"INSERT INTO {tableName.Value} (");
        else
            stringBuilder.Append($"INSERT INTO {postgresSchemaName}.\"{tableName.Value}\" (");

        var first = true;

        foreach (var (name, _) in schema.Keywords.OfType<PropertiesKeyword>()
                     .SelectMany(x => x.Properties))
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

            var vr = schema.Validate(
                entity.ToJsonElement(),
                SchemaExtensions.DefaultValidationOptions
            );

            if (!vr.IsValid)
            {
                errors.AddRange(
                    vr.GetErrorMessages()
                        .Select(
                            x => ErrorCode.SchemaViolation.ToErrorBuilder(
                                x.message,
                                x.location
                            )
                        )
                );

                continue;
            }

            stringBuilder.Append('(');
            first = true;

            foreach (var (name, _) in schema.Keywords.OfType<PropertiesKeyword>()
                         .SelectMany(x => x.Properties))
            {
                if (!first)
                    stringBuilder.Append(", ");

                var ev = entity.TryGetValue(name);

                var val = GetValue(ev, name);

                var valueKey = $"v{i}";

                if (val.IsFailure)
                    errors.Add(val.Error);
                else
                    parameters.Add((valueKey, val.Value.o!, val.Value.dbType));

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
            Maybe<ISCLObject> entityValue,
            string columnName)
        {
            if (entityValue.HasNoValue)
                return (null, DbType.String);

            return GetValue( entityValue.GetValueOrThrow(), columnName );

            static Result<(object? o, DbType dbType), IErrorBuilder>  GetValue(ISCLObject sclObject, string columnName)
            {
                return sclObject switch
            {
                SCLBool boolean => (boolean.Value, DbType.Boolean),
                SCLDateTime date   => (date.Value, DbType.DateTime2),
                SCLInt integer => (integer.Value, DbType.Int32),
                SCLDouble d        => (d.Value, DbType.Double),
                ISCLEnum enumerationValue => (
                    enumerationValue.EnumValue, DbType.String),
                
                Entity => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                    nameof(Entity),
                    columnName
                ),
                IArray => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                    "Array",
                    columnName
                ),
                SCLNull => (null, DbType.String),
                Unit => (null, DbType.String),
                StringStream s => (s.GetString(), DbType.String),
                ISCLOneOf oneOf => GetValue( oneOf.Value, columnName),
                _ => throw new ArgumentOutOfRangeException(sclObject.Serialize(SerializeOptions.Serialize))
            };
            }
        }
    }
}
