using System.Data;
using System.Linq;
using Json.More;
using Json.Schema;
using Microsoft.SqlServer.Management.SqlParser.Parser;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using Reductech.Sequence.Core.Internal.Errors;
using Entity = Reductech.Sequence.Core.Entity;

namespace Reductech.Sequence.Connectors.Sql.Steps;

/// <summary>
/// Creates a Schema entity from a SQL table
/// </summary>
public sealed class SqlCreateSchemaFromTable : CompoundStep<Entity>
{
    /// <inheritdoc />
    protected override async ValueTask<Result<Entity, IError>> Run(
        IStateMonad stateMonad,
        CancellationToken cancellationToken)
    {
        var table = await Table.Run(stateMonad, cancellationToken)
            .Map(x => x.GetStringAsync())
            .Bind(x => Extensions.CheckSqlObjectName(x).MapError(e => e.WithLocation(this)));

        if (table.IsFailure)
            return table.ConvertFailure<Entity>();

        var databaseConnectionMetadata = await DatabaseConnectionMetadata.GetOrCreate(
            Connection,
            stateMonad,
            this,
            cancellationToken
        );

        if (databaseConnectionMetadata.IsFailure)
            return databaseConnectionMetadata.ConvertFailure<Entity>();

        string? postgresSchema = null;

        if (PostgresSchema is not null)
        {
            var s = await PostgresSchema.Run(stateMonad, cancellationToken)
                .Map(x => x.GetStringAsync())
                .Bind(x => Extensions.CheckSqlObjectName(x).MapError(e => e.WithLocation(this)));

            if (s.IsFailure)
                return s.ConvertFailure<Entity>();

            postgresSchema = s.Value;
        }

        var factory = stateMonad.ExternalContext
            .TryGetContext<IDbConnectionFactory>(DbConnectionFactory.DbConnectionName);

        if (factory.IsFailure)
            return factory
                .MapError(x => x.WithLocation(this))
                .ConvertFailure<Entity>();

        using var conn = factory.Value.GetDatabaseConnection(databaseConnectionMetadata.Value);

        conn.Open();

        var queryString = GetQuery(
            table.Value,
            postgresSchema,
            databaseConnectionMetadata.Value.DatabaseType
        );

        using var command = conn.CreateCommand();
        command.CommandText = queryString;

        var r = Convert(command, table.Value, databaseConnectionMetadata.Value.DatabaseType)
            .MapError(x => x.WithLocation(this));

        return r;
    }

    private static void SetType(JsonSchemaBuilder builder, TypeReference sclType)
    {
        if (sclType == TypeReference.Actual.String)
            builder.Type(SchemaValueType.String);
        else if (sclType == TypeReference.Actual.Integer)
            builder.Type(SchemaValueType.Integer);
        else if (sclType == TypeReference.Actual.Double)
            builder.Type(SchemaValueType.Number);
        else if (sclType is TypeReference.Enum)
            builder.Type(SchemaValueType.String);
        else if (sclType == TypeReference.Actual.Bool)
            builder.Type(SchemaValueType.Boolean);
        else if (sclType == TypeReference.Actual.Date)
        {
            builder.Type(SchemaValueType.String);
            builder.Format(new Format("date-time"));
        }
        else if (sclType is TypeReference.Entity)
        {
            builder.Type(SchemaValueType.Object);
        }
        else
            throw new ArgumentOutOfRangeException(sclType.Name);
    }

    private static Result<Entity, IErrorBuilder> Convert(
        IDbCommand command,
        string tableName,
        DatabaseType databaseType)
    {
        return databaseType switch
        {
            DatabaseType.SQLite => ConvertSQLite(command, tableName),
            _                   => ConvertDefault(command, tableName, databaseType)
        };

        static Result<Entity, IErrorBuilder> ConvertDefault(
            IDbCommand command,
            string tableName,
            DatabaseType databaseType)
        {
            IDataReader reader;

            try
            {
                reader = command.ExecuteReader();
            }
            catch (Exception e)
            {
                return Result.Failure<Entity, IErrorBuilder>(
                    ErrorCode_Sql.SqlError.ToErrorBuilder(e.Message)
                );
            }

            var properties         = new Dictionary<string, JsonSchema>();
            var requiredProperties = new List<string>();

            try
            {
                var row = new object[reader.FieldCount];

                while (!reader.IsClosed && reader.Read())
                {
                    JsonSchemaBuilder builder      = new();
                    string            propertyName = "";
                    var               required     = false;

                    reader.GetValues(row);

                    for (var col = 0; col < row.Length; col++)
                    {
                        var name  = reader.GetName(col);
                        var value = row[col].ToString()!;

                        switch (name.ToUpperInvariant())
                        {
                            case "COLUMN_NAME":
                            {
                                propertyName = value;
                                //builder.Title(value);
                            }

                                break;
                            case "IS_NULLABLE":
                            {
                                if (value.Equals("NO", StringComparison.OrdinalIgnoreCase))
                                    required = true;

                                break;
                            }
                            case "DATA_TYPE":
                            {
                                var r = TypeConversion.TryConvertDataType(
                                    value,
                                    propertyName,
                                    databaseType
                                );

                                if (r.IsFailure)
                                    return r.ConvertFailure<Entity>();

                                SetType(builder, r.Value);

                                break;
                            }
                        }
                    }

                    properties.Add(propertyName, builder.Build());

                    if (required)
                        requiredProperties.Add(propertyName);
                }
            }
            finally
            {
                reader.Close();
                reader.Dispose();
            }

            var schemaBuilder = new JsonSchemaBuilder()
                .Title(tableName)
                .AdditionalProperties(JsonSchema.False)
                .Properties(properties);

            if (requiredProperties.Any())
                schemaBuilder.Required(requiredProperties);

            var schema = schemaBuilder.Build();

            return Entity.Create(schema.ToJsonDocument().RootElement);
        }

        static Result<Entity, IErrorBuilder> ConvertSQLite(IDbCommand command, string tableName)
        {
            string queryResult;

            try
            {
                queryResult = command.ExecuteScalar()?.ToString()!;
            }
            catch (Exception e)
            {
                return Result.Failure<Entity, IErrorBuilder>(
                    ErrorCode_Sql.SqlError.ToErrorBuilder(e.Message)
                );
            }

            var parseResult =
                Parser.Parse(queryResult);

            var statements = parseResult.Script
                .SelfAndDescendants<SqlCodeObject>(x => x.Children)
                .OfType<SqlCreateTableStatement>()
                .ToList();

            if (statements.Count != 1)
                return ErrorCode_Sql.CouldNotGetCreateTable.ToErrorBuilder(tableName);

            var entity = ToSchema(statements.Single())
                .Map(x => Entity.Create(x.ToJsonDocument().RootElement));

            return entity;
        }
    }

    /// <summary>
    /// The table to create a schema from
    /// </summary>
    [StepProperty(order: 1)]
    [Required]
    public IStep<StringStream> Table { get; set; } = null!;

    /// <summary>
    /// The schema this table belongs to, if postgres
    /// </summary>
    [StepProperty(2)]
    [DefaultValueExplanation("No schema")]
    public IStep<StringStream>? PostgresSchema { get; set; } = null;

    /// <summary>
    /// The Connection String
    /// </summary>
    [StepProperty(order: 3)]
    [DefaultValueExplanation("The Most Recent Connection")]
    public IStep<Entity>? Connection { get; set; } = null;

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<SqlCreateSchemaFromTable, Entity>();

    private static string GetQuery(string tableName, string? schema, DatabaseType databaseType)
    {
        return databaseType switch
        {
            DatabaseType.SQLite => $"SELECT sql FROM SQLite_master WHERE name = '{tableName}';",
            _                   => CreateQuery(tableName, schema)
        };

        static string CreateQuery(string tableName, string? schema)
        {
            var q =
                $"SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE  from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '{tableName}'";

            if (schema != null)
                q += $" table_schema = '{schema}'";

            return q;
        }
    }

    /// <summary>
    /// Convert a SQL create table statement to an SCL Schema.
    /// </summary>
    public static Result<JsonSchema, IErrorBuilder> ToSchema(SqlCreateTableStatement statement)
    {
        var schemaProperties   = new Dictionary<string, JsonSchema>();
        var requiredProperties = new List<string>();

        var errors = new List<IErrorBuilder>();

        foreach (var columnDefinition in statement.Definition.ColumnDefinitions)
        {
            var gts = columnDefinition.DataType.DataType.GetTypeSpec();

            var r = TypeConversion.TryConvertSqlDataType(
                gts.SqlDataType,
                columnDefinition.Name.Value
            );

            if (r.IsFailure)
                errors.Add(r.Error);
            else
            {
                if (columnDefinition.Constraints.Any(x => x.Type == SqlConstraintType.NotNull))
                    requiredProperties.Add(columnDefinition.Name.Value);

                var builder = new JsonSchemaBuilder();

                SetType(builder, r.Value);

                schemaProperties.Add(columnDefinition.Name.Value, builder.Build());
            }
        }

        if (errors.Any())
            return Result.Failure<JsonSchema, IErrorBuilder>(ErrorBuilderList.Combine(errors));

        var mainBuilder = new JsonSchemaBuilder()
            .Title(statement.Name.ObjectName.Value)
            .AdditionalProperties(JsonSchema.False)
            .Properties(schemaProperties);

        if (requiredProperties.Any())
            mainBuilder.Required(requiredProperties);

        return mainBuilder.Build();
    }
}
