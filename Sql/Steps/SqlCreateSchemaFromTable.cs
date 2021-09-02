using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CSharpFunctionalExtensions;
using Microsoft.SqlServer.Management.SqlParser.Parser;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Attributes;
using Reductech.EDR.Core.Entities;
using Reductech.EDR.Core.Enums;
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Internal.Errors;
using Entity = Reductech.EDR.Core.Entity;

namespace Reductech.EDR.Connectors.Sql.Steps
{

/// <summary>
/// Creates a Schema entity from a SQL table
/// </summary>
public sealed class SqlCreateSchemaFromTable : CompoundStep<Entity>
{
    /// <inheritdoc />
    protected override async Task<Result<Entity, IError>> Run(
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

            var properties = new Dictionary<string, SchemaProperty>();

            try
            {
                var row = new object[reader.FieldCount];

                while (!reader.IsClosed && reader.Read())
                {
                    SchemaProperty schemaProperty = new();
                    string         propertyName   = "";

                    reader.GetValues(row);

                    for (var col = 0; col < row.Length; col++)
                    {
                        var name  = reader.GetName(col);
                        var value = row[col].ToString()!;

                        switch (name.ToUpperInvariant())
                        {
                            case "COLUMN_NAME":
                                propertyName = value;
                                break;
                            case "IS_NULLABLE":
                            {
                                schemaProperty = schemaProperty with
                                {
                                    Multiplicity = value
                                        switch
                                        {
                                            "YES" => Multiplicity.UpToOne,
                                            "NO"  => Multiplicity.ExactlyOne,
                                            _     => throw new ArgumentException(value)
                                        }
                                };

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

                                schemaProperty = schemaProperty with { Type = r.Value };

                                break;
                            }
                        }
                    }

                    properties.Add(propertyName, schemaProperty);
                }
            }
            finally
            {
                reader.Close();
                reader.Dispose();
            }

            Schema schema = new()
            {
                Name            = tableName,
                ExtraProperties = ExtraPropertyBehavior.Fail,
                Properties      = properties.ToImmutableSortedDictionary()
            };

            return schema.ConvertToEntity();
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

            var schemas = parseResult.Script
                .SelfAndDescendants<SqlCodeObject>(x => x.Children)
                .OfType<SqlCreateTableStatement>()
                .ToList();

            if (schemas.Count != 1)
                return ErrorCode_Sql.CouldNotGetCreateTable.ToErrorBuilder(tableName);

            var entity = ToSchema(schemas.Single()).Map(x => x.ConvertToEntity());

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
    public static Result<Schema, IErrorBuilder> ToSchema(SqlCreateTableStatement statement)
    {
        var schemaProperties = new Dictionary<string, SchemaProperty>();

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
                var multiplicity =
                    columnDefinition.Constraints.Any(x => x.Type == SqlConstraintType.NotNull)
                        ? Multiplicity.ExactlyOne
                        : Multiplicity.UpToOne;

                var property = new SchemaProperty { Type = r.Value, Multiplicity = multiplicity };

                schemaProperties.Add(columnDefinition.Name.Value, property);
            }
        }

        if (errors.Any())
            return Result.Failure<Schema, IErrorBuilder>(ErrorBuilderList.Combine(errors));

        return new Schema
        {
            ExtraProperties = ExtraPropertyBehavior.Fail,
            Name            = statement.Name.ObjectName.Value,
            Properties      = schemaProperties.ToImmutableSortedDictionary(),
        };
    }
}

}
