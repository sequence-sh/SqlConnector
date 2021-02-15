using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CSharpFunctionalExtensions;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
using Npgsql.PostgresTypes;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Attributes;
using Reductech.EDR.Core.Entities;
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Internal.Errors;
using Reductech.EDR.Core.Util;
using Entity = Reductech.EDR.Core.Entity;
using SqlDataType = Microsoft.SqlServer.Management.SqlParser.Metadata.SqlDataType;

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
        var connectionString =
            await ConnectionString.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (connectionString.IsFailure)
            return connectionString.ConvertFailure<Entity>();

        var table = await Table.Run(stateMonad, cancellationToken)
            .Map(x => x.GetStringAsync())
            .Bind(x => Extensions.CheckSqlObjectName(x).MapError(e => e.WithLocation(this)));

        if (table.IsFailure)
            return table.ConvertFailure<Entity>();

        var databaseType = await DatabaseType.Run(stateMonad, cancellationToken);

        if (databaseType.IsFailure)
            return databaseType.ConvertFailure<Entity>();

        string? schema = null;

        if (Schema is not null)
        {
            var s = await Schema.Run(stateMonad, cancellationToken)
                .Map(x => x.GetStringAsync())
                .Bind(x => Extensions.CheckSqlObjectName(x).MapError(e => e.WithLocation(this)));

            if (s.IsFailure)
                return s.ConvertFailure<Entity>();

            schema = s.Value;
        }

        var factory = stateMonad.ExternalContext
            .TryGetContext<IDbConnectionFactory>(DbConnectionFactory.DbConnectionName);

        if (factory.IsFailure)
            return factory
                .MapError(x => x.WithLocation(this))
                .ConvertFailure<Entity>();

        using var conn = factory.Value.GetDatabaseConnection(
            databaseType.Value,
            connectionString.Value
        );

        conn.Open();

        var queryString = GetQuery(table.Value, schema, databaseType.Value);

        using var command = conn.CreateCommand();
        command.CommandText = queryString;

        var r = Convert(command, table.Value, databaseType.Value)
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
            Sql.DatabaseType.SQLite => ConvertSQLite(command, tableName),
            Sql.DatabaseType.MsSql => ConvertDefault(command, tableName, databaseType),
            Sql.DatabaseType.Postgres => ConvertDefault(command, tableName, databaseType),
            _ => throw new ArgumentOutOfRangeException(nameof(databaseType), databaseType, null)
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
                                schemaProperty.Multiplicity = value
                                    switch
                                    {
                                        "YES" => Multiplicity.UpToOne,
                                        "NO"  => Multiplicity.ExactlyOne,
                                        _     => throw new ArgumentException(value)
                                    };

                                break;
                            }
                            case "DATA_TYPE":
                            {
                                var r = ConvertDataType(value, propertyName, databaseType);

                                if (r.IsFailure)
                                    return r.ConvertFailure<Entity>();

                                schemaProperty.Type = r.Value;

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
                Name = tableName, AllowExtraProperties = false, Properties = properties
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
                Microsoft.SqlServer.Management.SqlParser.Parser.Parser.Parse(queryResult);

            var schema = parseResult.Script
                .SelfAndDescendants<SqlCodeObject>(x => x.Children)
                .OfType<SqlCreateTableStatement>()
                .EnsureSingle(
                    ErrorCode_Sql.CouldNotGetCreateTable.ToErrorBuilder(tableName) as IErrorBuilder
                )
                .Bind(ToSchema)
                .Map(x => x.ConvertToEntity());

            return schema;
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
    public IStep<StringStream> Table { get; set; } = null!;

    /// <summary>
    /// The Database Type to connect to
    /// </summary>
    [StepProperty(3)]
    [DefaultValueExplanation("Sql")]
    [Alias("DB")]
    public IStep<DatabaseType> DatabaseType { get; set; } =
        new EnumConstant<DatabaseType>(Sql.DatabaseType.MsSql);

    /// <summary>
    /// The schema this table belongs to, if postgres
    /// </summary>
    [StepProperty(4)]
    [DefaultValueExplanation("No schema")]
    public IStep<StringStream>? Schema { get; set; } = null;

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<SqlCreateSchemaFromTable, Entity>();

    private static string GetQuery(string tableName, string? schema, DatabaseType databaseType)
    {
        return databaseType switch
        {
            Sql.DatabaseType.SQLite => $"SELECT sql FROM SQLite_master WHERE name = '{tableName}';",
            Sql.DatabaseType.MsSql => CreateQuery(tableName, schema),
            Sql.DatabaseType.Postgres => CreateQuery(tableName, schema),
            _ => throw new ArgumentOutOfRangeException(nameof(databaseType), databaseType, null)
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

    public static Result<Schema, IErrorBuilder> ToSchema(SqlCreateTableStatement statement)
    {
        var schemaProperties = new Dictionary<string, SchemaProperty>();

        var errors = new List<IErrorBuilder>();

        foreach (var columnDefinition in statement.Definition.ColumnDefinitions)
        {
            var gts = columnDefinition.DataType.DataType.GetTypeSpec();

            var r = ConvertSqlDataType(gts.SqlDataType, columnDefinition.Name.Value);

            if (r.IsFailure)
                errors.Add(r.Error);
            else
            {
                var multiplicity =
                    columnDefinition.Constraints.Any(x => x.Type == SqlConstraintType.NotNull)
                        ? Multiplicity.ExactlyOne
                        : Multiplicity.UpToOne;

                var property = new SchemaProperty() { Type = r.Value, Multiplicity = multiplicity };

                schemaProperties.Add(columnDefinition.Name.Value, property);
            }
        }

        if (errors.Any())
            return Result.Failure<Schema, IErrorBuilder>(ErrorBuilderList.Combine(errors));

        return new Schema
        {
            AllowExtraProperties = false,
            Name                 = statement.Name.ObjectName.Value,
            Properties           = schemaProperties,
        };
    }

    private static Result<SchemaPropertyType, IErrorBuilder> ConvertDataType(
        string dataTypeString,
        string column,
        DatabaseType databaseType)
    {
        switch (databaseType)
        {
            case Sql.DatabaseType.SQLite:
                return ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(dataTypeString, column);
            case Sql.DatabaseType.MsSql:
            {
                if (Enum.TryParse(dataTypeString, true, out SqlDataType dt))
                    return ConvertSqlDataType(dt, column);

                return ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(dataTypeString, column);
            }
            case Sql.DatabaseType.Postgres:
            {
                return ConvertPostgresDataType(dataTypeString, column);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(databaseType), databaseType, null);
        }
    }

    private static Result<SchemaPropertyType, IErrorBuilder> ConvertPostgresDataType(
        string dataType,
        string column)
    {
        return dataType.ToLowerInvariant() switch //This list is not exhaustive
        {
            "bigint" => SchemaPropertyType.Integer,
            "bit" => SchemaPropertyType.Bool,
            "bit varying" => SchemaPropertyType.Bool,
            "boolean" => SchemaPropertyType.Bool,
            "char" => SchemaPropertyType.String,
            "character varying" => SchemaPropertyType.String,
            "character" => SchemaPropertyType.String,
            "varchar" => SchemaPropertyType.String,
            "date" => SchemaPropertyType.Date,
            "double precision" => SchemaPropertyType.Double,
            "integer" => SchemaPropertyType.Integer,
            "numeric" => SchemaPropertyType.Double,
            "decimal" => SchemaPropertyType.Double,
            "real" => SchemaPropertyType.Double,
            "smallint" => SchemaPropertyType.Integer,
            "text" => SchemaPropertyType.String,
            "time" => SchemaPropertyType.Date,
            "timestamp" => SchemaPropertyType.Date,
            _ => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(dataType, column)
        };
    }

    private static Result<SchemaPropertyType, IErrorBuilder> ConvertSqlDataType(
        SqlDataType dataType,
        string column)
    {
        return dataType switch
        {
            SqlDataType.None => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                dataType,
                column
            ),
            SqlDataType.BigInt => SchemaPropertyType.Integer,
            SqlDataType.Binary => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                dataType,
                column
            ),
            SqlDataType.Bit            => SchemaPropertyType.Bool,
            SqlDataType.Char           => SchemaPropertyType.String,
            SqlDataType.Date           => SchemaPropertyType.Date,
            SqlDataType.DateTime       => SchemaPropertyType.Date,
            SqlDataType.DateTime2      => SchemaPropertyType.Date,
            SqlDataType.DateTimeOffset => SchemaPropertyType.Date,
            SqlDataType.Decimal        => SchemaPropertyType.Double,
            SqlDataType.Float          => SchemaPropertyType.Double,
            SqlDataType.Geography => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                dataType,
                column
            ),
            SqlDataType.Geometry => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                dataType,
                column
            ),
            SqlDataType.HierarchyId => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                dataType,
                column
            ),
            SqlDataType.Image => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                dataType,
                column
            ),
            SqlDataType.Int           => SchemaPropertyType.Integer,
            SqlDataType.Money         => SchemaPropertyType.Double,
            SqlDataType.NChar         => SchemaPropertyType.String,
            SqlDataType.NText         => SchemaPropertyType.String,
            SqlDataType.Numeric       => SchemaPropertyType.Double,
            SqlDataType.NVarChar      => SchemaPropertyType.String,
            SqlDataType.NVarCharMax   => SchemaPropertyType.String,
            SqlDataType.Real          => SchemaPropertyType.Double,
            SqlDataType.SmallDateTime => SchemaPropertyType.Date,
            SqlDataType.SmallInt      => SchemaPropertyType.Integer,
            SqlDataType.SmallMoney    => SchemaPropertyType.Double,
            SqlDataType.SysName       => SchemaPropertyType.String,
            SqlDataType.Text          => SchemaPropertyType.String,
            SqlDataType.Time          => SchemaPropertyType.Date,
            SqlDataType.Timestamp     => SchemaPropertyType.Date,
            SqlDataType.TinyInt       => SchemaPropertyType.Integer,
            SqlDataType.UniqueIdentifier => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                dataType,
                column
            ),
            SqlDataType.VarBinary => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                dataType,
                column
            ),
            SqlDataType.VarBinaryMax => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                dataType,
                column
            ),
            SqlDataType.VarChar    => SchemaPropertyType.String,
            SqlDataType.VarCharMax => SchemaPropertyType.String,
            SqlDataType.Variant    => SchemaPropertyType.String,
            SqlDataType.Xml        => SchemaPropertyType.String,
            SqlDataType.XmlNode => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                dataType,
                column
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(dataType), dataType, null)
        };
    }
}

}
