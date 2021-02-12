using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CSharpFunctionalExtensions;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;
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

        var table = await Table.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (table.IsFailure)
            return table.ConvertFailure<Entity>();

        var databaseType = await DatabaseType.Run(stateMonad, cancellationToken);

        if (databaseType.IsFailure)
            return databaseType.ConvertFailure<Entity>();

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

        var queryString = $"SELECT sql FROM sqlite_master WHERE name = '{table.Value}';";

        using var command = conn.CreateCommand();
        command.CommandText = queryString;

        string createStatement;

        try
        {
            createStatement = command.ExecuteScalar()?.ToString()!;
        }
        catch (Exception e)
        {
            return Result.Failure<Entity, IError>(
                ErrorCode_Sql.SqlError.ToErrorBuilder(e.Message).WithLocation(this)
            );
        }

        var parseResult =
            Microsoft.SqlServer.Management.SqlParser.Parser.Parser.Parse(createStatement);

        var finalResult = parseResult.Script
            .SelfAndDescendants<SqlCodeObject>(x => x.Children)
            .OfType<SqlCreateTableStatement>()
            .EnsureSingle(
                ErrorCode_Sql.CouldNotGetCreateTable.ToErrorBuilder(table.Value)
                    .WithLocation(this)
            )
            .Bind(x => ToSchema(x).MapError(e => e.WithLocation(this)))
            .Map(x => x.ConvertToEntity());

        return finalResult;
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

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<SqlCreateSchemaFromTable, Entity>();
}

}
