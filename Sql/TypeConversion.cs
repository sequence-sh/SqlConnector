using System.Linq;
using Json.Schema;
using Microsoft.SqlServer.Management.SqlParser.Metadata;
using MySqlConnector;
using Reductech.EDR.Core.Internal.Errors;

namespace Reductech.EDR.Connectors.Sql;

internal static class TypeConversion
{
    internal static Result<TypeReference.Actual, IErrorBuilder> TryConvertDataType(
        string dataTypeString,
        string column,
        DatabaseType databaseType)
    {
        switch (databaseType)
        {
            case DatabaseType.SQLite:
                return ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(dataTypeString, column);
            case DatabaseType.MsSql:
            {
                if (Enum.TryParse(dataTypeString, true, out SqlDataType dt))
                    return TryConvertSqlDataType(dt, column);

                return ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(dataTypeString, column);
            }
            case DatabaseType.Postgres:
            {
                return TryConvertPostgresDataType(dataTypeString, column);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(databaseType), databaseType, null);
        }
    }

    internal static Result<TypeReference.Actual, IErrorBuilder> TryConvertPostgresDataType(
        string dataType,
        string column)
    {
        return dataType.ToLowerInvariant() switch //This list is not exhaustive
        {
            "bigint" => TypeReference.Actual.Integer,
            "bit" => TypeReference.Actual.Bool,
            "bit varying" => TypeReference.Actual.Bool,
            "boolean" => TypeReference.Actual.Bool,
            "char" => TypeReference.Actual.String,
            "character varying" => TypeReference.Actual.String,
            "character" => TypeReference.Actual.String,
            "varchar" => TypeReference.Actual.String,
            "date" => TypeReference.Actual.Date,
            "double precision" => TypeReference.Actual.Double,
            "integer" => TypeReference.Actual.Integer,
            "numeric" => TypeReference.Actual.Double,
            "decimal" => TypeReference.Actual.Double,
            "real" => TypeReference.Actual.Double,
            "smallint" => TypeReference.Actual.Integer,
            "text" => TypeReference.Actual.String,
            "time" => TypeReference.Actual.Date,
            "timestamp" => TypeReference.Actual.Date,
            _ => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(dataType, column)
        };
    }

    internal static Result<TypeReference.Actual, IErrorBuilder> TryConvertSqlDataType(
        SqlDataType dataType,
        string column)
    {
        return dataType switch
        {
            SqlDataType.None => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                dataType,
                column
            ),
            SqlDataType.BigInt => TypeReference.Actual.Integer,
            SqlDataType.Binary => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                dataType,
                column
            ),
            SqlDataType.Bit            => TypeReference.Actual.Bool,
            SqlDataType.Char           => TypeReference.Actual.String,
            SqlDataType.Date           => TypeReference.Actual.Date,
            SqlDataType.DateTime       => TypeReference.Actual.Date,
            SqlDataType.DateTime2      => TypeReference.Actual.Date,
            SqlDataType.DateTimeOffset => TypeReference.Actual.Date,
            SqlDataType.Decimal        => TypeReference.Actual.Double,
            SqlDataType.Float          => TypeReference.Actual.Double,
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
            SqlDataType.Int           => TypeReference.Actual.Integer,
            SqlDataType.Money         => TypeReference.Actual.Double,
            SqlDataType.NChar         => TypeReference.Actual.String,
            SqlDataType.NText         => TypeReference.Actual.String,
            SqlDataType.Numeric       => TypeReference.Actual.Double,
            SqlDataType.NVarChar      => TypeReference.Actual.String,
            SqlDataType.NVarCharMax   => TypeReference.Actual.String,
            SqlDataType.Real          => TypeReference.Actual.Double,
            SqlDataType.SmallDateTime => TypeReference.Actual.Date,
            SqlDataType.SmallInt      => TypeReference.Actual.Integer,
            SqlDataType.SmallMoney    => TypeReference.Actual.Double,
            SqlDataType.SysName       => TypeReference.Actual.String,
            SqlDataType.Text          => TypeReference.Actual.String,
            SqlDataType.Time          => TypeReference.Actual.Date,
            SqlDataType.Timestamp     => TypeReference.Actual.Date,
            SqlDataType.TinyInt       => TypeReference.Actual.Integer,
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
            SqlDataType.VarChar    => TypeReference.Actual.String,
            SqlDataType.VarCharMax => TypeReference.Actual.String,
            SqlDataType.Variant    => TypeReference.Actual.String,
            SqlDataType.Xml        => TypeReference.Actual.String,
            SqlDataType.XmlNode => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                dataType,
                column
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(dataType), dataType, null)
        };
    }

    internal static Result<string, IErrorBuilder> TryGetDataType(
        JsonSchema schema,
        DatabaseType databaseType)
    {
        if (schema.Keywords is null)
            ErrorCode_Sql.CouldNotCreateTable.ToErrorBuilder("Schema has no keywords");

        var type = schema.Keywords!.OfType<TypeKeyword>()
            .Select(x => x.Type)
            .DefaultIfEmpty(SchemaValueType.Object)
            .First();

        switch (type)
        {
            case SchemaValueType.Object:
            {
                return ErrorCode_Sql.CouldNotCreateTable
                    .ToErrorBuilder("Sql does not support nested entities");
            }
            case SchemaValueType.Array:
            {
                return ErrorCode_Sql.CouldNotCreateTable
                    .ToErrorBuilder("Sql does not support nested lists");
            }
            case SchemaValueType.Boolean:
            {
                    return databaseType switch
                    {
                        DatabaseType.Postgres => "boolean",
                        DatabaseType.SQLite or DatabaseType.MsSql => SqlDataType.Bit.ToString().ToUpperInvariant(),
                        DatabaseType.MySql or DatabaseType.MariaDb => MySqlDbType.Bit.ToString().ToUpperInvariant(),
                        _ => throw new ArgumentOutOfRangeException(
nameof(databaseType),
databaseType,
null
),
                    };
                }
            case SchemaValueType.String:
            {
                var format = schema.Keywords!.OfType<FormatKeyword>()
                    .Select(x => x.Value.Key)
                    .DefaultIfEmpty("")
                    .First();

                if (format.Equals("date-time", StringComparison.OrdinalIgnoreCase))
                {
                    return databaseType switch
                    {
                        DatabaseType.Postgres => "date",
                        DatabaseType.SQLite => SqlDataType.DateTime2.ToString().ToUpperInvariant(),
                        DatabaseType.MsSql => SqlDataType.DateTime2.ToString().ToUpperInvariant(),
                        DatabaseType.MySql => MySqlDbType.DateTime.ToString().ToUpperInvariant(),
                        DatabaseType.MariaDb => MySqlDbType.DateTime.ToString().ToUpperInvariant(),
                        _ => throw new ArgumentOutOfRangeException(
                            nameof(databaseType),
                            databaseType,
                            null
                        )
                    };
                }
                else
                {
                    return databaseType switch
                    {
                        DatabaseType.Postgres => "text",
                        DatabaseType.SQLite   => SqlDataType.NText.ToString().ToUpperInvariant(),
                        DatabaseType.MsSql    => SqlDataType.NText.ToString().ToUpperInvariant(),
                        DatabaseType.MySql    => MySqlDbType.Text.ToString().ToUpperInvariant(),
                        DatabaseType.MariaDb  => MySqlDbType.Text.ToString().ToUpperInvariant(),
                        _ => throw new ArgumentOutOfRangeException(
                            nameof(databaseType),
                            databaseType,
                            null
                        )
                    };
                }
            }
            case SchemaValueType.Number:
            {
                    return databaseType switch
                    {
                        DatabaseType.Postgres => "double precision",
                        DatabaseType.SQLite or DatabaseType.MsSql => SqlDataType.Float.ToString().ToUpperInvariant(),
                        DatabaseType.MySql or DatabaseType.MariaDb => MySqlDbType.Float.ToString().ToUpperInvariant(),
                        _ => throw new ArgumentOutOfRangeException(
nameof(databaseType),
databaseType,
null
),
                    };
                }
            case SchemaValueType.Integer:
            {
                    return databaseType switch
                    {
                        DatabaseType.Postgres => "integer",
                        DatabaseType.SQLite or DatabaseType.MsSql => SqlDataType.Int.ToString().ToUpperInvariant(),
                        DatabaseType.MySql or DatabaseType.MariaDb => "INT",
                        _ => throw new ArgumentOutOfRangeException(
nameof(databaseType),
databaseType,
null
),
                    };
                }
            case SchemaValueType.Null:
            {
                return ErrorCode_Sql.CouldNotCreateTable
                    .ToErrorBuilder("Sql does not support null data type");
            }
            default: throw new ArgumentOutOfRangeException(type.ToString());
        }
    }
}
