using System.Linq;
using Json.Schema;
using Microsoft.SqlServer.Management.SqlParser.Metadata;
using MySqlConnector;
using Reductech.EDR.Core.Internal.Errors;

namespace Reductech.EDR.Connectors.Sql;

internal static class TypeConversion
{
    internal static Result<SCLType, IErrorBuilder> TryConvertDataType(
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

    internal static Result<SCLType, IErrorBuilder> TryConvertPostgresDataType(
        string dataType,
        string column)
    {
        return dataType.ToLowerInvariant() switch //This list is not exhaustive
        {
            "bigint" => SCLType.Integer,
            "bit" => SCLType.Bool,
            "bit varying" => SCLType.Bool,
            "boolean" => SCLType.Bool,
            "char" => SCLType.String,
            "character varying" => SCLType.String,
            "character" => SCLType.String,
            "varchar" => SCLType.String,
            "date" => SCLType.Date,
            "double precision" => SCLType.Double,
            "integer" => SCLType.Integer,
            "numeric" => SCLType.Double,
            "decimal" => SCLType.Double,
            "real" => SCLType.Double,
            "smallint" => SCLType.Integer,
            "text" => SCLType.String,
            "time" => SCLType.Date,
            "timestamp" => SCLType.Date,
            _ => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(dataType, column)
        };
    }

    internal static Result<SCLType, IErrorBuilder> TryConvertSqlDataType(
        SqlDataType dataType,
        string column)
    {
        return dataType switch
        {
            SqlDataType.None => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                dataType,
                column
            ),
            SqlDataType.BigInt => SCLType.Integer,
            SqlDataType.Binary => ErrorCode_Sql.CouldNotHandleDataType.ToErrorBuilder(
                dataType,
                column
            ),
            SqlDataType.Bit            => SCLType.Bool,
            SqlDataType.Char           => SCLType.String,
            SqlDataType.Date           => SCLType.Date,
            SqlDataType.DateTime       => SCLType.Date,
            SqlDataType.DateTime2      => SCLType.Date,
            SqlDataType.DateTimeOffset => SCLType.Date,
            SqlDataType.Decimal        => SCLType.Double,
            SqlDataType.Float          => SCLType.Double,
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
            SqlDataType.Int           => SCLType.Integer,
            SqlDataType.Money         => SCLType.Double,
            SqlDataType.NChar         => SCLType.String,
            SqlDataType.NText         => SCLType.String,
            SqlDataType.Numeric       => SCLType.Double,
            SqlDataType.NVarChar      => SCLType.String,
            SqlDataType.NVarCharMax   => SCLType.String,
            SqlDataType.Real          => SCLType.Double,
            SqlDataType.SmallDateTime => SCLType.Date,
            SqlDataType.SmallInt      => SCLType.Integer,
            SqlDataType.SmallMoney    => SCLType.Double,
            SqlDataType.SysName       => SCLType.String,
            SqlDataType.Text          => SCLType.String,
            SqlDataType.Time          => SCLType.Date,
            SqlDataType.Timestamp     => SCLType.Date,
            SqlDataType.TinyInt       => SCLType.Integer,
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
            SqlDataType.VarChar    => SCLType.String,
            SqlDataType.VarCharMax => SCLType.String,
            SqlDataType.Variant    => SCLType.String,
            SqlDataType.Xml        => SCLType.String,
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
                switch (databaseType)
                {
                    case DatabaseType.Postgres: return "boolean";

                    case DatabaseType.SQLite:
                    case DatabaseType.MsSql:
                        return SqlDataType.Bit.ToString().ToUpperInvariant();

                    case DatabaseType.MySql:
                    case DatabaseType.MariaDb:
                        return MySqlDbType.Bit.ToString().ToUpperInvariant();
                    default:
                        throw new ArgumentOutOfRangeException(
                            nameof(databaseType),
                            databaseType,
                            null
                        );
                }
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
                switch (databaseType)
                {
                    case DatabaseType.Postgres: return "double precision";

                    case DatabaseType.SQLite:
                    case DatabaseType.MsSql:
                        return SqlDataType.Float.ToString().ToUpperInvariant();

                    case DatabaseType.MySql:
                    case DatabaseType.MariaDb:
                        return MySqlDbType.Float.ToString().ToUpperInvariant();
                    default:
                        throw new ArgumentOutOfRangeException(
                            nameof(databaseType),
                            databaseType,
                            null
                        );
                }
            }
            case SchemaValueType.Integer:
            {
                switch (databaseType)
                {
                    case DatabaseType.Postgres: return "integer";

                    case DatabaseType.SQLite:
                    case DatabaseType.MsSql:
                        return SqlDataType.Int.ToString().ToUpperInvariant();

                    case DatabaseType.MySql:
                    case DatabaseType.MariaDb:
                        return "INT";
                    default:
                        throw new ArgumentOutOfRangeException(
                            nameof(databaseType),
                            databaseType,
                            null
                        );
                }
            }
            case SchemaValueType.Null:
            {
                return ErrorCode_Sql.CouldNotCreateTable
                    .ToErrorBuilder("Sql does not support null data type");
            }
            default: throw new ArgumentOutOfRangeException();
        }
    }
}
