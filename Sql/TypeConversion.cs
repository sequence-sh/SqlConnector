using System;
using CSharpFunctionalExtensions;
using Microsoft.SqlServer.Management.SqlParser.Metadata;
using MySqlConnector;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Internal.Errors;

namespace Reductech.EDR.Connectors.Sql
{

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
        SCLType schemaPropertyType,
        DatabaseType databaseType)
    {
        switch (databaseType)
        {
            case DatabaseType.Postgres:
            {
                return schemaPropertyType switch
                {
                    SCLType.String  => "text",
                    SCLType.Integer => "integer",
                    SCLType.Double  => "double precision",
                    SCLType.Enum    => "text",
                    SCLType.Bool    => "boolean",
                    SCLType.Date    => "date",
                    SCLType.Entity => ErrorCode_Sql.CouldNotCreateTable
                        .ToErrorBuilder("Sql does not support nested entities"),
                    _ => throw new ArgumentOutOfRangeException(
                        nameof(schemaPropertyType),
                        schemaPropertyType,
                        null
                    )
                };
            }

            case DatabaseType.SQLite:
            case DatabaseType.MsSql:
            {
                Result<SqlDataType, IErrorBuilder> sqlDbType = schemaPropertyType switch
                {
                    SCLType.String  => SqlDataType.NText,
                    SCLType.Integer => SqlDataType.Int,
                    SCLType.Double  => SqlDataType.Float,
                    SCLType.Enum    => SqlDataType.NText,
                    SCLType.Bool    => SqlDataType.Bit,
                    SCLType.Date    => SqlDataType.DateTime2,
                    SCLType.Entity => ErrorCode_Sql.CouldNotCreateTable
                        .ToErrorBuilder("Sql does not support nested entities"),
                    _ => throw new ArgumentOutOfRangeException(
                        nameof(schemaPropertyType),
                        schemaPropertyType,
                        null
                    )
                };

                return sqlDbType.Map(x => x.ToString().ToUpperInvariant());
            }
            case DatabaseType.MySql:
            case DatabaseType.MariaDb:
            {
                Result<MySqlDbType, IErrorBuilder> sqlDbType = schemaPropertyType switch
                {
                    SCLType.String  => MySqlDbType.Text,
                    SCLType.Integer => MySqlDbType.Int32,
                    SCLType.Double  => MySqlDbType.Float,
                    SCLType.Enum    => MySqlDbType.Text,
                    SCLType.Bool    => MySqlDbType.Bit,
                    SCLType.Date    => MySqlDbType.DateTime,
                    SCLType.Entity => ErrorCode_Sql.CouldNotCreateTable
                        .ToErrorBuilder("Sql does not support nested entities"),
                    _ => throw new ArgumentOutOfRangeException(
                        nameof(schemaPropertyType),
                        schemaPropertyType,
                        null
                    )
                };

                return sqlDbType.Map(
                    x => x == MySqlDbType.Int32 ? "INT" : x.ToString().ToUpperInvariant()
                );
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(databaseType), databaseType, null);
        }
    }
}

}
