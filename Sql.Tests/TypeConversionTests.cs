using System;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using Json.Schema;
using Microsoft.SqlServer.Management.SqlParser.Metadata;
using Reductech.EDR.Core;
using Reductech.EDR.Core.TestHarness;
using Xunit.Abstractions;
using static Reductech.EDR.Core.TestHarness.SchemaHelpers;

namespace Reductech.EDR.Connectors.Sql.Tests
{

[AutoTheory.UseTestOutputHelper]
public partial class TypeConversionTests
{
    [AutoTheory.GenerateTheory("CheckGetDataType")]
    public IEnumerable<GetDataTypeTest> GetDataTypeTests
    {
        get
        {
            yield return new GetDataTypeTest(AnyString, DatabaseType.Postgres, "text");
            yield return new GetDataTypeTest(AnyInt,    DatabaseType.Postgres, "integer");

            yield return new GetDataTypeTest(
                AnyNumber,
                DatabaseType.Postgres,
                "double precision"
            );

            yield return new GetDataTypeTest(EnumProperty("a", "b"), DatabaseType.Postgres, "text");
            yield return new GetDataTypeTest(AnyBool, DatabaseType.Postgres, "boolean");
            yield return new GetDataTypeTest(AnyDateTime, DatabaseType.Postgres, "date");
            yield return new GetDataTypeTest(AnyEntity, DatabaseType.Postgres, null);

            foreach (var dbType in new[] { DatabaseType.MsSql, DatabaseType.SQLite })
            {
                yield return new GetDataTypeTest(AnyString,              dbType, "NTEXT");
                yield return new GetDataTypeTest(AnyInt,                 dbType, "INT");
                yield return new GetDataTypeTest(AnyNumber,              dbType, "FLOAT");
                yield return new GetDataTypeTest(EnumProperty("a", "b"), dbType, "NTEXT");
                yield return new GetDataTypeTest(AnyBool,                dbType, "BIT");
                yield return new GetDataTypeTest(AnyDateTime,            dbType, "DATETIME2");
                yield return new GetDataTypeTest(AnyEntity,              dbType, null);
            }

            foreach (var dbType in new[] { DatabaseType.MySql, DatabaseType.MariaDb })
            {
                yield return new GetDataTypeTest(AnyString,              dbType, "TEXT");
                yield return new GetDataTypeTest(AnyInt,                 dbType, "INT");
                yield return new GetDataTypeTest(AnyNumber,              dbType, "FLOAT");
                yield return new GetDataTypeTest(EnumProperty("a", "b"), dbType, "TEXT");
                yield return new GetDataTypeTest(AnyBool,                dbType, "BIT");
                yield return new GetDataTypeTest(AnyDateTime,            dbType, "DATETIME");
                yield return new GetDataTypeTest(AnyEntity,              dbType, null);
            }
        }
    }

    [AutoTheory.GenerateTheory("TestConvertDataType")]
    public IEnumerable<ConvertSQLDataTypeTest> ConvertDataTypeTests
    {
        get
        {
            foreach (var sqlDataType in Enum.GetValues<SqlDataType>())
            {
                SCLType? expectedSCLType = sqlDataType switch
                {
                    SqlDataType.None => null,
                    SqlDataType.BigInt => SCLType.Integer,
                    SqlDataType.Binary => null,
                    SqlDataType.Bit => SCLType.Bool,
                    SqlDataType.Char => SCLType.String,
                    SqlDataType.Date => SCLType.Date,
                    SqlDataType.DateTime => SCLType.Date,
                    SqlDataType.DateTime2 => SCLType.Date,
                    SqlDataType.DateTimeOffset => SCLType.Date,
                    SqlDataType.Decimal => SCLType.Double,
                    SqlDataType.Float => SCLType.Double,
                    SqlDataType.Geography => null,
                    SqlDataType.Geometry => null,
                    SqlDataType.HierarchyId => null,
                    SqlDataType.Image => null,
                    SqlDataType.Int => SCLType.Integer,
                    SqlDataType.Money => SCLType.Double,
                    SqlDataType.NChar => SCLType.String,
                    SqlDataType.NText => SCLType.String,
                    SqlDataType.Numeric => SCLType.Double,
                    SqlDataType.NVarChar => SCLType.String,
                    SqlDataType.NVarCharMax => SCLType.String,
                    SqlDataType.Real => SCLType.Double,
                    SqlDataType.SmallDateTime => SCLType.Date,
                    SqlDataType.SmallInt => SCLType.Integer,
                    SqlDataType.SmallMoney => SCLType.Double,
                    SqlDataType.SysName => SCLType.String,
                    SqlDataType.Text => SCLType.String,
                    SqlDataType.Time => SCLType.Date,
                    SqlDataType.Timestamp => SCLType.Date,
                    SqlDataType.TinyInt => SCLType.Integer,
                    SqlDataType.UniqueIdentifier => null,
                    SqlDataType.VarBinary => null,
                    SqlDataType.VarBinaryMax => null,
                    SqlDataType.VarChar => SCLType.String,
                    SqlDataType.VarCharMax => SCLType.String,
                    SqlDataType.Variant => SCLType.String,
                    SqlDataType.Xml => SCLType.String,
                    SqlDataType.XmlNode => null,
                    _ => throw new ArgumentOutOfRangeException(sqlDataType.ToString())
                };

                var testCase = new ConvertSQLDataTypeTest(
                    sqlDataType.ToString(),
                    DatabaseType.MsSql,
                    expectedSCLType
                );

                yield return testCase;
            }

            var pg = DatabaseType.Postgres;
            yield return new ConvertSQLDataTypeTest("bigint",            pg, SCLType.Integer);
            yield return new ConvertSQLDataTypeTest("bit",               pg, SCLType.Bool);
            yield return new ConvertSQLDataTypeTest("bit varying",       pg, SCLType.Bool);
            yield return new ConvertSQLDataTypeTest("boolean",           pg, SCLType.Bool);
            yield return new ConvertSQLDataTypeTest("char",              pg, SCLType.String);
            yield return new ConvertSQLDataTypeTest("character varying", pg, SCLType.String);
            yield return new ConvertSQLDataTypeTest("character",         pg, SCLType.String);
            yield return new ConvertSQLDataTypeTest("varchar",           pg, SCLType.String);
            yield return new ConvertSQLDataTypeTest("date",              pg, SCLType.Date);
            yield return new ConvertSQLDataTypeTest("double precision",  pg, SCLType.Double);
            yield return new ConvertSQLDataTypeTest("integer",           pg, SCLType.Integer);
            yield return new ConvertSQLDataTypeTest("numeric",           pg, SCLType.Double);
            yield return new ConvertSQLDataTypeTest("decimal",           pg, SCLType.Double);
            yield return new ConvertSQLDataTypeTest("real",              pg, SCLType.Double);
            yield return new ConvertSQLDataTypeTest("smallint",          pg, SCLType.Integer);
            yield return new ConvertSQLDataTypeTest("text",              pg, SCLType.String);
            yield return new ConvertSQLDataTypeTest("time",              pg, SCLType.Date);
            yield return new ConvertSQLDataTypeTest("timestamp",         pg, SCLType.Date);
        }
    }

    public record ConvertSQLDataTypeTest
    (
        String DataTypeString,
        DatabaseType DatabaseType,
        SCLType? ExpectedSCLType) : AutoTheory.ITestInstance
    {
        /// <inheritdoc />
        public void Run(ITestOutputHelper testOutputHelper)
        {
            var actual = TypeConversion.TryConvertDataType(DataTypeString, "column", DatabaseType);

            if (ExpectedSCLType.HasValue)
            {
                actual.ShouldBeSuccessful();
                actual.Value.Should().Be(ExpectedSCLType.Value);
            }
            else
            {
                actual.ShouldBeFailure();
            }
        }

        /// <inheritdoc />
        public string Name => $"{DataTypeString}-{DatabaseType}";
    }

    public record GetDataTypeTest(
        JsonSchema Schema,
        DatabaseType DatabaseType,
        string? ExpectedOutput) : AutoTheory.ITestInstance
    {
        /// <inheritdoc />
        public void Run(ITestOutputHelper testOutputHelper)
        {
            var actualType = TypeConversion.TryGetDataType(Schema, DatabaseType);

            if (ExpectedOutput is null)
            {
                actualType.ShouldBeFailure();
            }
            else
            {
                actualType.ShouldBeSuccessful();
                actualType.Value.Should().Be(ExpectedOutput);
            }
        }

        public string SchemaName
        {
            get
            {
                var type = Schema.Keywords!.OfType<TypeKeyword>()
                    .Select(x => x.Type.ToString())
                    .FirstOrDefault();

                var format = Schema.Keywords!.OfType<FormatKeyword>()
                    .Select(x => x.Value.Key)
                    .FirstOrDefault();

                var enumValues =
                    string.Join(
                        ",",
                        Schema.Keywords!.OfType<EnumKeyword>()
                            .SelectMany(x => x.Values)
                    );

                var data = $"{type}{format}{enumValues}";
                return data;
            }
        }

        /// <inheritdoc />
        public string Name => $"{SchemaName}-{DatabaseType}";
    }
}

}
