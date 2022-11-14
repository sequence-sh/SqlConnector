using System.Linq;
using CSharpFunctionalExtensions;
using FluentAssertions;
using Json.Schema;
using Microsoft.SqlServer.Management.SqlParser.Metadata;
using Xunit.Abstractions;
using static Sequence.Core.TestHarness.SchemaHelpers;

namespace Sequence.Connectors.Sql.Tests;

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
                TypeReference.Actual? expectedSCLType = sqlDataType switch
                {
                    SqlDataType.None => null,
                    SqlDataType.BigInt => TypeReference.Actual.Integer,
                    SqlDataType.Binary => null,
                    SqlDataType.Bit => TypeReference.Actual.Bool,
                    SqlDataType.Char => TypeReference.Actual.String,
                    SqlDataType.Date => TypeReference.Actual.Date,
                    SqlDataType.DateTime => TypeReference.Actual.Date,
                    SqlDataType.DateTime2 => TypeReference.Actual.Date,
                    SqlDataType.DateTimeOffset => TypeReference.Actual.Date,
                    SqlDataType.Decimal => TypeReference.Actual.Double,
                    SqlDataType.Float => TypeReference.Actual.Double,
                    SqlDataType.Geography => null,
                    SqlDataType.Geometry => null,
                    SqlDataType.HierarchyId => null,
                    SqlDataType.Image => null,
                    SqlDataType.Int => TypeReference.Actual.Integer,
                    SqlDataType.Money => TypeReference.Actual.Double,
                    SqlDataType.NChar => TypeReference.Actual.String,
                    SqlDataType.NText => TypeReference.Actual.String,
                    SqlDataType.Numeric => TypeReference.Actual.Double,
                    SqlDataType.NVarChar => TypeReference.Actual.String,
                    SqlDataType.NVarCharMax => TypeReference.Actual.String,
                    SqlDataType.Real => TypeReference.Actual.Double,
                    SqlDataType.SmallDateTime => TypeReference.Actual.Date,
                    SqlDataType.SmallInt => TypeReference.Actual.Integer,
                    SqlDataType.SmallMoney => TypeReference.Actual.Double,
                    SqlDataType.SysName => TypeReference.Actual.String,
                    SqlDataType.Text => TypeReference.Actual.String,
                    SqlDataType.Time => TypeReference.Actual.Date,
                    SqlDataType.Timestamp => TypeReference.Actual.Date,
                    SqlDataType.TinyInt => TypeReference.Actual.Integer,
                    SqlDataType.UniqueIdentifier => null,
                    SqlDataType.VarBinary => null,
                    SqlDataType.VarBinaryMax => null,
                    SqlDataType.VarChar => TypeReference.Actual.String,
                    SqlDataType.VarCharMax => TypeReference.Actual.String,
                    SqlDataType.Variant => TypeReference.Actual.String,
                    SqlDataType.Xml => TypeReference.Actual.String,
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
            yield return new ConvertSQLDataTypeTest("bigint",            pg, TypeReference.Actual.Integer);
            yield return new ConvertSQLDataTypeTest("bit",               pg, TypeReference.Actual.Bool);
            yield return new ConvertSQLDataTypeTest("bit varying",       pg, TypeReference.Actual.Bool);
            yield return new ConvertSQLDataTypeTest("boolean",           pg, TypeReference.Actual.Bool);
            yield return new ConvertSQLDataTypeTest("char",              pg, TypeReference.Actual.String);
            yield return new ConvertSQLDataTypeTest("character varying", pg, TypeReference.Actual.String);
            yield return new ConvertSQLDataTypeTest("character",         pg, TypeReference.Actual.String);
            yield return new ConvertSQLDataTypeTest("varchar",           pg, TypeReference.Actual.String);
            yield return new ConvertSQLDataTypeTest("date",              pg, TypeReference.Actual.Date);
            yield return new ConvertSQLDataTypeTest("double precision",  pg, TypeReference.Actual.Double);
            yield return new ConvertSQLDataTypeTest("integer",           pg, TypeReference.Actual.Integer);
            yield return new ConvertSQLDataTypeTest("numeric",           pg, TypeReference.Actual.Double);
            yield return new ConvertSQLDataTypeTest("decimal",           pg, TypeReference.Actual.Double);
            yield return new ConvertSQLDataTypeTest("real",              pg, TypeReference.Actual.Double);
            yield return new ConvertSQLDataTypeTest("smallint",          pg, TypeReference.Actual.Integer);
            yield return new ConvertSQLDataTypeTest("text",              pg, TypeReference.Actual.String);
            yield return new ConvertSQLDataTypeTest("time",              pg, TypeReference.Actual.Date);
            yield return new ConvertSQLDataTypeTest("timestamp",         pg, TypeReference.Actual.Date);
        }
    }

    public record ConvertSQLDataTypeTest
    (
        string DataTypeString,
        DatabaseType DatabaseType,
        TypeReference.Actual? ExpectedSCLType) : AutoTheory.ITestInstance
    {
        /// <inheritdoc />
        public void Run(ITestOutputHelper testOutputHelper)
        {
            var actual = TypeConversion.TryConvertDataType(DataTypeString, "column", DatabaseType);

            if (ExpectedSCLType is not null)
            {
                actual.ShouldBeSuccessful();
                actual.Value.Should().Be(ExpectedSCLType);
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
