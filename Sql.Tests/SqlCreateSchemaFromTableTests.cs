using System.Collections.Generic;
using System.Data;
using Reductech.EDR.Connectors.Sql.Steps;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Entities;
using Reductech.EDR.Core.Enums;
using Reductech.EDR.Core.TestHarness;
using static Reductech.EDR.Core.TestHarness.StaticHelpers;
using static Reductech.EDR.Connectors.Sql.Tests.StaticHelpers;

namespace Reductech.EDR.Connectors.Sql.Tests
{

public partial class SqlCreateSchemaFromTableTests : StepTestBase<SqlCreateSchemaFromTable, Entity>
{
    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            var expectedSchema = new Schema()
            {
                ExtraProperties = ExtraPropertyBehavior.Fail,
                Name            = "MyTable",
                Properties = new Dictionary<string, SchemaProperty>()
                {
                    {
                        "Id",
                        new SchemaProperty()
                        {
                            Type = SCLType.Integer, Multiplicity = Multiplicity.ExactlyOne
                        }
                    },
                    {
                        "Name",
                        new SchemaProperty()
                        {
                            Type = SCLType.String, Multiplicity = Multiplicity.UpToOne
                        }
                    }
                }
            };

            yield return new StepCase(
                    "Simple SQLite case",
                    new SqlCreateSchemaFromTable()
                    {
                        Connection = GetConnectionMetadata(DatabaseType.SQLite),
                        Table      = Constant("MyTable"),
                    },
                    expectedSchema
                        .ConvertToEntity()
                ).WithDbConnectionInState(DatabaseType.SQLite)
                .WithContextMock(
                    DbConnectionFactory.DbConnectionName,
                    mr =>
                        DbMockHelper.SetupConnectionFactoryForScalarQuery(
                            mr,
                            DatabaseType.SQLite,
                            TestConnectionString,
                            "SELECT sql FROM SQLite_master WHERE name = 'MyTable';",
                            @"CREATE TABLE ""MyTable"" (
    ""Id"" INT NOT NULL PRIMARY KEY,
    ""Name"" TEXT NULL
)"
                        )
                );

            DataTable msSqlDataTable = new();
            msSqlDataTable.Clear();
            msSqlDataTable.Columns.Add("COLUMN_NAME");
            msSqlDataTable.Columns.Add("IS_NULLABLE");
            msSqlDataTable.Columns.Add("DATA_TYPE");
            msSqlDataTable.Rows.Add("Id",   "NO",  "int");
            msSqlDataTable.Rows.Add("Name", "YES", "nvarchar");

            yield return new StepCase(
                    "Simple MsSql case",
                    new SqlCreateSchemaFromTable()
                    {
                        Connection = GetConnectionMetadata(DatabaseType.MsSql),
                        Table      = Constant("MyTable"),
                    },
                    expectedSchema
                        .ConvertToEntity()
                ).WithDbConnectionInState(DatabaseType.MsSql)
                .WithContextMock(
                    DbConnectionFactory.DbConnectionName,
                    mr =>
                        DbMockHelper.SetupConnectionFactoryForQuery(
                            mr,
                            DatabaseType.MsSql,
                            TestConnectionString,
                            "SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE  from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = 'MyTable'",
                            msSqlDataTable
                        )
                );

            DataTable postGresDataTable = new();
            postGresDataTable.Clear();
            postGresDataTable.Columns.Add("column_name");
            postGresDataTable.Columns.Add("is_nullable");
            postGresDataTable.Columns.Add("data_type");
            postGresDataTable.Rows.Add("Id",   "NO",  "integer");
            postGresDataTable.Rows.Add("Name", "YES", "text");

            yield return new StepCase(
                    "Simple Postgres Case",
                    new SqlCreateSchemaFromTable()
                    {
                        Connection = GetConnectionMetadata(DatabaseType.Postgres),
                        Table      = Constant("MyTable")
                    },
                    expectedSchema
                        .ConvertToEntity()
                ).WithDbConnectionInState(DatabaseType.Postgres)
                .WithContextMock(
                    DbConnectionFactory.DbConnectionName,
                    mr =>
                        DbMockHelper.SetupConnectionFactoryForQuery(
                            mr,
                            DatabaseType.Postgres,
                            TestConnectionString,
                            "SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE  from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = 'MyTable'",
                            postGresDataTable
                        )
                );
        }
    }

    /// <inheritdoc />
    protected override IEnumerable<ErrorCase> ErrorCases
    {
        get
        {
            yield return new ErrorCase(
                    "Invalid Parse",
                    new SqlCreateSchemaFromTable()
                    {
                        Connection = GetConnectionMetadata(DatabaseType.SQLite),
                        Table      = Constant("MyTable"),
                    },
                    ErrorCode_Sql.CouldNotGetCreateTable.ToErrorBuilder("MyTable")
                ).WithDbConnectionInState(DatabaseType.SQLite)
                .WithContextMock(
                    DbConnectionFactory.DbConnectionName,
                    mr =>
                        DbMockHelper.SetupConnectionFactoryForScalarQuery(
                            mr,
                            DatabaseType.SQLite,
                            TestConnectionString,
                            "SELECT sql FROM SQLite_master WHERE name = 'MyTable';",
                            "This is not a create table statement"
                        )
                );

            foreach (var errorCase in base.ErrorCases)
                yield return errorCase;
        }
    }
}

}
