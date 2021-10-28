using System.Collections.Generic;
using System.Data;
using Json.More;
using Json.Schema;
using Reductech.EDR.Connectors.Sql.Steps;
using Reductech.EDR.Core;
using Reductech.EDR.Core.TestHarness;
using static Reductech.EDR.Core.TestHarness.StaticHelpers;
using static Reductech.EDR.Core.TestHarness.SchemaHelpers;
using static Reductech.EDR.Connectors.Sql.Tests.StaticHelpers;

namespace Reductech.EDR.Connectors.Sql.Tests.Steps
{

public partial class SqlCreateSchemaFromTableTests : StepTestBase<SqlCreateSchemaFromTable, Entity>
{
    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            var expectedSchema = new JsonSchemaBuilder()
                .Title("MyTable")
                .AdditionalProperties(JsonSchema.False)
                .Properties(("Id", AnyInt), ("Name", AnyString))
                .Required("Id")
                .Build();

            yield return new StepCase(
                    "Simple SQLite case",
                    new SqlCreateSchemaFromTable()
                    {
                        Connection = GetConnectionMetadata(DatabaseType.SQLite),
                        Table      = Constant("MyTable"),
                    },
                    Entity.Create(expectedSchema.ToJsonDocument().RootElement)
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
                    Entity.Create(expectedSchema.ToJsonDocument().RootElement)
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

            DataTable postgresDataTable = new();
            postgresDataTable.Clear();
            postgresDataTable.Columns.Add("column_name");
            postgresDataTable.Columns.Add("is_nullable");
            postgresDataTable.Columns.Add("data_type");
            postgresDataTable.Rows.Add("Id",   "NO",  "integer");
            postgresDataTable.Rows.Add("Name", "YES", "text");

            yield return new StepCase(
                    "Simple Postgres Case",
                    new SqlCreateSchemaFromTable()
                    {
                        Connection = GetConnectionMetadata(DatabaseType.Postgres),
                        Table      = Constant("MyTable")
                    },
                    Entity.Create(expectedSchema.ToJsonDocument().RootElement)
                ).WithDbConnectionInState(DatabaseType.Postgres)
                .WithContextMock(
                    DbConnectionFactory.DbConnectionName,
                    mr =>
                        DbMockHelper.SetupConnectionFactoryForQuery(
                            mr,
                            DatabaseType.Postgres,
                            TestConnectionString,
                            "SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE  from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = 'MyTable'",
                            postgresDataTable
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

            yield return new ErrorCase(
                "Invalid table name",
                new SqlCreateSchemaFromTable()
                {
                    Table      = Constant("Bad^Table^Name"),
                    Connection = GetConnectionMetadata(DatabaseType.SQLite),
                },
                ErrorCode_Sql.InvalidName.ToErrorBuilder("Bad^Table^Name")
            );

            yield return new ErrorCase(
                "Invalid postgres schema name",
                new SqlCreateSchemaFromTable()
                {
                    Table          = Constant("MyTable"),
                    PostgresSchema = Constant("Bad^Postgres^Schema"),
                    Connection     = GetConnectionMetadata(DatabaseType.SQLite),
                },
                ErrorCode_Sql.InvalidName.ToErrorBuilder("Bad^Postgres^Schema")
            ) { IgnoreFinalState = true };

            foreach (var errorCase in base.ErrorCases)
                yield return errorCase;
        }
    }
}

}
