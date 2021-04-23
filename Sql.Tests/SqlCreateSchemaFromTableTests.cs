using System.Collections.Generic;
using System.Data;
using Reductech.EDR.Connectors.Sql.Steps;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Entities;
using Reductech.EDR.Core.Enums;
using Reductech.EDR.Core.TestHarness;
using static Reductech.EDR.Core.TestHarness.StaticHelpers;

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
                        ConnectionString = Constant("MyConnectionString"),
                        Table            = Constant("MyTable"),
                        DatabaseType     = Constant(DatabaseType.SQLite),
                    },
                    expectedSchema
                        .ConvertToEntity()
                )
                .WithContextMock(
                    DbConnectionFactory.DbConnectionName,
                    mr =>
                        DbMockHelper.SetupConnectionFactoryForScalarQuery(
                            mr,
                            DatabaseType.SQLite,
                            "MyConnectionString",
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
                        ConnectionString = Constant("MyConnectionString"),
                        Table            = Constant("MyTable"),
                        DatabaseType     = Constant(DatabaseType.MsSql),
                    },
                    expectedSchema
                        .ConvertToEntity()
                )
                .WithContextMock(
                    DbConnectionFactory.DbConnectionName,
                    mr =>
                        DbMockHelper.SetupConnectionFactoryForQuery(
                            mr,
                            DatabaseType.MsSql,
                            "MyConnectionString",
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
                        ConnectionString = Constant("MyConnectionString"),
                        Table            = Constant("MyTable"),
                        DatabaseType     = Constant(DatabaseType.Postgres),
                    },
                    expectedSchema
                        .ConvertToEntity()
                )
                .WithContextMock(
                    DbConnectionFactory.DbConnectionName,
                    mr =>
                        DbMockHelper.SetupConnectionFactoryForQuery(
                            mr,
                            DatabaseType.Postgres,
                            "MyConnectionString",
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
                    ConnectionString = Constant("MyConnectionString"),
                    Table            = Constant("MyTable"),
                    DatabaseType     = Constant(DatabaseType.SQLite),
                },
                ErrorCode_Sql.CouldNotGetCreateTable.ToErrorBuilder("MyTable")
            ).WithContextMock(
                DbConnectionFactory.DbConnectionName,
                mr =>
                    DbMockHelper.SetupConnectionFactoryForScalarQuery(
                        mr,
                        DatabaseType.SQLite,
                        "MyConnectionString",
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
