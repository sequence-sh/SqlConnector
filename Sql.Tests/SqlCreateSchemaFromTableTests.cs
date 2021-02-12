using System.Collections.Generic;
using System.Data;
using Reductech.EDR.Connectors.Sql.Steps;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Entities;
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
                AllowExtraProperties = false,
                Name                 = "MyTable",
                Properties = new Dictionary<string, SchemaProperty>()
                {
                    {
                        "Id",
                        new SchemaProperty()
                        {
                            Type         = SchemaPropertyType.Integer,
                            Multiplicity = Multiplicity.ExactlyOne
                        }
                    },
                    {
                        "Name",
                        new SchemaProperty()
                        {
                            Type         = SchemaPropertyType.String,
                            Multiplicity = Multiplicity.UpToOne
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

            DataTable dt = new();
            dt.Clear();
            dt.Columns.Add("COLUMN_NAME");
            dt.Columns.Add("IS_NULLABLE");
            dt.Columns.Add("DATA_TYPE");
            dt.Rows.Add("Id",   "NO",  "int");
            dt.Rows.Add("Name", "YES", "nvarchar");

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
                            dt
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
