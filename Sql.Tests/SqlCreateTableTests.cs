using System.Collections.Generic;
using Reductech.EDR.Connectors.Sql.Steps;
using Reductech.EDR.Core.Entities;
using Reductech.EDR.Core.TestHarness;
using Reductech.EDR.Core.Util;

namespace Reductech.EDR.Connectors.Sql.Tests
{

public partial class SqlCreateTableTests : StepTestBase<SqlCreateTable, Unit>
{
    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            yield return new StepCase(
                        "Create a table",
                        new SqlCreateTable()
                        {
                            ConnectionString = StaticHelpers.Constant("MyConnectionString"),
                            DatabaseType     = StaticHelpers.Constant(DatabaseType.SQLite),
                            Schema = StaticHelpers.Constant(
                                new Schema()
                                {
                                    Name                 = "MyTable",
                                    AllowExtraProperties = false,
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
                                        },
                                    }
                                }.ConvertToEntity()
                            )
                        },
                        Unit.Default,
                        "Command executed with 1 rows affected."
                    )
                    .WithContextMock(
                        DbConnectionFactory.DbConnectionName,
                        mr =>
                            DbMockHelper.SetupConnectionFactoryForCommand(
                                mr,
                                DatabaseType.SQLite,
                                "MyConnectionString",
                                "CREATE TABLE \"MyTable\" (\r\n\"Id\" INT NOT NULL\r\n,\"Name\" NTEXT NULL\r\n)\r\n",
                                1
                            )
                    )
                ;
        }
    }
}

}
