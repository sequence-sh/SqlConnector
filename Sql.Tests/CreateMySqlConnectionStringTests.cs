using System.Collections.Generic;
using Reductech.EDR.Connectors.Sql.Steps;
using Reductech.EDR.Core;
using Reductech.EDR.Core.TestHarness;

namespace Reductech.EDR.Connectors.Sql.Tests
{

public partial class
    CreateMySqlConnectionStringTests : StepTestBase<CreateMySQLConnectionString, Entity>
{
    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            yield return new StepCase(
                "Create MySQL connection string",
                new CreateMySQLConnectionString()
                {
                    Database = StaticHelpers.Constant("Database"),
                    Pwd      = StaticHelpers.Constant("Password"),
                    Server   = StaticHelpers.Constant("Server"),
                    UId      = StaticHelpers.Constant("UserName"),
                    Port     = StaticHelpers.Constant(1234)
                },
                new DatabaseConnection()
                {
                    ConnectionString =
                        "Server=Server;Port=1234;Database=Database;Uid=UserName;Pwd=Password;",
                    DatabaseType = DatabaseType.MsSql
                }.ConvertToEntity()
            );
        }
    }
}

}
