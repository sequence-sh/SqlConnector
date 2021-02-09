using System.Collections.Generic;
using Reductech.EDR.Core;
using Reductech.EDR.Core.TestHarness;

namespace Reductech.EDR.Connectors.Sql.Tests
{

public partial class
    CreateConnectionStringTests : StepTestBase<CreateConnectionString, StringStream>
{
    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            yield return new StepCase(
                "Create connection string",
                new CreateConnectionString()
                {
                    Database = StaticHelpers.Constant("Database"),
                    Password = StaticHelpers.Constant("Password"),
                    Server   = StaticHelpers.Constant("Server"),
                    UserName = StaticHelpers.Constant("UserName"),
                },
                "Server=Server;Database=Database;User Id=UserName;Password=Password;"
            );
        }
    }
}

}
