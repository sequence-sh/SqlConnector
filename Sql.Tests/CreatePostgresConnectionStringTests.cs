using System.Collections.Generic;
using Reductech.EDR.Connectors.Sql.Steps;
using Reductech.EDR.Core;
using Reductech.EDR.Core.TestHarness;

namespace Reductech.EDR.Connectors.Sql.Tests
{

public partial class
    CreatePostgresConnectionStringTests : StepTestBase<CreatePostgresConnectionString, StringStream>
{
    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            yield return new StepCase(
                "Create Postgres connection string",
                new CreatePostgresConnectionString
                {
                    Database = StaticHelpers.Constant("Database"),
                    Password = StaticHelpers.Constant("Password"),
                    Host     = StaticHelpers.Constant("Host"),
                    Port     = StaticHelpers.Constant(123),
                    UserId   = StaticHelpers.Constant("User")
                },
                "User ID=User;Password=Password;Host=Host;Port=123;Database=Database;"
            );
        }
    }
}

}
