using System.Collections.Generic;
using Reductech.EDR.Connectors.Sql.Steps;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Internal.Errors;
using Reductech.EDR.Core.TestHarness;
using static Reductech.EDR.Core.TestHarness.StaticHelpers;

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
                "Create connection string with username and password",
                new CreateConnectionString
                {
                    Database = Constant("Database"),
                    Password = Constant("Password"),
                    Server   = Constant("Server"),
                    UserName = Constant("UserName"),
                },
                "Server=Server;Database=Database;User Id=UserName;Password=Password;"
            );

            yield return new StepCase(
                "Create connection string with integrated security",
                new CreateConnectionString
                {
                    Database = Constant("Database"), Server = Constant("Server"),
                },
                "Server=Server;Database=Database;Integrated Security=true;"
            );
        }
    }

    /// <inheritdoc />
    protected override IEnumerable<ErrorCase> ErrorCases
    {
        get
        {
            yield return new ErrorCase(
                "Missing password",
                new CreateConnectionString
                {
                    Server   = Constant("Server"),
                    Database = Constant("Database"),
                    UserName = Constant("Username"),
                    Password = null
                },
                ErrorCode.MissingParameter.ToErrorBuilder(nameof(CreateConnectionString.Password))
            );

            yield return new ErrorCase(
                "Missing username",
                new CreateConnectionString
                {
                    Server   = Constant("Server"),
                    Database = Constant("Database"),
                    UserName = null,
                    Password = Constant("Password")
                },
                ErrorCode.MissingParameter.ToErrorBuilder(nameof(CreateConnectionString.UserName))
            );

            foreach (var errorCase in base.ErrorCases)
                yield return errorCase;
        }
    }
}

}
