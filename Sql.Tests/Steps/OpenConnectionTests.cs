using System.Collections.Generic;
using Reductech.EDR.Connectors.Sql.Steps;
using Reductech.EDR.Core;
using Reductech.EDR.Core.TestHarness;
using Reductech.EDR.Core.Util;
using static Reductech.EDR.Core.TestHarness.StaticHelpers;

namespace Reductech.EDR.Connectors.Sql.Tests.Steps
{

public partial class OpenConnectionTests : StepTestBase<OpenConnection, Unit>
{
    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            var connection = new DatabaseConnectionMetadata()
            {
                ConnectionString =
                    "Server=Server;Port=1234;Database=Database;Uid=UserName;Pwd=Password;",
                DatabaseType = DatabaseType.MySql
            }.ConvertToEntity();

            yield return new StepCase(
                "Open Connection",
                new OpenConnection() { Connection = Constant(connection) },
                Unit.Default
            ).WithExpectedFinalState(
                DatabaseConnectionMetadata.DatabaseConnectionVariableName.Name,
                connection
            );
        }
    }
}

}
