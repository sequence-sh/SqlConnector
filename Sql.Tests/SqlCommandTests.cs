using System.Collections.Generic;
using Reductech.EDR.Connectors.Sql.Steps;
using Reductech.EDR.Core.TestHarness;
using Reductech.EDR.Core.Util;
using static Reductech.EDR.Core.TestHarness.StaticHelpers;

namespace Reductech.EDR.Connectors.Sql.Tests
{

public partial class SqlCommandTests : StepTestBase<SqlCommand, Unit>
{
    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            yield return new StepCase(
                "Sql Lite command",
                new SqlCommand()
                {
                    ConnectionString = Constant(@"My Connection String"),
                    Command          = Constant(@"My Command String"),
                    DatabaseType     = Constant(DatabaseType.SQLite)
                },
                Unit.Default,
                "Command executed with 5 rows affected."
            ).WithContextMock(
                DbConnectionFactory.DbConnectionName,
                mr =>
                {
                    var factory =
                        DbMockHelper.SetupConnectionFactoryForCommand(
                            mr,
                            DatabaseType.SQLite,
                            "My Connection String",
                            "My Command String",
                            5
                        );

                    return factory;
                }
            );
        }
    }
}

}
