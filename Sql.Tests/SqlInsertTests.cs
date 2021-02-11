using System.Collections.Generic;
using Reductech.EDR.Core;
using Reductech.EDR.Core.TestHarness;
using Reductech.EDR.Core.Util;

namespace Reductech.EDR.Connectors.Sql.Tests
{

public partial class SqlInsertTests : StepTestBase<SqlInsert, Unit>
{
    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            yield return new StepCase(
                "Insert One Entity",
                new SqlInsert()
                {
                    ConnectionString = StaticHelpers.Constant("My Connection String"),
                    Table = StaticHelpers.Constant("My Table"),
                    DatabaseType = StaticHelpers.Constant(DatabaseType.SQLite),
                    Entities = StaticHelpers.Array(Entity.Create(("Id", 123), ("Name", "Mark")))
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
