using Reductech.EDR.Core;
using Reductech.EDR.Core.TestHarness;
using System.Collections.Generic;
using System.Data;
using Reductech.EDR.Core.Steps;
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
                    DatabaseType     = Constant(DatabaseType.SqlLite)
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
                            DatabaseType.SqlLite,
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

public partial class SqlQueryTests : StepTestBase<SqlQuery, Array<Entity>>
{
    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            DataTable dt = new();
            dt.Clear();
            dt.Columns.Add("Name");
            dt.Columns.Add("Id");
            dt.Rows.Add("Mark", 500);
            dt.Rows.Add("Ruth", 501);

            var stepCase = new StepCase(
                        "Connect to SQL Lite",
                        new ForEach<Entity>()
                        {
                            Array = new SqlQuery()
                            {
                                ConnectionString = Constant(@"My Connection String"),
                                Query            = Constant(@"My Query String"),
                                DatabaseType     = Constant(DatabaseType.SqlLite)
                            },
                            Action = new Print<Entity>() { Value = GetEntityVariable }
                        },
                        Unit.Default
                    )
                    .WithConsoleAction(
                        x => x.Setup(c => c.WriteLine("(Name: \"Mark\" Id: \"500\")"))
                    )
                    .WithConsoleAction(
                        x => x.Setup(c => c.WriteLine("(Name: \"Ruth\" Id: \"501\")"))
                    )
                    .WithContextMock(
                        DbConnectionFactory.DbConnectionName,
                        mr =>
                        {
                            var factory =
                                DbMockHelper.SetupConnectionFactoryForQuery(
                                    mr,
                                    DatabaseType.SqlLite,
                                    "My Connection String",
                                    "My Query String",
                                    dt
                                );

                            return factory;
                        }
                    )
                ;

            yield return stepCase;
        }
    }
}

}
