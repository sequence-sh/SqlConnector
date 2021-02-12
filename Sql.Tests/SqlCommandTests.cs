using System;
using System.Collections.Generic;
using Reductech.EDR.Connectors.Sql.Steps;
using Reductech.EDR.Core.Internal.Errors;
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

            yield return new StepCase(
                "MSSSQL command",
                new SqlCommand()
                {
                    ConnectionString = Constant(@"My Connection String"),
                    Command          = Constant(@"My Command String"),
                    DatabaseType     = Constant(DatabaseType.MsSql)
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
                            DatabaseType.MsSql,
                            "My Connection String",
                            "My Command String",
                            5
                        );

                    return factory;
                }
            );
        }
    }

    /// <inheritdoc />
    protected override IEnumerable<ErrorCase> ErrorCases
    {
        get
        {
            yield return new ErrorCase(
                "External context not set",
                new SqlCommand()
                {
                    ConnectionString = Constant(@"My Connection String"),
                    Command          = Constant(@"My Command String"),
                    DatabaseType     = Constant(DatabaseType.SQLite)
                },
                ErrorCode.MissingContext.ToErrorBuilder(nameof(IDbConnectionFactory))
            );

            yield return new ErrorCase(
                "Sql Error",
                new SqlCommand
                {
                    ConnectionString = Constant(@"My Connection String"),
                    Command          = Constant(@"My Command String"),
                    DatabaseType     = Constant(DatabaseType.SQLite)
                },
                ErrorCode_Sql.SqlError.ToErrorBuilder("Test Error")
            ).WithContextMock(
                DbConnectionFactory.DbConnectionName,
                mr =>
                {
                    var factory =
                        DbMockHelper.SetupConnectionFactoryErrorForCommand(
                            mr,
                            DatabaseType.SQLite,
                            "My Connection String",
                            "My Command String",
                            new Exception("Test Error")
                        );

                    return factory;
                }
            );

            foreach (var errorCase in base.ErrorCases)
                yield return errorCase;
        }
    }
}

}
