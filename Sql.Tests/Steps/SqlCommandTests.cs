﻿using Sequence.Connectors.Sql.Steps;
using static Sequence.Connectors.Sql.Tests.StaticHelpers;

namespace Sequence.Connectors.Sql.Tests.Steps;

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
                        Connection = GetConnectionMetadata(DatabaseType.SQLite),
                        Command    = Constant(@"My Command String")
                    },
                    Unit.Default,
                    "Command executed with 5 rows affected."
                ).WithDbConnectionInState(DatabaseType.SQLite)
                .WithContextMock(
                    DbConnectionFactory.DbConnectionName,
                    mr =>
                    {
                        var factory =
                            DbMockHelper.SetupConnectionFactoryForCommand(
                                mr,
                                DatabaseType.SQLite,
                                TestConnectionString,
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
                        Connection = GetConnectionMetadata(DatabaseType.MsSql),
                        Command    = Constant(@"My Command String")
                    },
                    Unit.Default,
                    "Command executed with 5 rows affected."
                ).WithDbConnectionInState(DatabaseType.MsSql)
                .WithContextMock(
                    DbConnectionFactory.DbConnectionName,
                    mr =>
                    {
                        var factory =
                            DbMockHelper.SetupConnectionFactoryForCommand(
                                mr,
                                DatabaseType.MsSql,
                                TestConnectionString,
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
                    Connection = GetConnectionMetadata(DatabaseType.SQLite),
                    Command    = Constant(@"My Command String"),
                },
                ErrorCode.MissingContext.ToErrorBuilder(nameof(IDbConnectionFactory))
            );

            yield return new ErrorCase(
                    "Sql Error",
                    new SqlCommand
                    {
                        Connection = GetConnectionMetadata(DatabaseType.SQLite),
                        Command    = Constant(@"My Command String")
                    },
                    ErrorCode_Sql.SqlError.ToErrorBuilder("Test Error")
                ).WithDbConnectionInState(DatabaseType.SQLite)
                .WithContextMock(
                    DbConnectionFactory.DbConnectionName,
                    mr =>
                    {
                        var factory =
                            DbMockHelper.SetupConnectionFactoryErrorForCommand(
                                mr,
                                DatabaseType.SQLite,
                                TestConnectionString,
                                "My Command String",
                                new Exception("Test Error")
                            );

                        return factory;
                    }
                );

            foreach (var errorCase in base.ErrorCases)
            {
                if (errorCase.Name != "Test Error Message: 'Connection Error'")
                    yield return errorCase;
            }
        }
    }
}
