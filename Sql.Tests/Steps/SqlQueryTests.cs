using System.Data;
using Reductech.EDR.Connectors.Sql.Steps;
using Reductech.EDR.Core.Steps;
using static Reductech.EDR.Connectors.Sql.Tests.StaticHelpers;

namespace Reductech.EDR.Connectors.Sql.Tests.Steps;

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
                                Connection = GetConnectionMetadata(DatabaseType.SQLite),
                                Query      = Constant(@"My Query String")
                            },
                            Action = new LambdaFunction<Entity, Unit>(
                                null,
                                new Print() { Value = GetEntityVariable }
                            )
                        },
                        Unit.Default
                    ).WithDbConnectionInState(DatabaseType.SQLite)
                    .WithConsoleAction(
                        x => x.Setup(c => c.WriteLine("('Name': \"Mark\" 'Id': \"500\")"))
                    )
                    .WithConsoleAction(
                        x => x.Setup(c => c.WriteLine("('Name': \"Ruth\" 'Id': \"501\")"))
                    )
                    .WithContextMock(
                        DbConnectionFactory.DbConnectionName,
                        mr =>
                        {
                            var factory =
                                DbMockHelper.SetupConnectionFactoryForQuery(
                                    mr,
                                    DatabaseType.SQLite,
                                    TestConnectionString,
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

    /// <inheritdoc />
    protected override IEnumerable<ErrorCase> ErrorCases
    {
        get
        {
            yield return new ErrorCase(
                        "Sql Error",
                        new SqlQuery()
                        {
                            Connection = GetConnectionMetadata(DatabaseType.SQLite),
                            Query      = Constant(@"My Query String")
                        },
                        ErrorCode_Sql.SqlError.ToErrorBuilder("Test Error")
                    ).WithDbConnectionInState(DatabaseType.SQLite)
                    .WithContextMock(
                        DbConnectionFactory.DbConnectionName,
                        mr =>
                        {
                            var factory =
                                DbMockHelper.SetupConnectionFactoryErrorForQuery(
                                    mr,
                                    DatabaseType.SQLite,
                                    TestConnectionString,
                                    "My Query String",
                                    new Exception("Test Error")
                                );

                            return factory;
                        }
                    )
                ;

            foreach (var errorCase in base.ErrorCases)
                yield return errorCase;
        }
    }
}
