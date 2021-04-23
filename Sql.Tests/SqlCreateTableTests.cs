using System;
using System.Collections.Generic;
using Reductech.EDR.Connectors.Sql.Steps;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Entities;
using Reductech.EDR.Core.Enums;
using Reductech.EDR.Core.Internal.Errors;
using Reductech.EDR.Core.TestHarness;
using Reductech.EDR.Core.Util;
using static Reductech.EDR.Core.TestHarness.StaticHelpers;

namespace Reductech.EDR.Connectors.Sql.Tests
{

public partial class SqlCreateTableTests : StepTestBase<SqlCreateTable, Unit>
{
    private static Schema TestSchema => new()
    {
        Name            = "MyTable",
        ExtraProperties = ExtraPropertyBehavior.Fail,
        Properties = new Dictionary<string, SchemaProperty>
        {
            {
                "Id",
                new SchemaProperty
                {
                    Type = SCLType.Integer, Multiplicity = Multiplicity.ExactlyOne
                }
            },
            {
                "Name",
                new SchemaProperty
                {
                    Type = SCLType.String, Multiplicity = Multiplicity.UpToOne
                }
            },
        }
    };

    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            yield return new StepCase(
                        "Create a table",
                        new SqlCreateTable
                        {
                            ConnectionString = Constant("MyConnectionString"),
                            DatabaseType     = Constant(DatabaseType.SQLite),
                            Schema           = Constant(TestSchema.ConvertToEntity())
                        },
                        Unit.Default,
                        "Command executed with 1 rows affected."
                    )
                    .WithContextMock(
                        DbConnectionFactory.DbConnectionName,
                        mr =>
                            DbMockHelper.SetupConnectionFactoryForCommand(
                                mr,
                                DatabaseType.SQLite,
                                "MyConnectionString",
                                "CREATE TABLE \"MyTable\" (\r\n\"Id\" INT NOT NULL\r\n,\"Name\" NTEXT NULL\r\n)\r\n",
                                1
                            )
                    )
                ;
        }
    }

    /// <inheritdoc />
    protected override IEnumerable<ErrorCase> ErrorCases
    {
        get
        {
            yield return new ErrorCase(
                "External context not set",
                new SqlCreateTable
                {
                    ConnectionString = Constant(@"My Connection String"),
                    Schema           = Constant(TestSchema.ConvertToEntity()),
                    DatabaseType     = Constant(DatabaseType.SQLite)
                },
                ErrorCode.MissingContext.ToErrorBuilder(nameof(IDbConnectionFactory))
            );

            yield return new ErrorCase(
                "Sql Error",
                new SqlCreateTable
                {
                    ConnectionString = Constant(@"My Connection String"),
                    Schema           = Constant(TestSchema.ConvertToEntity()),
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
                            "CREATE TABLE \"MyTable\" (\r\n\"Id\" INT NOT NULL\r\n,\"Name\" NTEXT NULL\r\n)\r\n",
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
