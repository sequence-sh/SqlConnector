using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text.RegularExpressions;
using CSharpFunctionalExtensions;
using FluentAssertions;
using Reductech.EDR.Connectors.Sql.Steps;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Entities;
using Reductech.EDR.Core.Enums;
using Reductech.EDR.Core.Internal.Errors;
using Reductech.EDR.Core.TestHarness;
using Reductech.EDR.Core.Util;
using Xunit.Abstractions;
using static Reductech.EDR.Core.TestHarness.StaticHelpers;
using static Reductech.EDR.Connectors.Sql.Tests.StaticHelpers;

namespace Reductech.EDR.Connectors.Sql.Tests.Steps
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
        }.ToImmutableSortedDictionary()
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
                            Connection = GetConnectionMetadata(DatabaseType.SQLite),
                            Schema     = Constant(TestSchema.ConvertToEntity())
                        },
                        Unit.Default,
                        "Command executed with 1 rows affected."
                    ).WithDbConnectionInState(DatabaseType.SQLite)
                    .WithContextMock(
                        DbConnectionFactory.DbConnectionName,
                        mr =>
                            DbMockHelper.SetupConnectionFactoryForCommand(
                                mr,
                                DatabaseType.SQLite,
                                TestConnectionString,
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
                    Connection = GetConnectionMetadata(DatabaseType.SQLite),
                    Schema     = Constant(TestSchema.ConvertToEntity()),
                },
                ErrorCode.MissingContext.ToErrorBuilder(nameof(IDbConnectionFactory))
            ).WithDbConnectionInState(DatabaseType.SQLite);

            yield return new ErrorCase(
                    "Sql Error",
                    new SqlCreateTable
                    {
                        Connection = GetConnectionMetadata(DatabaseType.SQLite),
                        Schema     = Constant(TestSchema.ConvertToEntity()),
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

    [AutoTheory.GenerateTheory("TestGetCommand")]
    public IEnumerable<GetCommandTest> GetCommandTests
    {
        get
        {
            static Schema Create(
                string tableName,
                Func<Schema, Schema>? transform,
                params (string name, SchemaProperty schemaProperty)[] schemaProperties)
            {
                Schema s = new()
                {
                    Name = tableName,
                    Properties = schemaProperties.ToDictionary(
                            x => x.name,
                            x => x.schemaProperty
                        )
                        .ToImmutableSortedDictionary(),
                    ExtraProperties = ExtraPropertyBehavior.Fail
                };

                if (transform is not null)
                    s = transform(s);

                return s;
            }

            yield return new GetCommandTest(
                Create(
                    "SQLiteTable",
                    null,
                    ("MyColumn",
                     new SchemaProperty()
                     {
                         Type = SCLType.String, Multiplicity = Multiplicity.ExactlyOne
                     })
                ),
                DatabaseType.SQLite,
                @"CREATE TABLE ""SQLiteTable"" ( ""MyColumn"" NTEXT NOT NULL )"
            );

            yield return new GetCommandTest(
                Create(
                    "MsSQLTable",
                    null,
                    ("MyColumn",
                     new SchemaProperty()
                     {
                         Type = SCLType.String, Multiplicity = Multiplicity.ExactlyOne
                     })
                ),
                DatabaseType.MsSql,
                @"CREATE TABLE ""MsSQLTable"" (
""MyColumn"" NTEXT NOT NULL
)"
            );

            yield return new GetCommandTest(
                Create(
                    "PostgresTable",
                    null,
                    ("MyColumn",
                     new SchemaProperty()
                     {
                         Type = SCLType.String, Multiplicity = Multiplicity.ExactlyOne
                     })
                ),
                DatabaseType.Postgres,
                @"CREATE TABLE ""PostgresTable"" ( ""MyColumn"" text NOT NULL )"
            );

            yield return new GetCommandTest(
                Create(
                    "MySQLTable",
                    null,
                    ("MyColumn",
                     new SchemaProperty()
                     {
                         Type = SCLType.String, Multiplicity = Multiplicity.ExactlyOne
                     })
                ),
                DatabaseType.MySql,
                @"CREATE TABLE MySQLTable (
MyColumn TEXT NOT NULL
)"
            );

            yield return new GetCommandTest(
                Create(
                    "MariaDbTable",
                    null,
                    ("MyColumn",
                     new SchemaProperty()
                     {
                         Type = SCLType.String, Multiplicity = Multiplicity.ExactlyOne
                     })
                ),
                DatabaseType.MariaDb,
                "CREATE TABLE MariaDbTable ( MyColumn TEXT NOT NULL )"
            );

            //ERROR CASES

            yield return new GetCommandTest(
                Create(
                    "AllowExtraProperties",
                    s => s with { ExtraProperties = ExtraPropertyBehavior.Allow }
                ),
                DatabaseType.MySql,
                ErrorCode_Sql.CouldNotCreateTable.ToErrorBuilder(
                    $"Schema has {nameof(Schema.ExtraProperties)} set to 'Allow'"
                )
            );

            yield return new GetCommandTest(
                Create(
                    "Bad^Table^Name",
                    null
                ),
                DatabaseType.MySql,
                ErrorCode_Sql.InvalidName.ToErrorBuilder("Bad^Table^Name")
            );

            yield return new GetCommandTest(
                Create(
                    "BadColumnNameTable",
                    null,
                    ("Bad^Column^Name",
                     new SchemaProperty()
                     {
                         Type = SCLType.String, Multiplicity = Multiplicity.ExactlyOne
                     })
                ),
                DatabaseType.MySql,
                ErrorCode_Sql.InvalidName.ToErrorBuilder("Bad^Column^Name")
            );

            yield return new GetCommandTest(
                Create(
                    "BadDataTypeTable",
                    null,
                    ("BadDataTypeColumn",
                     new SchemaProperty()
                     {
                         Type = SCLType.Entity, Multiplicity = Multiplicity.ExactlyOne
                     })
                ),
                DatabaseType.MySql,
                ErrorCode_Sql.CouldNotCreateTable
                    .ToErrorBuilder("Sql does not support nested entities")
            );

            yield return new GetCommandTest(
                Create(
                    "MultiplicityAnyTable",
                    null,
                    ("MultiplicityAnyColumn",
                     new SchemaProperty()
                     {
                         Type = SCLType.String, Multiplicity = Multiplicity.Any
                     })
                ),
                DatabaseType.MySql,
                ErrorCode_Sql.CouldNotCreateTable.ToErrorBuilder(
                    $"Sql does not support Multiplicity '{Multiplicity.Any}'"
                )
            );

            yield return new GetCommandTest(
                Create(
                    "MultiplicityAtLeastOneTable",
                    null,
                    ("MultiplicityAtLeastOneColumn",
                     new SchemaProperty()
                     {
                         Type = SCLType.String, Multiplicity = Multiplicity.AtLeastOne
                     })
                ),
                DatabaseType.MySql,
                ErrorCode_Sql.CouldNotCreateTable.ToErrorBuilder(
                    $"Sql does not support Multiplicity '{Multiplicity.AtLeastOne}'"
                )
            );
        }
    }

    public record GetCommandTest(
        Schema Schema,
        DatabaseType DatabaseType,
        Result<string, IErrorBuilder> ExpectedResult) : AutoTheory.ITestInstance
    {
        /// <inheritdoc />
        public void Run(ITestOutputHelper testOutputHelper)
        {
            var result = SqlCreateTable.GetCommandText(Schema, DatabaseType);

            if (ExpectedResult.IsSuccess)
            {
                result.ShouldBeSuccessful();
                TrimSpaces(result.Value).Should().Be(TrimSpaces(ExpectedResult.Value));

                static string TrimSpaces(string s)
                {
                    return Regex.Replace(s, @"\s+", " ").Trim();
                }
            }
            else
            {
                result.ShouldBeFailure();

                getMessages(result.Error)
                    .Should()
                    .BeEquivalentTo(getMessages(ExpectedResult.Error));

                result.Error.Should().Be(ExpectedResult.Error);

                IReadOnlyCollection<string> getMessages(IErrorBuilder errorBuilder)
                {
                    return errorBuilder.GetErrorBuilders().Select(x => x.AsString).ToList();
                }
            }
        }

        /// <inheritdoc />
        public string Name => $"{Schema.Name} {DatabaseType}";
    }
}

}
