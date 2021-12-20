using System.Linq;
using System.Text.RegularExpressions;
using CSharpFunctionalExtensions;
using FluentAssertions;
using Json.Schema;
using Reductech.Sequence.Connectors.Sql.Steps;
using Reductech.Sequence.Core.Entities;
using Xunit.Abstractions;
using static Reductech.Sequence.Core.TestHarness.SchemaHelpers;
using static Reductech.Sequence.Connectors.Sql.Tests.StaticHelpers;

namespace Reductech.Sequence.Connectors.Sql.Tests.Steps;

public partial class SqlCreateTableTests : StepTestBase<SqlCreateTable, Unit>
{
    private static JsonSchema TestSchema => new JsonSchemaBuilder()
        .Title("MyTable")
        .AdditionalProperties(JsonSchema.False)
        .Properties(("Id", AnyInt), ("Name", AnyString))
        .Required("Id")
        .Build();

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
            yield return new GetCommandTest(
                new JsonSchemaBuilder()
                    .Title("SQLiteTable")
                    .AdditionalProperties(JsonSchema.False)
                    .Required("MyColumn")
                    .Properties(("MyColumn", AnyString)),
                DatabaseType.SQLite,
                @"CREATE TABLE ""SQLiteTable"" ( ""MyColumn"" NTEXT NOT NULL )"
            );

            yield return new GetCommandTest(
                new JsonSchemaBuilder()
                    .Title("MsSQLTable")
                    .Required("MyColumn")
                    .AdditionalProperties(JsonSchema.False)
                    .Properties(("MyColumn", AnyString)),
                DatabaseType.MsSql,
                @"CREATE TABLE ""MsSQLTable"" (
""MyColumn"" NTEXT NOT NULL
)"
            );

            yield return new GetCommandTest(
                new JsonSchemaBuilder()
                    .Title("PostgresTable")
                    .Required("MyColumn")
                    .AdditionalProperties(JsonSchema.False)
                    .Properties(("MyColumn", AnyString)),
                DatabaseType.Postgres,
                @"CREATE TABLE ""PostgresTable"" ( ""MyColumn"" text NOT NULL )"
            );

            yield return new GetCommandTest(
                new JsonSchemaBuilder()
                    .Title("MySQLTable")
                    .Required("MyColumn")
                    .AdditionalProperties(JsonSchema.False)
                    .Properties(("MyColumn", AnyString)),
                DatabaseType.MySql,
                @"CREATE TABLE MySQLTable (
MyColumn TEXT NOT NULL
)"
            );

            yield return new GetCommandTest(
                new JsonSchemaBuilder()
                    .Title("MariaDbTable")
                    .Required("MyColumn")
                    .AdditionalProperties(JsonSchema.False)
                    .Properties(("MyColumn", AnyString)),
                DatabaseType.MariaDb,
                "CREATE TABLE MariaDbTable ( MyColumn TEXT NOT NULL )"
            );

            //ERROR CASES

            yield return new GetCommandTest(
                new JsonSchemaBuilder()
                    .Title("Bad^Table^Name"),
                DatabaseType.MySql,
                ErrorCode_Sql.InvalidName.ToErrorBuilder("Bad^Table^Name")
            );

            yield return new GetCommandTest(
                new JsonSchemaBuilder()
                    .Title("BadColumnNameTable")
                    .AdditionalProperties(false)
                    .Properties(("Bad^Column^Name", AnyString)),
                DatabaseType.MySql,
                ErrorCode_Sql.InvalidName.ToErrorBuilder("Bad^Column^Name")
            );

            yield return new GetCommandTest(
                new JsonSchemaBuilder()
                    .Title("BadDataTypeTable")
                    .AdditionalProperties(JsonSchema.False)
                    .Properties(("BadDataTypeColumn", AnyEntity)),
                DatabaseType.MySql,
                ErrorCode_Sql.CouldNotCreateTable
                    .ToErrorBuilder("Sql does not support nested entities")
            );

            yield return new GetCommandTest(
                new JsonSchemaBuilder()
                    .Title("ArrayPropertyTable")
                    .AdditionalProperties(JsonSchema.False)
                    .Properties(("ArrayColumn", AnyArray)),
                DatabaseType.MySql,
                ErrorCode_Sql.CouldNotCreateTable.ToErrorBuilder(
                    $"Sql does not support nested lists"
                )
            );
        }
    }

    public record GetCommandTest(
        JsonSchema Schema,
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

                GetMessages(result.Error)
                    .Should()
                    .BeEquivalentTo(GetMessages(ExpectedResult.Error));

                result.Error.Should().Be(ExpectedResult.Error);

                static IReadOnlyCollection<string> GetMessages(IErrorBuilder errorBuilder)
                {
                    return errorBuilder.GetErrorBuilders().Select(x => x.AsString).ToList();
                }
            }
        }

        /// <inheritdoc />
        public string Name =>
            $"{Schema.Keywords!.OfType<TitleKeyword>().Single().Value} {DatabaseType}";

        public string SchemaName
        {
            get
            {
                var type = Schema.Keywords!.OfType<TypeKeyword>()
                    .Select(x => x.Type.ToString())
                    .FirstOrDefault();

                var format = Schema.Keywords!.OfType<FormatKeyword>()
                    .Select(x => x.Value.Key)
                    .FirstOrDefault();

                var enumValues =
                    string.Join(
                        ",",
                        Schema.Keywords!.OfType<EnumKeyword>()
                            .SelectMany(x => x.Values)
                    );

                var data = $"{type}{format}{enumValues}";
                return data;
            }
        }
    }
}
