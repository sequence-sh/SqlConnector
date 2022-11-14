using FluentAssertions;
using Xunit.Abstractions;

namespace Sequence.Connectors.Sql.Tests;

[AutoTheory.UseTestOutputHelper]
public partial class ExtensionTests
{
    [AutoTheory.GenerateTheory("CheckSQLObjectNamesAreCorrect")]
    public IEnumerable<CheckSqlObjectNameTest> CheckSQLObjectNames
    {
        get

        {
            yield return new CheckSqlObjectNameTest("",         null);
            yield return new CheckSqlObjectNameTest("Hello",    "Hello");
            yield return new CheckSqlObjectNameTest("_Hello",   null);
            yield return new CheckSqlObjectNameTest("Hello1",   "Hello1");
            yield return new CheckSqlObjectNameTest("Hello@_$", "Hello@_$");
            yield return new CheckSqlObjectNameTest("Hello^",   null);
        }
    }

    public record CheckSqlObjectNameTest
        (string TableName, string? Expected) : AutoTheory.ITestInstance
    {
        /// <inheritdoc />
        public void Run(ITestOutputHelper testOutputHelper)
        {
            var result = Extensions.CheckSqlObjectName(TableName);

            if (Expected is not null)
            {
                result.ShouldBeSuccessful();
                result.Value.Should().Be(Expected);
            }
            else
            {
                result.ShouldBeFailure();

                result.Error.AsString.Should().Be($"Invalid Name for a SQL object: '{TableName}'");
            }
        }

        /// <inheritdoc />
        public string Name => TableName;
    }
}
