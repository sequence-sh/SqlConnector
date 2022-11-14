using Sequence.Connectors.Sql.Steps;

namespace Sequence.Connectors.Sql.Tests.Steps;

public partial class OpenConnectionTests : StepTestBase<OpenConnection, Unit>
{
    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            var connection = new DatabaseConnectionMetadata()
            {
                ConnectionString =
                    "Server=Server;Port=1234;Database=Database;Uid=UserName;Pwd=Password;",
                DatabaseType = DatabaseType.MySql
            }.ConvertToEntity();

            yield return new StepCase(
                "Open Connection",
                new OpenConnection() { Connection = Constant(connection) },
                Unit.Default
            ).WithExpectedFinalState(
                DatabaseConnectionMetadata.DatabaseConnectionVariableName.Name,
                connection
            );
        }
    }
}
