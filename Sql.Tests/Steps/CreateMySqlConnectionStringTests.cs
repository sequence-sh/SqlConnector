using Reductech.Sequence.Connectors.Sql.Steps;

namespace Reductech.Sequence.Connectors.Sql.Tests.Steps;

public partial class
    CreateMySqlConnectionStringTests : StepTestBase<CreateMySQLConnectionString, Entity>
{
    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            yield return new StepCase(
                "Create MySQL connection string",
                new CreateMySQLConnectionString()
                {
                    Database = Constant("Database"),
                    Pwd      = Constant("Password"),
                    Server   = Constant("Server"),
                    UId      = Constant("UserName"),
                    Port     = Constant(1234)
                },
                new DatabaseConnectionMetadata()
                {
                    ConnectionString =
                        "Server=Server;Port=1234;Database=Database;Uid=UserName;Pwd=Password;",
                    DatabaseType = DatabaseType.MySql
                }.ConvertToEntity()
            );
        }
    }
}
