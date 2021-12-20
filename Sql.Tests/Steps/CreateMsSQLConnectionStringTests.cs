using Reductech.Sequence.Connectors.Sql.Steps;

namespace Reductech.Sequence.Connectors.Sql.Tests.Steps;

public partial class
    CreateMsSQLConnectionStringTests : StepTestBase<CreateMsSQLConnectionString, Entity>
{
    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            yield return new StepCase(
                "Create connection string with username and password",
                new CreateMsSQLConnectionString
                {
                    Database = Constant("Database"),
                    Password = Constant("Password"),
                    Server   = Constant("Server"),
                    UserName = Constant("UserName"),
                },
                new DatabaseConnectionMetadata()
                {
                    ConnectionString =
                        "Server=Server;Database=Database;User Id=UserName;Password=Password;",
                    DatabaseType = DatabaseType.MsSql
                }.ConvertToEntity()
            );

            yield return new StepCase(
                "Create connection string with integrated security",
                new CreateMsSQLConnectionString
                {
                    Database = Constant("Database"), Server = Constant("Server"),
                },
                new DatabaseConnectionMetadata()
                {
                    ConnectionString =
                        "Server=Server;Database=Database;Integrated Security=true;",
                    DatabaseType = DatabaseType.MsSql
                }.ConvertToEntity()
            );
        }
    }

    /// <inheritdoc />
    protected override IEnumerable<ErrorCase> ErrorCases
    {
        get
        {
            yield return new ErrorCase(
                "Missing password",
                new CreateMsSQLConnectionString
                {
                    Server   = Constant("Server"),
                    Database = Constant("Database"),
                    UserName = Constant("Username"),
                    Password = null
                },
                ErrorCode.MissingParameter.ToErrorBuilder(
                    nameof(CreateMsSQLConnectionString.Password)
                )
            );

            yield return new ErrorCase(
                "Missing username",
                new CreateMsSQLConnectionString
                {
                    Server   = Constant("Server"),
                    Database = Constant("Database"),
                    UserName = null,
                    Password = Constant("Password")
                },
                ErrorCode.MissingParameter.ToErrorBuilder(
                    nameof(CreateMsSQLConnectionString.UserName)
                )
            );

            foreach (var errorCase in base.ErrorCases)
                yield return errorCase;
        }
    }
}
