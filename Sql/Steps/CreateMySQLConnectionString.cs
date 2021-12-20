using Reductech.Sequence.Core.Internal.Errors;
using Entity = Reductech.Sequence.Core.Entity;

namespace Reductech.Sequence.Connectors.Sql.Steps;

/// <summary>
/// Create a database connection string for a MySQL database.
/// </summary>
public sealed class CreateMySQLConnectionString : CompoundStep<Entity>
{
    /// <inheritdoc />
    protected override async Task<Result<Entity, IError>> Run(
        IStateMonad stateMonad,
        CancellationToken cancellationToken)
    {
        var server = await Server.Run(stateMonad, cancellationToken)
            .Map(x => x.GetStringAsync());

        if (server.IsFailure)
            return server.ConvertFailure<Entity>();

        var port = await Port.Run(stateMonad, cancellationToken);

        if (port.IsFailure)
            return port.ConvertFailure<Entity>();

        var database =
            await Database.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (database.IsFailure)
            return database.ConvertFailure<Entity>();

        var username = await UId.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (username.IsFailure)
            return username.ConvertFailure<Entity>();

        var password = await Pwd.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (password.IsFailure)
            return password.ConvertFailure<Entity>();

        var connectionString =
            $"Server={server.Value};Port={port.Value.Value};Database={database.Value};Uid={username.Value};Pwd={password.Value};";

        var databaseConnection = new DatabaseConnectionMetadata
        {
            ConnectionString = connectionString, DatabaseType = DatabaseType.MySql
        };

        var entity = databaseConnection.ConvertToEntity();

        return entity;
    }

    /// <summary>
    /// The server address
    /// </summary>
    [StepProperty(1)]
    [Required]
    public IStep<StringStream> Server { get; set; } = null!;

    /// <summary>
    /// The database to run the query against
    /// </summary>
    [StepProperty(2)]
    [Required]
    [Alias("Db")]
    public IStep<StringStream> Database { get; set; } = null!;

    /// <summary>
    /// The username for database access.
    /// </summary>
    [StepProperty(3)]
    [Required]
    [Alias("UserId")]
    [Alias("Username")]
    public IStep<StringStream> UId { get; set; } = null!;

    /// <summary>
    /// The password for database access.
    /// </summary>
    [StepProperty(4)]
    [Required]
    [Alias("Password")]
    public IStep<StringStream> Pwd { get; set; } = null!;

    /// <summary>
    /// The server port
    /// </summary>
    [StepProperty(5)]
    [DefaultValueExplanation("3306")]
    public IStep<SCLInt> Port { get; set; } = new SCLConstant<SCLInt>(3306.ConvertToSCLObject());

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<CreateMySQLConnectionString, Entity>();
}
