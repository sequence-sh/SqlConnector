using Reductech.EDR.Core.Internal.Errors;
using Entity = Reductech.EDR.Core.Entity;

namespace Reductech.EDR.Connectors.Sql.Steps;

/// <summary>
/// Creates a PostgreSQL connection string
/// </summary>
public sealed class CreatePostgresConnectionString : CompoundStep<Entity>
{
    /// <inheritdoc />
    protected override async Task<Result<Entity, IError>> Run(
        IStateMonad stateMonad,
        CancellationToken cancellationToken)
    {
        var host = await Host.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (host.IsFailure)
            return host.ConvertFailure<Entity>();

        var port = await Port.Run(stateMonad, cancellationToken);

        if (port.IsFailure)
            return port.ConvertFailure<Entity>();

        var database =
            await Database.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (database.IsFailure)
            return database.ConvertFailure<Entity>();

        var userId = await UserId.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (userId.IsFailure)
            return userId.ConvertFailure<Entity>();

        var password =
            await Password.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (password.IsFailure)
            return password.ConvertFailure<Entity>();

        var connectionString =
            $"User ID={userId.Value};Password={password.Value};Host={host.Value};Port={port.Value};Database={database.Value};";

        var databaseConnection = new DatabaseConnectionMetadata()
        {
            ConnectionString = connectionString, DatabaseType = DatabaseType.Postgres
        };

        var entity = EntityConversionHelpers.ConvertToEntity(databaseConnection);

        return entity;
    }

    /// <summary>
    /// The host name
    /// </summary>
    [StepProperty(1)]
    [Required]
    public IStep<StringStream> Host { get; set; } = null!;

    /// <summary>
    /// The server port
    /// </summary>
    [StepProperty(2)]
    [Required]
    public IStep<int> Port { get; set; } = null!;

    /// <summary>
    /// The database to run the query against
    /// </summary>
    [StepProperty(3)]
    [Required]
    [Alias("Db")]
    public IStep<StringStream> Database { get; set; } = null!;

    /// <summary>
    /// The user id for database access
    /// </summary>
    [StepProperty(4)]
    [Required]
    public IStep<StringStream> UserId { get; set; } = null!;

    /// <summary>
    /// The password for database access.
    /// </summary>
    [StepProperty(5)]
    [Required]
    public IStep<StringStream> Password { get; set; } = null!;

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<CreatePostgresConnectionString, Entity>();
}
