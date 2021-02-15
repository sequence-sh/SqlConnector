using System.ComponentModel.DataAnnotations;
using System.Threading;
using System.Threading.Tasks;
using CSharpFunctionalExtensions;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Attributes;
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Internal.Errors;

namespace Reductech.EDR.Connectors.Sql.Steps
{

public sealed class CreatePostgresConnectionString : CompoundStep<StringStream>
{
    /// <inheritdoc />
    protected override async Task<Result<StringStream, IError>> Run(
        IStateMonad stateMonad,
        CancellationToken cancellationToken)
    {
        var host = await Host.Run(stateMonad, cancellationToken);

        if (host.IsFailure)
            return host.ConvertFailure<StringStream>();

        var port = await Port.Run(stateMonad, cancellationToken);

        if (port.IsFailure)
            return port.ConvertFailure<StringStream>();

        var database = await Database.Run(stateMonad, cancellationToken);

        if (database.IsFailure)
            return database.ConvertFailure<StringStream>();

        var userId = await UserId.Run(stateMonad, cancellationToken);

        if (userId.IsFailure)
            return userId.ConvertFailure<StringStream>();

        var password = await Password.Run(stateMonad, cancellationToken);

        if (password.IsFailure)
            return password.ConvertFailure<StringStream>();

        var result =
            $"User ID={userId.Value};Password={password.Value};Host={host.Value};Port={port.Value};Database={database.Value};";

        return new StringStream(result);
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
    [StepProperty(1)]
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
    public IStep<StringStream> UserId { get; set; } = null!;

    /// <summary>
    /// The password for database access.
    /// </summary>
    [StepProperty(5)]
    public IStep<StringStream> Password { get; set; } = null!;

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<CreatePostgresConnectionString, StringStream>();
}

}
