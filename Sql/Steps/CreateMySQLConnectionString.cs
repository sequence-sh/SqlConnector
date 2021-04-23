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

public sealed class CreateMySQLConnectionString : CompoundStep<StringStream>
{
    /// <inheritdoc />
    protected override async Task<Result<StringStream, IError>> Run(
        IStateMonad stateMonad,
        CancellationToken cancellationToken)
    {
        var server = await Server.Run(stateMonad, cancellationToken)
            .Map(x => x.GetStringAsync());

        if (server.IsFailure)
            return server.ConvertFailure<StringStream>();

        var port = await Port.Run(stateMonad, cancellationToken);

        if (port.IsFailure)
            return port.ConvertFailure<StringStream>();

        var database =
            await Database.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (database.IsFailure)
            return database.ConvertFailure<StringStream>();

        var username = await UId.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (username.IsFailure)
            return username.ConvertFailure<StringStream>();

        var password = await Pwd.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (password.IsFailure)
            return password.ConvertFailure<StringStream>();

        var s = new StringStream(
            $"Server={server.Value};Port={port.Value};Database={database.Value};Uid={username.Value};Pwd={password.Value};"
        );

        return s;
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
    public IStep<int> Port { get; set; } = new IntConstant(3306);

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<CreateMySQLConnectionString, StringStream>();
}

}
