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

/// <summary>
/// Creates an MSSQL connection string
/// </summary>
public sealed class CreateMsSQLConnectionString : CompoundStep<StringStream>
{
    /// <inheritdoc />
    protected override async Task<Result<StringStream, IError>> Run(
        IStateMonad stateMonad,
        CancellationToken cancellationToken)
    {
        var server = await Server.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (server.IsFailure)
            return server.ConvertFailure<StringStream>();

        var db = await Database.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (db.IsFailure)
            return db.ConvertFailure<StringStream>();

        var cs = $"Server={server.Value};Database={db.Value};";

        string? user = null;

        if (UserName != null)
        {
            var userResult = await UserName.Run(stateMonad, cancellationToken)
                .Map(x => x.GetStringAsync());

            if (userResult.IsFailure)
                return userResult.ConvertFailure<StringStream>();

            user = userResult.Value;
        }

        string? pass = null;

        if (Password != null)
        {
            var passResult = await Password.Run(stateMonad, cancellationToken)
                .Map(x => x.GetStringAsync());

            if (passResult.IsFailure)
                return passResult.ConvertFailure<StringStream>();

            pass = passResult.Value;
        }

        if (!string.IsNullOrEmpty(user) || !string.IsNullOrEmpty(pass))
        {
            if (string.IsNullOrEmpty(user))
                return new SingleError(
                    new StepErrorLocation(this),
                    ErrorCode.MissingParameter,
                    nameof(UserName)
                );

            if (string.IsNullOrEmpty(pass))
                return new SingleError(
                    new StepErrorLocation(this),
                    ErrorCode.MissingParameter,
                    nameof(Password)
                );

            cs += $"User Id={user};Password={pass};";
        }
        else
        {
            cs += "Integrated Security=true;";
        }

        return new StringStream(cs);
    }

    /// <summary>
    /// The server address (and port)
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
    [DefaultValueExplanation("Use integrated security if not set.")]
    public IStep<StringStream>? UserName { get; set; } = null;

    /// <summary>
    /// The password for database access.
    /// </summary>
    [StepProperty(4)]
    [DefaultValueExplanation("Use integrated security if not set.")]
    public IStep<StringStream>? Password { get; set; } = null;

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<CreateMsSQLConnectionString, StringStream>();
}

}
