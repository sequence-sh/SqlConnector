using Reductech.Sequence.Core.Internal.Errors;
using Entity = Reductech.Sequence.Core.Entity;

namespace Reductech.Sequence.Connectors.Sql.Steps;

/// <summary>
/// Creates an MSSQL connection string
/// </summary>
public sealed class CreateMsSQLConnectionString : CompoundStep<Entity>
{
    private static string EscapeString(string s)
    {
        return "\"" + s.Replace("\"", "\"\"") + "\"";
    }

    /// <inheritdoc />
    protected override async Task<Result<Entity, IError>> Run(
        IStateMonad stateMonad,
        CancellationToken cancellationToken)
    {
        var result = await
            stateMonad.RunStepsAsync(
                Server.WrapStringStream(),
                Database.WrapStringStream(),
                UserName.WrapNullable(StepMaps.String()),
                Password.WrapNullable(StepMaps.String()),
                AttachDbFilename.WrapNullable(StepMaps.String()),
                Authentication.WrapNullable(StepMaps.String()),
                Encrypt,
                IntegratedSecurity,
                TrustServerCertificate,
                cancellationToken
            );

        if (result.IsFailure)
            return result.ConvertFailure<Entity>();

        var (server, db, user, pass, attachDbFilename, authentication, encrypt,
                integratedSecurity, trustServerCertificate)
            = result.Value;

        var cs = $"Server={EscapeString(server)};Database={EscapeString(db)};";

        if (user.HasValue || pass.HasValue)
        {
            if (user.HasNoValue || string.IsNullOrEmpty(user.Value))
                return new SingleError(
                    new ErrorLocation(this),
                    ErrorCode.MissingParameter,
                    nameof(UserName)
                );

            if (pass.HasNoValue || string.IsNullOrEmpty(pass.Value))
                return new SingleError(
                    new ErrorLocation(this),
                    ErrorCode.MissingParameter,
                    nameof(Password)
                );

            cs += $"User Id={EscapeString(user.Value)};Password={EscapeString(pass.Value)};";
        }
        else
        {
            integratedSecurity = SCLBool.True;
        }

        if (integratedSecurity.Value)
        {
            cs += "Integrated Security=true;";
        }

        if (encrypt.Value)
        {
            cs += "Encrypt=true;";
        }

        if (trustServerCertificate.Value)
        {
            cs += "TrustServerCertificate=true;";
        }

        if (attachDbFilename.HasValue)
        {
            cs += $"AttachDbFileName={EscapeString(attachDbFilename.Value)};";
        }

        if (authentication.HasValue)
        {
            cs += $"Authentication={EscapeString(attachDbFilename.Value)};";
        }

        var databaseConnection = new DatabaseConnectionMetadata()
        {
            ConnectionString = cs, DatabaseType = DatabaseType.MsSql
        };

        var entity = databaseConnection.ConvertToEntity();

        return entity;
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

    /// <summary>
    /// The name of the primary database file, including the full path name of an attachable database.
    /// AttachDBFilename is only supported for primary data files with an .mdf extension.
    /// </summary>
    [StepProperty]
    [DefaultValueExplanation("None")]
    [Alias("ExtendedProperties")]
    [Alias("InitialFileName")]
    public IStep<StringStream>? AttachDbFilename { get; set; } = null;

    /// <summary>
    /// The authentication method used for Connecting to SQL Database By Using Azure Active Directory Authentication.
    /// Valid values are: Active Directory Integrated, Active Directory Password, Sql Password.
    /// </summary>
    [StepProperty]
    [DefaultValueExplanation("None")]
    public IStep<StringStream>? Authentication { get; set; } = null;

    /// <summary>
    /// When true, SQL Server uses SSL encryption for all data sent between the client and server if the server has a certificate installed.
    /// Recognized values are true, false, yes, and no.
    /// For more information, see Connection String Syntax.
    /// </summary>
    [StepProperty]
    [DefaultValueExplanation("False")]
    public IStep<SCLBool> Encrypt { get; set; } = new SCLConstant<SCLBool>(SCLBool.False);

    /// <summary>
    /// When false, User ID and Password are specified in the connection.
    /// When true, the current Windows account credentials are used for authentication.
    /// </summary>
    [StepProperty]
    [DefaultValueExplanation("True if Username and Password are not set")]
    [Alias("TrustedConnection")]
    public IStep<SCLBool> IntegratedSecurity { get; set; } =
        new SCLConstant<SCLBool>(SCLBool.False);

    /// <summary>
    /// When set to true, SSL is used to encrypt the channel when bypassing walking the certificate chain to validate trust.
    /// If TrustServerCertificate is set to true and Encrypt is set to false, the channel is not encrypted.
    /// </summary>
    [StepProperty]
    [DefaultValueExplanation("False")]
    public IStep<SCLBool> TrustServerCertificate { get; set; } =
        new SCLConstant<SCLBool>(SCLBool.False);

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<CreateMsSQLConnectionString, Entity>();
}
