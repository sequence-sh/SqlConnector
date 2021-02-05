using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using CSharpFunctionalExtensions;
using Reductech.EDR.Core.Attributes;
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Internal.Errors;

namespace Reductech.EDR.Connectors.Sql
{

using Reductech.EDR.Core;

/// <summary>
/// Executes a SQL query and returns the result as an entity stream.
/// </summary>
public sealed class SqlQuery : CompoundStep<Array<Entity>>
{
    /// <inheritdoc />
    protected override async Task<Result<Array<Entity>, IError>> Run(
        IStateMonad stateMonad,
        CancellationToken cancellationToken)
    {
        var server = await Server.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (server.IsFailure)
            return server.ConvertFailure<Array<Entity>>();

        var db = await Database.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (db.IsFailure)
            return db.ConvertFailure<Array<Entity>>();

        var query = await Query.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (query.IsFailure)
            return query.ConvertFailure<Array<Entity>>();

        var cs = $"Server={server};Database={db};";

        string? user = null;

        if (UserName != null)
        {
            var userResult = await UserName.Run(stateMonad, cancellationToken)
                .Map(x => x.GetStringAsync());

            if (userResult.IsFailure)
                return userResult.ConvertFailure<Array<Entity>>();

            user = userResult.Value;
        }

        string? pass = null;

        if (Password != null)
        {
            var passResult = await Password.Run(stateMonad, cancellationToken)
                .Map(x => x.GetStringAsync());

            if (passResult.IsFailure)
                return passResult.ConvertFailure<Array<Entity>>();

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
            cs += "IntegratedSecurity=true;";
        }

        await using var conn = new SqlConnection(cs);

        var command = new SqlCommand(query.Value, conn);

        await using var dbReader = await command.ExecuteReaderAsync(cancellationToken);

        return dbReader.HasRows
            ? GetEntityEnumerable(dbReader).ToSequence()
            : new Array<Entity>(Array.Empty<Entity>());

        async IAsyncEnumerable<Entity> GetEntityEnumerable(DbDataReader reader)
        {
            //var types = new Type?[reader.FieldCount];

            //for (var i = 0; i < reader.FieldCount; i++)
            //    types[i] = reader.GetFieldType(i);

            var row = new object[reader.FieldCount];

            while (await reader.ReadAsync(cancellationToken))
            {
                reader.GetValues(row);
                var props = new List<(string, object?)>(row.Length);

                for (var col = 0; col < row.Length; col++)
                    props.Add((reader.GetName(col), row[col]));

                yield return Entity.Create(props);
            }
        }
    }

    /// <summary>
    /// The server address (and port)
    /// </summary>
    [StepProperty(order: 1)]
    [Required]
    public IStep<StringStream> Server { get; set; } = null!;

    /// <summary>
    /// The database to run the query against
    /// </summary>
    [StepProperty(order: 2)]
    [Required]
    [Alias("Db")]
    public IStep<StringStream> Database { get; set; } = null!;

    /// <summary>
    /// The SQL query to run
    /// </summary>
    [StepProperty(order: 3)]
    [Required]
    [Alias("SQL")]
    public IStep<StringStream> Query { get; set; } = null!;

    /// <summary>
    /// The username for database access.
    /// </summary>
    [StepProperty(order: 4)]
    [DefaultValueExplanation("Use integrated security if not set.")]
    public IStep<StringStream>? UserName { get; set; } = null;

    /// <summary>
    /// The password for database access.
    /// </summary>
    [StepProperty(order: 5)]
    [DefaultValueExplanation("Use integrated security if not set.")]
    public IStep<StringStream>? Password { get; set; } = null;

    /// <inheritdoc />
    public override IStepFactory StepFactory => SqlQueryFactory.Instance;
}

/// <summary>
/// Executes a SQL query and returns the result as an entity stream.
/// </summary>
public sealed class SqlQueryFactory : SimpleStepFactory<SqlQuery, Array<Entity>>
{
    private SqlQueryFactory() { }

    /// <summary>
    /// An instance of SqlQueryFactory
    /// </summary>
    public static SimpleStepFactory<SqlQuery, Array<Entity>> Instance { get; } =
        new SqlQueryFactory();
}

}
