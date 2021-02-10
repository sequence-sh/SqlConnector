using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Attributes;
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Internal.Errors;
using CSharpFunctionalExtensions;
using Entity = Reductech.EDR.Core.Entity;

namespace Reductech.EDR.Connectors.Sql
{

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
        var query = await
            Query.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (query.IsFailure)
            return query.ConvertFailure<Array<Entity>>();

        var connectionString =
            await ConnectionString.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (connectionString.IsFailure)
            return connectionString.ConvertFailure<Array<Entity>>();

        var databaseType = await DatabaseType.Run(stateMonad, cancellationToken);

        if (databaseType.IsFailure)
            return databaseType.ConvertFailure<Array<Entity>>();

        var factory = stateMonad.ExternalContext
            .TryGetContext<IDbConnectionFactory>(DbConnectionFactory.DbConnectionName);

        if (factory.IsFailure)
            return factory.MapError(x => x.WithLocation(this)).ConvertFailure<Array<Entity>>();

        IDbConnection conn = factory.Value.GetDatabaseConnection(
            databaseType.Value,
            connectionString.Value
        );

        conn.Open();

        var command = conn.CreateCommand();
        command.CommandText = query.Value;

        var dbReader = command.ExecuteReader();

        var array = GetEntityEnumerable(dbReader, command, conn, cancellationToken).ToSequence();

        return array;
    }

    static async IAsyncEnumerable<Entity> GetEntityEnumerable(
        IDataReader reader,
        IDbCommand command,
        IDbConnection connection,
        [EnumeratorCancellation] CancellationToken cancellation)
    {
        try
        {
            var row = new object[reader.FieldCount];

            //TODO async properly

            while (!reader.IsClosed && reader.Read() && !cancellation.IsCancellationRequested)
            {
                reader.GetValues(row);
                var props = new List<(string, object?)>(row.Length);

                for (var col = 0; col < row.Length; col++)
                    props.Add((reader.GetName(col), row[col]));

                yield return Entity.Create(props.ToArray());
            }
        }
        finally
        {
            reader.Close();
            reader.Dispose();
            command.Dispose();
            connection.Dispose();
        }

        await ValueTask.CompletedTask;
    }

    /// <summary>
    /// The Connection String
    /// </summary>
    [StepProperty(order: 1)]
    [Required]
    public IStep<StringStream> ConnectionString { get; set; } = null!;

    /// <summary>
    /// The SQL query to run
    /// </summary>
    [StepProperty(order: 2)]
    [Required]
    [Alias("SQL")]
    public IStep<StringStream> Query { get; set; } = null!;

    [StepProperty]
    [DefaultValueExplanation("SQL")]
    [Alias("DB")]
    public IStep<DatabaseType> DatabaseType { get; set; } =
        new EnumConstant<DatabaseType>(Sql.DatabaseType.Sql);

    /// <inheritdoc />
    public override IStepFactory StepFactory => new SimpleStepFactory<SqlQuery, Array<Entity>>();
}

}
