using System;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using CSharpFunctionalExtensions;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Attributes;
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Internal.Errors;
using Reductech.EDR.Core.Util;
using Entity = Reductech.EDR.Core.Entity;

namespace Reductech.EDR.Connectors.Sql
{

public sealed class SqlInsert : CompoundStep<Unit>
{
    /// <inheritdoc />
    protected override async Task<Result<Unit, IError>> Run(
        IStateMonad stateMonad,
        CancellationToken cancellationToken)
    {
        var connectionString =
            await ConnectionString.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (connectionString.IsFailure)
            return connectionString.ConvertFailure<Unit>();

        var entities = await
            Entities.Run(stateMonad, cancellationToken);

        if (entities.IsFailure)
            return entities.ConvertFailure<Unit>();

        var table = await Table.Run(stateMonad, cancellationToken);

        if (table.IsFailure)
            return table.ConvertFailure<Unit>();

        var databaseType = await DatabaseType.Run(stateMonad, cancellationToken);

        if (databaseType.IsFailure)
            return databaseType.ConvertFailure<Unit>();

        var factory = stateMonad.ExternalContext
            .TryGetContext<IDbConnectionFactory>(DbConnectionFactory.DbConnectionName);

        if (factory.IsFailure)
            return factory.MapError(x => x.WithLocation(this)).ConvertFailure<Unit>();

        IDbConnection conn = factory.Value.GetDatabaseConnection(
            databaseType.Value,
            connectionString.Value
        );

        throw new NotImplementedException();

        //conn.Open();

        //var command = conn.CreateCommand();
        //command.CommandText = entities.Value;

        //var dbReader = command.ExecuteReader();

        //var array = GetEntityEnumerable(dbReader, command, conn, cancellationToken).ToSequence();

        //return array;
    }

    /// <summary>
    /// The Connection String
    /// </summary>
    [StepProperty(order: 1)]
    [Required]
    public IStep<StringStream> ConnectionString { get; set; } = null!;

    /// <summary>
    /// The entities to insert
    /// </summary>
    [StepProperty(order: 2)]
    [Required]
    [Alias("Sql")]
    public IStep<Array<Entity>> Entities { get; set; } = null!;

    /// <summary>
    /// The table to insert into
    /// </summary>
    [StepProperty(order: 3)]
    [Required]
    [Alias("Sql")]
    public IStep<StringStream> Table { get; set; } = null!;

    [StepProperty(4)]
    [DefaultValueExplanation("Sql")]
    [Alias("DB")]
    public IStep<DatabaseType> DatabaseType { get; set; } =
        new EnumConstant<DatabaseType>(Sql.DatabaseType.MsSql);

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<SqlInsert, Unit>();
}

}
