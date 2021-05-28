using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Threading;
using System.Threading.Tasks;
using CSharpFunctionalExtensions;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Attributes;
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Internal.Errors;
using Reductech.EDR.Core.Util;
using Entity = Reductech.EDR.Core.Entity;

namespace Reductech.EDR.Connectors.Sql.Steps
{

/// <summary>
/// Open a new connection to a database.
/// </summary>
public class OpenConnection : CompoundStep<Unit>
{
    /// <inheritdoc />
    protected override async Task<Result<Unit, IError>> Run(
        IStateMonad stateMonad,
        CancellationToken cancellationToken)
    {
        var connection = await Connection.Run(stateMonad, cancellationToken);

        if (connection.IsFailure)
            return connection.ConvertFailure<Unit>();

        var r = await DatabaseConnectionMetadata.TrySetConnection(
                connection.Value,
                stateMonad,
                this
            )
            .Map(x => Unit.Default);

        return r;
    }

    /// <summary>
    /// Database-specific connection properties.
    /// </summary>
    [StepProperty(1)]
    [Required]
    public IStep<Entity> Connection { get; set; } = null!;

    /// <inheritdoc />
    public override IStepFactory StepFactory => OpenConnectionStepFactory.Instance;

    private sealed class OpenConnectionStepFactory : SimpleStepFactory<OpenConnection, Unit>
    {
        private OpenConnectionStepFactory() { }

        public static SimpleStepFactory<OpenConnection, Unit> Instance { get; } =
            new OpenConnectionStepFactory();

        /// <inheritdoc />
        public override IEnumerable<Type> ExtraEnumTypes
        {
            get
            {
                yield return typeof(DatabaseType);
            }
        }
    }
}

}
