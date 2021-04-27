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

        var r = await DatabaseConnection.TrySetConnection(connection.Value, stateMonad, this);

        return r;
    }

    [StepProperty(1)][Required] public IStep<Entity> Connection { get; set; } = null!;

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<OpenConnection, Unit>();
}

}
