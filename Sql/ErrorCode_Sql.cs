using System.Diagnostics;
using Reductech.EDR.Core.Internal.Errors;

namespace Reductech.EDR.Connectors.Sql
{

/// <summary>
/// Identifying code for an error message in Sql connector
/// </summary>
// ReSharper disable once InconsistentNaming
public sealed record ErrorCode_Sql : ErrorCodeBase
{
    private ErrorCode_Sql(string code) : base(code) { }

    /// <inheritdoc />
    public override string GetFormatString()
    {
        var localizedMessage =
            ErrorMessages_EN.ResourceManager.GetString(Code); //TODO static method to get this

        Debug.Assert(localizedMessage != null, nameof(localizedMessage) + " != null");
        return localizedMessage;
    }

    /// <summary>
    /// Could not get create table: '{0}'
    /// </summary>
    public static readonly ErrorCode_Sql CouldNotGetCreateTable =
        new(nameof(CouldNotGetCreateTable));

    /// <summary>
    /// Could not handle data type '{0}' in column '{1}'
    /// </summary>
    public static readonly ErrorCode_Sql CouldNotHandleDataType =
        new(nameof(CouldNotHandleDataType));
}

}
