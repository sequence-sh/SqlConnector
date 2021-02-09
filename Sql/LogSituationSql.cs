﻿using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Reductech.EDR.Core.Internal.Logging;

namespace Reductech.EDR.Connectors.Sql
{

public sealed record LogSituationSql : LogSituationBase
{
    private LogSituationSql(string code, LogLevel logLevel) : base(code, logLevel) { }

    /// <inheritdoc />
    public override string GetLocalizedString()
    {
        var localizedMessage = LogMessages_EN
            .ResourceManager.GetString(Code); //TODO static method to get this

        Debug.Assert(localizedMessage != null, nameof(localizedMessage) + " != null");
        return localizedMessage;
    }

    /// <summary>
    /// Command executed with {0} rows affected.
    /// </summary>
    public static readonly LogSituationSql CommandExecuted = new(
        nameof(CommandExecuted),
        LogLevel.Information
    );
}

}
