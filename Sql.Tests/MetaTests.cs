using System.Reflection;
using Reductech.EDR.Connectors.Sql.Steps;

namespace Reductech.EDR.Connectors.Sql.Tests;

/// <summary>
/// Makes sure all steps have a test class
/// </summary>
public class MetaTests : MetaTestsBase
{
    /// <inheritdoc />
    public override Assembly StepAssembly => typeof(SqlInsert).Assembly;

    /// <inheritdoc />
    public override Assembly TestAssembly => typeof(MetaTests).Assembly;
}
