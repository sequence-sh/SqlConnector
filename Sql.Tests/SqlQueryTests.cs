using Reductech.EDR.Core;
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Steps;
using Reductech.EDR.Core.TestHarness;
using static Reductech.EDR.Core.TestHarness.StaticHelpers;
using Reductech.EDR.Core.Util;
using System.Collections.Generic;

namespace Reductech.EDR.Connectors.Sql.Tests
{

public partial class SqlQueryTests : StepTestBase<SqlQuery, Array<Entity>>
{
    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            yield return new StepCase(
                "Run a SQL query and print each row",
                new ForEach<Entity>()
                {
                    Array = new SqlQuery
                    {
                        Server   = Constant("server"),
                        Database = Constant("database"),
                        Query    = Constant("SELECT * FROM TABLE1")
                    },
                    Action = new Print<Entity>
                    {
                        Value = new GetVariable<Entity> { Variable = VariableName.Entity }
                    }
                },
                Unit.Default,
                "(Col1: \"Row1\" Col2: 1)",
                "(Col1: \"Row2\" Col2: 2)"
            );
        }
    }

    /// <inheritdoc />
    protected override IEnumerable<DeserializeCase> DeserializeCases
    {
        get
        {
            yield return new DeserializeCase(
                "Run script that returns two PSObjects and print results",
                @"
- ForEach
    Array: (SqlQuery Server: ""server"" Database: ""database"" Query: ""SELECT * FROM TABLE1"")
    Action: (Print <entity>)",
                Unit.Default,
                "(Col1: \"Row1\" Col2: 1)",
                "(Col1: \"Row2\" Col2: 2)"
            );
        }
    }
}

}
