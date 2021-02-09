using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Abstractions;
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Steps;
using Reductech.EDR.Core.TestHarness;
using Reductech.EDR.Core.Util;
using Xunit;
using Xunit.Abstractions;
using static Reductech.EDR.Core.TestHarness.StaticHelpers;

namespace Reductech.EDR.Connectors.Sql.Tests
{

/// <summary>
/// These are not really tests but ways to quickly and easily run steps
/// </summary>
[AutoTheory.UseTestOutputHelper]
public partial class ExampleTests
{
    [Fact(Skip = "skip")]
    //[Fact]
    [Trait("Category", "Integration")]
    public async Task RunObjectSequence()
    {
        var step = new Sequence<Unit>()
        {
            InitialSteps = new List<IStep<Unit>>
            {
                new ForEach<Entity>()
                {
                    Array = new SqlQuery()
                    {
                        ConnectionString =
                            Constant(
                                @"Data Source=C:\Users\wainw\source\repos\MarkPersonal\ProgressiveAnagram\ProgressiveAnagram\Quotes.db; Version=3;"
                            ),
                        Query        = Constant(@"SELECT *  FROM Quotes limit 5"),
                        DatabaseType = Constant(DatabaseType.SqlLite)
                    },
                    Action = new Log<Entity>
                    {
                        Value = new GetVariable<Entity> { Variable = VariableName.Entity }
                    },
                },
            },
            FinalStep = new DoNothing()
        };

        var scl = step.Serialize();

        TestOutputHelper.WriteLine(scl);

        var context = new ExternalContext(
            ExternalContext.Default.FileSystemHelper,
            ExternalContext.Default.ExternalProcessRunner,
            ExternalContext.Default.Console,
            (DbConnectionFactory.DbConnectionName, DbConnectionFactory.Instance)
        );

        var monad = new StateMonad(
            TestOutputHelper.BuildLogger(),
            SCLSettings.EmptySettings,
            StepFactoryStore.CreateUsingReflection(),
            context
        );

        var r = await (step as IStep<Unit>).Run(monad, CancellationToken.None);

        r.ShouldBeSuccessful(x => x.ToString()!);
    }
}

}
