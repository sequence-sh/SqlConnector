using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Abstractions;
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Internal.Serialization;
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
    public async Task RunSCLSequence()
    {
        const string scl =
            @"SqlCreateSchemaFromTable 'Data Source=C:\Users\wainw\source\repos\MarkPersonal\ProgressiveAnagram\ProgressiveAnagram\Quotes.db; Version=3;' 'Quotes' 'SQLite'";

        var logger = TestOutputHelper.BuildLogger(LogLevel.Information);
        var sfs    = StepFactoryStore.CreateUsingReflection(typeof(CreateConnectionString));

        var context = new ExternalContext(
            ExternalContext.Default.FileSystemHelper,
            ExternalContext.Default.ExternalProcessRunner,
            ExternalContext.Default.Console,
            (DbConnectionFactory.DbConnectionName, DbConnectionFactory.Instance)
        );

        var runner = new SCLRunner(
            SCLSettings.EmptySettings,
            logger,
            sfs,
            context
        );

        var r = await runner.RunSequenceFromTextAsync(scl, CancellationToken.None);

        r.ShouldBeSuccessful(x => x.ToString()!);
    }

    //[Fact(Skip = "skip")]
    [Fact]
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
                        //ConnectionString = new CreateConnectionString
                        //{
                        //    Database = Constant("Introspect"),
                        //    Server   = Constant("DESKTOP-GPBS4SN"),
                        //    UserName = Constant("mark"),
                        //    Password = Constant("vafm4YgWyU5pWxJ")
                        //},

                        ConnectionString =
                            Constant(
                                @"Data Source=C:\Users\wainw\source\repos\MarkPersonal\ProgressiveAnagram\ProgressiveAnagram\Quotes.db; Version=3;"
                            ),
                        Query        = Constant(@"SELECT *  FROM Quotes limit 5"),
                        DatabaseType = Constant(DatabaseType.SQLite)
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
