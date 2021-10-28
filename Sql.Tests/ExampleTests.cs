using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Json.More;
using Json.Schema;
using Microsoft.Extensions.Logging;
using Reductech.EDR.Connectors.Sql.Steps;
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
        const string connectionString =
            "User ID=postgres;Password=postgres;Host=localhost;Port=5432;Database=postgres;";

        string scl =
            // @"SqlQuery (CreateConnectionString 'DESKTOP-GPBS4SN' 'Introspect') 'SELECT *  FROM [AUN_CUSTODIAN]'";
            $@"SqlCreateSchemaFromTable '{connectionString}' 'MyTable' 'postgres'";

        var logger = TestOutputHelper.BuildLogger(LogLevel.Information);

        var assembly = typeof(CreateMsSQLConnectionString).Assembly;

        var sfs = StepFactoryStore.CreateFromAssemblies(assembly);

        var context = new ExternalContext(
            ExternalContext.Default.ExternalProcessRunner,
            ExternalContext.Default.Console,
            (DbConnectionFactory.DbConnectionName, DbConnectionFactory.Instance)
        );

        var runner = new SCLRunner(
            logger,
            sfs,
            context,
            DefaultRestClientFactory.Instance
        );

        var r = await runner.RunSequenceFromTextAsync(
            scl,
            new Dictionary<string, object>(),
            CancellationToken.None
        );

        r.ShouldBeSuccessful();
    }

    [Fact(Skip = "skip")]
    //[Fact]
    [Trait("Category", "Integration")]
    public async Task RunObjectSequence()
    {
        //const string connectionString =
        //    @"Data Source=C:\Users\wainw\source\repos\MarkPersonal\ProgressiveAnagram\ProgressiveAnagram\Quotes.db; Version=3;";

        const string tableName = "MyTable4";

        var schemaBuilder = new JsonSchemaBuilder()
            .Title(tableName)
            .Properties(("Id", SchemaHelpers.AnyInt))
            .Properties(("Name", SchemaHelpers.AnyString))
            .Properties(("TestDouble", SchemaHelpers.AnyNumber))
            .Properties(("TestDate", SchemaHelpers.AnyDateTime))
            .Properties(("TestBool", SchemaHelpers.AnyBool))
            .Properties(("TestEnum", SchemaHelpers.EnumProperty("EnumValue", "EnumValue2")))
            .AdditionalProperties(JsonSchema.False);

        var schema = schemaBuilder.Build();

        static Entity[] CreateEntityArray(int number)
        {
            var entities = new List<Entity>();

            for (var i = 0; i < number; i++)
            {
                entities.Add(CreateEntity(i));
            }

            static Entity CreateEntity(int i)
            {
                return Entity.Create(
                    ("Id", i),
                    ("Name", $"Mark{i}"),
                    ("TestDouble", 3.142 + i),
                    ("TestDate", new DateTime(1970 + (i / 12), (i % 12) + 1, 6)),
                    ("TestBool", i % 2 == 0),
                    ("TestEnum", i % 2 == 0 ? "EnumValue" : "EnumValue2")
                );
            }

            return entities.ToArray();
        }

        const int numberOfEntities = 3;

        var step = new Sequence<Unit>()
        {
            InitialSteps = new List<IStep<Unit>>
            {
                new SetVariable<Entity>()
                {
                    Variable = new VariableName("ConnectionString"),
                    //Value = new CreatePostgresConnectionString()
                    //{
                    //    Database = Constant("postgres"),
                    //    Host     = Constant("localhost"),
                    //    Password = Constant("postgres"),
                    //    Port     = Constant(5432),
                    //    UserId   = Constant("postgres")
                    //}
                    Value = new CreateMySQLConnectionString()
                    {
                        UId      = Constant("root"),
                        Database = Constant("Test"),
                        Server   = Constant("localhost"),
                        Pwd      = Constant("Steeltoe456"),
                    }
                },
                new SqlCommand()
                {
                    Connection = GetVariable<Entity>("ConnectionString"),
                    Command = Constant(
                        $"Drop table if exists {schema.Keywords!.OfType<TitleKeyword>().Select(x => x.Value).Single()};"
                    )
                },
                new SqlCreateTable()
                {
                    Schema = Constant(Entity.Create(schema.ToJsonDocument().RootElement))
                },
                new SqlInsert()
                {
                    Schema   = Constant(Entity.Create(schema.ToJsonDocument().RootElement)),
                    Entities = Array(CreateEntityArray(numberOfEntities)),
                },
                new SetVariable<int>()
                {
                    Value = new EntityGetValue<int>()
                    {
                        Entity = new ElementAtIndex<Entity>()
                        {
                            Array = new SqlQuery()
                            {
                                Query = Constant("SELECT COUNT(*) FROM MyTable4")
                            },
                            Index = Constant(0)
                        },
                        Property = Constant("Count(*)")
                    },
                    Variable = new VariableName("ActualCount")
                },
                new AssertEqual<int>()
                {
                    Left = GetVariable<int>("ActualCount"), Right = Constant(3)
                }
            },
            FinalStep = new DoNothing()
        };

        var scl = step.Serialize();

        TestOutputHelper.WriteLine(scl);

        var context = new ExternalContext(
            ExternalContext.Default.ExternalProcessRunner,
            ExternalContext.Default.Console,
            (DbConnectionFactory.DbConnectionName, DbConnectionFactory.Instance)
        );

        var monad = new StateMonad(
            TestOutputHelper.BuildLogger(),
            StepFactoryStore.Create(),
            context,
            DefaultRestClientFactory.Instance,
            new Dictionary<string, object>()
        );

        var r = await (step as IStep<Unit>).Run(monad, CancellationToken.None);

        r.ShouldBeSuccessful();
    }
}

}
