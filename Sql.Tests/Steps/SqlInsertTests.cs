using System;
using System.Collections.Generic;
using System.Linq;
using Json.More;
using Json.Schema;
using Reductech.EDR.Connectors.Sql.Steps;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Entities;
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Steps;
using Reductech.EDR.Core.TestHarness;
using Reductech.EDR.Core.Util;
using static Reductech.EDR.Core.TestHarness.StaticHelpers;
using static Reductech.EDR.Connectors.Sql.Tests.StaticHelpers;

namespace Reductech.EDR.Connectors.Sql.Tests.Steps
{

public partial class SqlInsertTests : StepTestBase<SqlInsert, Unit>
{
    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            var    inMemoryConnectionString = "Data Source=InMemorySample;Mode=Memory;Cache=Shared";
            string tableName                = "MyTable";

            var schemaBuilder = new JsonSchemaBuilder()
                .Title(tableName)
                .Properties(
                    ("Id", SchemaHelpers.AnyInt),
                    ("Name", SchemaHelpers.AnyString),
                    ("TestDouble", SchemaHelpers.AnyNumber),
                    ("TestDate", SchemaHelpers.AnyDateTime),
                    ("TestBool", SchemaHelpers.AnyBool),
                    ("TestEnum", SchemaHelpers.EnumProperty("EnumValue", "EnumValue2"))
                )
                .AdditionalProperties(JsonSchema.False);

            var schema         = schemaBuilder.Build();
            var schemaAsEntity = schema.ConvertToEntity();

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

            yield return new StepCase(
                    "Insert One Entity",
                    new Sequence<Unit>()
                    {
                        InitialSteps = new List<IStep<Unit>>()
                        {
                            new SqlCommand()
                            {
                                Connection =
                                    GetConnectionMetadata(
                                        DatabaseType.SQLite,
                                        inMemoryConnectionString
                                    ),
                                Command = Constant(
                                    $"DROP TABLE IF EXISTS {schema.Keywords!.OfType<TitleKeyword>().Select(x => x.Value).FirstOrDefault()}"
                                )
                            },
                            new SqlCreateTable()
                            {
                                //no need to set connection here, the previous connection is remembered
                                Schema = Constant(schemaAsEntity)
                            },
                            new SqlInsert
                            {
                                //no need to set connection here, the previous connection is remembered
                                Schema   = Constant(schemaAsEntity),
                                Entities = Array(CreateEntityArray(1))
                            }
                        },
                        FinalStep = new DoNothing()
                    },
                    Unit.Default
                ) { IgnoreLoggedValues = true }
                .WithDbConnectionInState(DatabaseType.SQLite, inMemoryConnectionString)
                .WithContext(
                    DbConnectionFactory.DbConnectionName,
                    DbConnectionFactory.Instance
                );

            yield return new StepCase(
                    "Insert 3000 Entities",
                    new Sequence<Unit>()
                    {
                        InitialSteps = new List<IStep<Unit>>()
                        {
                            new SqlCommand()
                            {
                                Connection =
                                    GetConnectionMetadata(
                                        DatabaseType.SQLite,
                                        inMemoryConnectionString
                                    ),
                                Command = Constant(
                                    $"DROP TABLE IF EXISTS {schema.Keywords!.OfType<TitleKeyword>().Select(x => x.Value).FirstOrDefault()}"
                                )
                            },
                            new SqlCreateTable()
                            {
                                Schema = Constant(
                                    Entity.Create(schema.ToJsonDocument().RootElement)
                                )
                            },
                            new SqlInsert
                            {
                                Schema = Constant(
                                    Entity.Create(schema.ToJsonDocument().RootElement)
                                ),
                                Entities = Array(CreateEntityArray(3000))
                            }
                        },
                        FinalStep = new DoNothing()
                    },
                    Unit.Default
                ) { IgnoreLoggedValues = true }
                .WithDbConnectionInState(DatabaseType.SQLite, inMemoryConnectionString)
                .WithContext(
                    DbConnectionFactory.DbConnectionName,
                    DbConnectionFactory.Instance
                );
        }
    }
}

}
