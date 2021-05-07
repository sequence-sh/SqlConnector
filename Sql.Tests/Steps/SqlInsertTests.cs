using System;
using System.Collections.Generic;
using Reductech.EDR.Connectors.Sql.Steps;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Entities;
using Reductech.EDR.Core.Enums;
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

            var schema = new Schema()
            {
                Name            = tableName,
                ExtraProperties = ExtraPropertyBehavior.Fail,
                Properties = new Dictionary<string, SchemaProperty>()
                {
                    {
                        "Id",
                        new SchemaProperty()
                        {
                            Type = SCLType.Integer, Multiplicity = Multiplicity.ExactlyOne
                        }
                    },
                    {
                        "Name",
                        new SchemaProperty()
                        {
                            Type = SCLType.String, Multiplicity = Multiplicity.UpToOne
                        }
                    },
                    {
                        "TestDouble",
                        new SchemaProperty()
                        {
                            Type = SCLType.Double, Multiplicity = Multiplicity.UpToOne
                        }
                    },
                    {
                        "TestDate",
                        new SchemaProperty()
                        {
                            Type = SCLType.Date, Multiplicity = Multiplicity.UpToOne
                        }
                    },
                    {
                        "TestBool",
                        new SchemaProperty()
                        {
                            Type = SCLType.Bool, Multiplicity = Multiplicity.UpToOne
                        }
                    },
                    {
                        "TestEnum",
                        new SchemaProperty()
                        {
                            Type         = SCLType.Enum,
                            Multiplicity = Multiplicity.UpToOne,
                            Values       = new List<string> { "EnumValue", "EnumValue2" },
                            EnumType     = "MyEnum"
                        }
                    },
                }
            };

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
                                Command = Constant($"DROP TABLE IF EXISTS {schema.Name}")
                            },
                            new SqlCreateTable()
                            {
                                //no need to set connection here, the previous connection is remembered
                                Schema = Constant(schema.ConvertToEntity())
                            },
                            new SqlInsert
                            {
                                //no need to set connection here, the previous connection is remembered
                                Schema   = Constant(schema.ConvertToEntity()),
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
                                Command = Constant($"DROP TABLE IF EXISTS {schema.Name}")
                            },
                            new SqlCreateTable() { Schema = Constant(schema.ConvertToEntity()) },
                            new SqlInsert
                            {
                                Schema   = Constant(schema.ConvertToEntity()),
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
