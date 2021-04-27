﻿using System.Collections.Generic;
using Reductech.EDR.Connectors.Sql.Steps;
using Reductech.EDR.Core;
using Reductech.EDR.Core.TestHarness;
using static Reductech.EDR.Core.TestHarness.StaticHelpers;

namespace Reductech.EDR.Connectors.Sql.Tests
{

public partial class
    CreatePostgresConnectionStringTests : StepTestBase<CreatePostgresConnectionString, Entity>
{
    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            yield return new StepCase(
                "Create Postgres connection string",
                new CreatePostgresConnectionString
                {
                    Database = Constant("Database"),
                    Password = Constant("Password"),
                    Host     = Constant("Host"),
                    Port     = Constant(123),
                    UserId   = Constant("User")
                },
                new DatabaseConnectionMetadata()
                {
                    ConnectionString =
                        "User ID=User;Password=Password;Host=Host;Port=123;Database=Database;",
                    DatabaseType = DatabaseType.Postgres
                }.ConvertToEntity()
            );
        }
    }
}

}
