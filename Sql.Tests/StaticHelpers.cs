﻿namespace Sequence.Connectors.Sql.Tests;

public static class StaticHelpers
{
    public static IStep<Entity> GetConnectionMetadata(
        DatabaseType databaseType,
        string connectionString = TestConnectionString) => Core.TestHarness.StaticHelpers.Constant(
        new DatabaseConnectionMetadata()
        {
            ConnectionString = connectionString, DatabaseType = databaseType
        }.ConvertToEntity()
    );

    public const string TestConnectionString = @"Test Connection String";

    public static T WithDbConnectionInState<T>(
        this T @case,
        DatabaseType databaseType,
        string connectionString = TestConnectionString)
        where T : ICaseThatExecutes
    {
        @case.WithExpectedFinalState(
            DatabaseConnectionMetadata.DatabaseConnectionVariableName.Name,
            new DatabaseConnectionMetadata()
            {
                ConnectionString = connectionString, DatabaseType = databaseType
            }.ConvertToEntity()
        );

        return @case;
    }
}
