using System;
using System.Collections.Generic;
using System.Data;

namespace Reductech.EDR.Connectors.Sql
{

public static class Extensions
{
    public static IEnumerable<T> SelfAndDescendants<T>(
        this T entity,
        Func<T, IEnumerable<T>> getChildren)
    {
        yield return entity;

        var children = getChildren(entity);

        foreach (var child in children)
        foreach (var descendant in SelfAndDescendants(child, getChildren))
            yield return descendant;
    }

    public static void AddParameter(
        this IDbCommand command,
        string key,
        object? value,
        DbType dbType)
    {
        var parameter = command.CreateParameter();
        parameter.Value         = value;
        parameter.DbType        = dbType;
        parameter.ParameterName = "@" + key;

        command.Parameters.Add(parameter);
    }
}

}
