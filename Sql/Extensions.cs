using System;
using System.Collections.Generic;

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
        foreach (var descendant in SelfAndDescendants<T>(child, getChildren))
            yield return descendant;
    }
}

}
