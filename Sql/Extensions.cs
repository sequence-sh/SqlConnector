using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using CSharpFunctionalExtensions;
using Reductech.EDR.Core.Internal.Errors;

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

    public static Result<string, IErrorBuilder> CheckSqlObjectName(string tableName)
    {
        Result<string, IErrorBuilder> CreateError() =>
            ErrorCode_Sql.InvalidName.ToErrorBuilder(tableName);

        if (string.IsNullOrWhiteSpace(tableName))
            return CreateError();

        var first = tableName.First();

        if (!char.IsLetter(first))
            return CreateError();

        var isLegal = tableName.All(IsLegal);

        if (!isLegal)
            return CreateError();

        return tableName;

        static bool IsLegal(char c)
        {
            if (char.IsLetterOrDigit(c))
                return true;

            if (c == '@' || c == '_' || c == '$')
                return true;

            return false;
        }
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
