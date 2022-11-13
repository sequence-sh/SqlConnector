using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Runtime.CompilerServices;
using Sequence.Core.Entities;
using Sequence.Core.Internal.Errors;

namespace Sequence.Connectors.Sql;

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

            if (c is '@' or '_' or '$')
                return true;

            return false;
        }
    }

    public static async IAsyncEnumerable<Core.Entity> GetEntityEnumerable(
        IDataReader reader,
        IDbCommand command,
        IDbConnection connection,
        [EnumeratorCancellation] CancellationToken cancellation)
    {
        try
        {
            var row = new object[reader.FieldCount];

            //TODO async properly

            var headers = Enumerable.Range(0, reader.FieldCount)
                .Select(reader.GetName)
                .Select(x => new EntityKey(x))
                .ToImmutableArray();

            while (!reader.IsClosed && reader.Read() && !cancellation.IsCancellationRequested)
            {
                reader.GetValues(row);
                var values = row.Select(ISCLObject.CreateFromCSharpObject).ToImmutableArray();

                yield return new Core.Entity(headers, values);
            }
        }
        finally
        {
            reader.Close();
            reader.Dispose();
            command.Dispose();
            connection.Dispose();
        }

        await ValueTask.CompletedTask;
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

    public static string MaybeQuote(string name, bool quote)
    {
        return quote ? $"\"{name}\"" : name;
    }
}
