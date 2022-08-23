using System.Diagnostics.Contracts;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using Json.Schema;
using Reductech.Sequence.Core.Entities;
using Reductech.Sequence.Core.Internal.Errors;
using Entity = Reductech.Sequence.Core.Entity;

namespace Reductech.Sequence.Connectors.Sql;

public static class EntityHelpers
{
    /// <summary>
    /// Convert this Entity to a Json Element.
    /// This is a duplicate of a similar method in Core
    /// </summary>
    [Pure]
    public static JsonElement ConvertToJsonElement(this Entity entity)
    {
        //TODO remove this
        var stream = new MemoryStream();
        var writer = new Utf8JsonWriter(stream);

        EntityJsonConverter.Instance.Write(writer, entity, ISCLObject.DefaultJsonSerializerOptions);

        var reader = new Utf8JsonReader(stream.ToArray());

        using var document = JsonDocument.ParseValue(ref reader);
        return document.RootElement.Clone();
    }

    public static Result<JsonSchema, IErrorBuilder> CreateSchemaFromEntity(Entity entity)
    {
        try
        {
            var options = new JsonSerializerOptions()
            {
                Converters = { new JsonStringEnumConverter(), VersionJsonConverter.Instance },
                PropertyNameCaseInsensitive = true
            };

            var entityJson = JsonSerializer.Serialize(entity, options);
            var obj        = JsonSerializer.Deserialize<JsonSchema>(entityJson, options);

            if (obj is null)
                return ErrorCode.CouldNotParse.ToErrorBuilder(entityJson, typeof(JsonSchema).Name);

            return obj;
        }
        catch (Exception e)
        {
            return ErrorCode.CouldNotParse.ToErrorBuilder(e);
        }
    }
}
