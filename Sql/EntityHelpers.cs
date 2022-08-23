using System.Diagnostics.Contracts;
using System.IO;
using System.Text.Json;
using Reductech.Sequence.Core.Entities;
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
}
