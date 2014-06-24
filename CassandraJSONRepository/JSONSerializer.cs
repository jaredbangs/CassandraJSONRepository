using System;
using Newtonsoft.Json;

namespace CassandraJSONRepository
{
    public interface IJSONSerializer
    {
        T DeserializeObject<T>(string json);
        string SerializeObject(object item);
    }

    public class JSONSerializer : IJSONSerializer
    {
        public T DeserializeObject<T>(string json)
        {
            return JsonConvert.DeserializeObject<T>(json);
        }

        public string SerializeObject(object item)
        {
            return JsonConvert.SerializeObject(item);
        }
    }
}