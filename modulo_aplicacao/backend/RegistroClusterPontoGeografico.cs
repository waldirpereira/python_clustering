using System.Collections.Generic;
using MongoDB.Bson.Serialization.Attributes;

namespace CronogramaDeSondas.Infra.Python
{
    [BsonIgnoreExtraElements]
    public class RegistroClusterPontoGeografico
    {
        public int Cluster { get; set; }
        public IEnumerable<string> NomesPontosGeograficos { get; set; }
    }
}