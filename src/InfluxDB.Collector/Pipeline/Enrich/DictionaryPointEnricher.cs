using System.Collections.Generic;

namespace InfluxDB.Collector.Pipeline.Enrich
{
    class DictionaryPointEnricher : IPointEnricher
    {
        readonly IReadOnlyDictionary<string, string> _tags;

        public DictionaryPointEnricher(IReadOnlyDictionary<string, string> tags)
        {
            _tags = tags;
        }

        public void Enrich(IMeasurement measure)
        {
            measure.Tags = measure.Tags ?? new Dictionary<string, string>();
            foreach (var tag in _tags)
            {
                if (!measure.Tags.ContainsKey(tag.Key))
                    measure.Tags.Add(tag.Key, tag.Value);
            }
        }
    }
}
