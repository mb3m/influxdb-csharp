using System;
using System.Collections.Generic;
using System.Linq;
using InfluxDB.LineProtocol.Payload;

namespace InfluxDB.Collector.Pipeline
{
    public class PointData : IPointData, IMeasurement
    {
        public string Name { get; }
        public Dictionary<string, object> Fields { get; }
        public IDictionary<string, string> Tags { get; set; }
        public DateTime? UtcTimestamp { get; }

        public PointData(
            string measurementName,
            IReadOnlyDictionary<string, object> fields,
            IReadOnlyDictionary<string, string> tags = null,
            DateTime? utcTimestamp = null)
        {
            if (measurementName == null) throw new ArgumentNullException(nameof(measurementName));
            if (fields == null) throw new ArgumentNullException(nameof(fields));
            Name = measurementName;
            Fields = fields.ToDictionary(kv => kv.Key, kv => kv.Value);
            if (tags != null)
                Tags = tags.ToDictionary(kv => kv.Key, kv => kv.Value);
            UtcTimestamp = utcTimestamp;
        }

        public ILineProtocolPoint AsLineProtocolPoint()
        {
            return new LineProtocolPoint(Name, Fields, Tags, UtcTimestamp);
        }
    }
}
