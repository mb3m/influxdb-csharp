using System.Collections.Generic;

namespace InfluxDB.Collector
{
    public interface IMeasurement
    {
        string Name { get; }
        IDictionary<string, string> Tags { get; set; }
    }
}
