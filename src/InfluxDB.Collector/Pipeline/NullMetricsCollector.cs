using System.Collections.Generic;

namespace InfluxDB.Collector.Pipeline
{
    class NullMetricsCollector : MetricsCollector
    {
        protected override void Emit(IEnumerable<PointData> points)
        {
        }
    }
}
