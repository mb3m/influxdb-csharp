using System.Collections.Generic;

namespace InfluxDB.Collector.Pipeline
{
    class NullMetricsCollector : MetricsCollector
    {
        protected override void Emit(IEnumerable<IPointData> points)
        {
        }

        protected override void RegisterMeasurement(IMeasurement measurement)
        {
        }
    }
}
