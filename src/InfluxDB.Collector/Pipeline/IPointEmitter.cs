using System.Collections.Generic;

namespace InfluxDB.Collector.Pipeline
{
    interface IPointEmitter
    {
        void Emit(PointData point);

        void Emit(IEnumerable<PointData> points);
    }
}
