using System.Collections.Generic;

namespace InfluxDB.Collector.Pipeline
{
    interface IPointEmitter
    {
        void Emit(IPointData point);

        void Emit(IEnumerable<IPointData> points);
    }
}
