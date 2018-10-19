using System;
using System.Collections.Generic;
using InfluxDB.Collector.Pipeline;

namespace InfluxDB.Collector.Configuration
{
    class DelegateEmitter : IPointEmitter
    {
        readonly Action<IEnumerable<PointData>> _emitter;

        public DelegateEmitter(Action<IEnumerable<PointData>> emitter)
        {
            if (emitter == null) throw new ArgumentNullException(nameof(emitter));
            _emitter = emitter;
        }

        public void Emit(PointData point)
        {
            _emitter(new[] { point });
        }

        public void Emit(IEnumerable<PointData> points)
        {
            _emitter(points);
        }
    }
}