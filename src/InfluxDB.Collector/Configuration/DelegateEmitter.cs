using System;
using System.Collections.Generic;
using InfluxDB.Collector.Pipeline;

namespace InfluxDB.Collector.Configuration
{
    class DelegateEmitter : IPointEmitter
    {
        readonly Action<IEnumerable<IPointData>> _emitter;

        public DelegateEmitter(Action<IEnumerable<IPointData>> emitter)
        {
            if (emitter == null) throw new ArgumentNullException(nameof(emitter));
            _emitter = emitter;
        }

        public void Emit(IPointData point)
        {
            _emitter(new[] { point });
        }

        public void Emit(IEnumerable<IPointData> points)
        {
            _emitter(points);
        }
    }
}