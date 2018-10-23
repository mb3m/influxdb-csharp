using System;
using System.Collections.Generic;
using InfluxDB.Collector.Pipeline;

namespace InfluxDB.Collector.Configuration
{
    class AggregateEmitter : IPointEmitter
    {
        readonly IEnumerable<IPointEmitter> _emitters;

        public AggregateEmitter(IEnumerable<IPointEmitter> emitters)
        {
            if (emitters == null) throw new ArgumentNullException(nameof(emitters));
            _emitters = emitters;
        }

        public void Emit(IPointData point)
        {
            foreach (var emitter in _emitters)
                emitter.Emit(point);
        }

        public void Emit(IEnumerable<IPointData> points)
        {
            foreach (var emitter in _emitters)
                emitter.Emit(points);
        }
    }
}