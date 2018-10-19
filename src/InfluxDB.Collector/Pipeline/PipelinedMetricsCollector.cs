
using System;
using System.Collections.Generic;

namespace InfluxDB.Collector.Pipeline
{
    class PipelinedMetricsCollector : MetricsCollector
    {
        readonly IPointEmitter _emitter;
        readonly IPointEnricher _enricher;
        readonly Action _dispose;

        public PipelinedMetricsCollector(IPointEmitter emitter, IPointEnricher enricher, Action dispose)
        {
            _emitter = emitter;
            _enricher = enricher;
            _dispose = dispose;
        }

        protected override void Emit(PointData point)
        {
            _enricher.Enrich(point);
            _emitter.Emit(point);
        }

        protected override void Emit(IEnumerable<PointData> points)
        {
            foreach (var point in points)
                _enricher.Enrich(point);

            _emitter.Emit(points);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _dispose();
        }
    }
}
