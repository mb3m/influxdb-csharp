using System;
using System.Collections.Generic;
using InfluxDB.Collector.Diagnostics;
using InfluxDB.Collector.Pipeline;

namespace InfluxDB.Collector
{
    public abstract class MetricsCollector : IPointEmitter, IDisposable
    {
        readonly Util.ITimestampSource _timestampSource = new Util.PseudoHighResTimestampSource();

        public Counter CreateCounter(string name, IReadOnlyDictionary<string, string> tags = null)
        {
            var counter = new Counter(name);
            RegisterMeasurement(counter);
            return counter;
        }

        public void Increment(string measurement, long count = 1, IReadOnlyDictionary<string, string> tags = null)
        {
            Write(measurement, new Dictionary<string, object> { { "count", count } }, tags);
        }

        public void Measure(string measurement, object value, IReadOnlyDictionary<string, string> tags = null)
        {
            Write(measurement, new Dictionary<string, object> { { "value", value } }, tags);
        }

        public void Increment(Counter counter, long count = 1)
        {
            counter.Increment(count);
            Emit(counter.Emit(_timestampSource.GetUtcNow()));
        }

        public void Increment(Faceted3Counter counter, string facet, long count = 1)
        {
            counter.Increment(facet, count);
            Emit(counter.Emit(_timestampSource.GetUtcNow()));
        }

        public IDisposable Time(string measurement, IReadOnlyDictionary<string, string> tags = null)
        {
            return new StopwatchTimer(this, measurement, tags);
        }

        public CollectorConfiguration Specialize()
        {
            return new CollectorConfiguration(this);
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing) { }

        public void Write(string measurement, IReadOnlyDictionary<string, object> fields, IReadOnlyDictionary<string, string> tags = null, DateTime? timestamp = null)
        {
            try
            {
                Emit(new PointData(measurement, fields, tags, timestamp ?? _timestampSource.GetUtcNow()));
            }
            catch (Exception ex)
            {
                CollectorLog.ReportError("Failed to write point", ex);
            }
        }

        void IPointEmitter.Emit(IPointData point)
        {
            Emit(point);
        }

        void IPointEmitter.Emit(IEnumerable<IPointData> points)
        {
            Emit(points);
        }

        protected virtual void Emit(IPointData point)
        {
            Emit(new[] { point });
        }

        protected abstract void RegisterMeasurement(IMeasurement measurement);

        protected abstract void Emit(IEnumerable<IPointData> points);
    }
}
