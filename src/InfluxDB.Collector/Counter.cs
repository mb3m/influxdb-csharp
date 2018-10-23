using InfluxDB.Collector.Pipeline;
using InfluxDB.LineProtocol.Payload;
using System;
using System.Collections.Generic;
using System.Linq;

namespace InfluxDB.Collector
{
    public class Counter : Measurement
    {
        readonly LineProtocolMeasure<long> _lpMeasure;

        long _count;

        public Counter(string name, IReadOnlyDictionary<string, string> tags = null)
            : base(name, tags)
        {
            _lpMeasure = new LineProtocolMeasure<long>(name, "value", tags.Keys);
        }

        public override IPointData Emit(DateTime timestamp) => new CounterPointData(this, _count, timestamp);

        public void Increment(long count)
        {
            _count += count;
        }

        public class CounterPointData : IPointData
        {
            private readonly Counter _counter;
            private readonly long _value;
            private readonly DateTime _timestamp;

            public CounterPointData(Counter counter, long value, DateTime timestamp)
            {
                _counter = counter;
                _value = value;
                this._timestamp = timestamp;
            }

            public ILineProtocolPoint AsLineProtocolPoint() => _counter._lpMeasure.AddPoint(_value, _counter.Tags.Values, _timestamp);
        }
    }

    /// <summary>
    /// Counter where the value is splitted between two states (ex: success / failure).
    /// This counter is represented as a single measure, with 3 fields : value, with the total value, and one field per state.
    /// </summary>
    public class Faceted2Counter : Measurement
    {
        readonly LineProtocolMeasure<long, long, long> _lpMeasure;

        long _countTotal;

        long _count1;

        long _count2;

        public Faceted2Counter(string name, string facet1Name, string facet2Name, IReadOnlyDictionary<string, string> tags = null)
            : base(name, tags)
        {
            _lpMeasure = new LineProtocolMeasure<long, long, long>(name, "value", facet1Name, facet2Name, tags.Keys);
        }

        public void Increment(string facet, long count)
        {
            if (string.Equals(facet, _lpMeasure.FieldNames[1], StringComparison.Ordinal))
                _count1 += count;
            else if (string.Equals(facet, _lpMeasure.FieldNames[2], StringComparison.Ordinal))
                _count2 += count;
            else
                throw new ArgumentException("Unknown facet");


            _countTotal++;
        }

        public override IPointData Emit(DateTime timestamp) => new PointData(this, _countTotal, _count1, _count2, timestamp);

        public class PointData : IPointData
        {
            private readonly Faceted2Counter _counter;
            private readonly long _value;
            private readonly long _facet1;
            private readonly long _facet2;
            private readonly DateTime _timestamp;

            public PointData(Faceted2Counter counter, long value, long facet1, long facet2, DateTime timestamp)
            {
                _counter = counter;
                _value = value;
                _facet1 = facet1;
                _facet2 = facet2;
                _timestamp = timestamp;
            }

            public ILineProtocolPoint AsLineProtocolPoint() => _counter._lpMeasure.AddPoint(_value, _facet1, _facet2, _counter.Tags.Values, _timestamp);
        }

    }

    /// <summary>
    /// Counter where the value is splitted between three states (ex: success / error / pending).
    /// This counter is represented as a single measure, with 4 fields : value, with the total value, and one field per state.
    /// </summary>
    public class Faceted3Counter : Measurement
    {
        readonly LineProtocolMeasure<long, long, long, long> _lpMeasure;

        long _countTotal;

        long _count1;

        long _count2;

        long _count3;

        public Faceted3Counter(string name, string facet1Name, string facet2Name, string facet3Name, IReadOnlyDictionary<string, string> tags = null)
            : base(name, tags)
        {
            _lpMeasure = new LineProtocolMeasure<long, long, long, long>(name, "value", facet1Name, facet2Name, facet3Name, tags.Keys);
        }

        public void Increment(string facet, long count)
        {
            if (string.Equals(facet, _lpMeasure.FieldNames[1], StringComparison.Ordinal))
                _count1 += count;
            else if (string.Equals(facet, _lpMeasure.FieldNames[2], StringComparison.Ordinal))
                _count2 += count;
            else if (string.Equals(facet, _lpMeasure.FieldNames[3], StringComparison.Ordinal))
                _count3 += count;
            else
                throw new ArgumentException("Unknown facet");


            _countTotal++;
        }

        public override IPointData Emit(DateTime timestamp) => new PointData(this, _countTotal, _count1, _count2, _count3, timestamp);

        public class PointData : IPointData
        {
            private readonly Faceted3Counter _counter;
            private readonly long _value;
            private readonly long _facet1;
            private readonly long _facet2;
            private readonly long _facet3;
            private readonly DateTime _timestamp;

            public PointData(Faceted3Counter counter, long value, long facet1, long facet2, long facet3, DateTime timestamp)
            {
                _counter = counter;
                _value = value;
                _facet1 = facet1;
                _facet2 = facet2;
                _facet3 = facet3;
                _timestamp = timestamp;
            }

            public ILineProtocolPoint AsLineProtocolPoint() => _counter._lpMeasure.AddPoint(_value, _facet1, _facet2, _facet3, _counter.Tags.Values, _timestamp);
        }

    }

    public abstract class Measurement : IMeasurement
    {
        protected Measurement(string name, IReadOnlyDictionary<string, string> tags)
        {
            Name = name;
            Tags = tags?.ToDictionary(k => k.Key, v => v.Value);
        }

        public string Name { get; }

        public IDictionary<string, string> Tags { get; set; }

        public abstract IPointData Emit(DateTime utcTimestamp);
    }
}
