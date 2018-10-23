using InfluxDB.LineProtocol.Payload;

namespace InfluxDB.Collector.Pipeline
{
    public class MeasurementPointData<T> : IPointData
    {
        private IMeasurement _measurement;
        private LineProtocolMeasure<T> _lpMeasure;

        public MeasurementPointData(LineProtocolMeasure<T> lpMeasure, T value)
        {
            _lpMeasure = lpMeasure;
            Value = value;
        }

        public T Value { get; }

        public ILineProtocolPoint AsLineProtocolPoint() => _lpMeasure.AddPoint(Value, _measurement.Tags.Values);
    }

    public class MeasurementPointData<T1, T2> : IPointData
    {
        private IMeasurement _measurement;
        private LineProtocolMeasure<T1, T2> _lpMeasure;

        public MeasurementPointData(LineProtocolMeasure<T1, T2> lpMeasure, T1 value1, T2 value2)
        {
            _lpMeasure = lpMeasure;
            Value1 = value1;
            Value2 = value2;
        }

        public T1 Value1 { get; }

        public T2 Value2 { get; }

        public ILineProtocolPoint AsLineProtocolPoint() => _lpMeasure.AddPoint(Value1, Value2, _measurement.Tags.Values);
    }

    public class MeasurementPointData<T1, T2, T3> : IPointData
    {
        private IMeasurement _measurement;
        private LineProtocolMeasure<T1, T2, T3> _lpMeasure;

        public MeasurementPointData(LineProtocolMeasure<T1, T2, T3> lpMeasure, T1 value1, T2 value2, T3 value3)
        {
            _lpMeasure = lpMeasure;
            Value1 = value1;
            Value2 = value2;
            Value3 = value3;
        }

        public T1 Value1 { get; }

        public T2 Value2 { get; }

        public T3 Value3 { get; }

        public ILineProtocolPoint AsLineProtocolPoint() => _lpMeasure.AddPoint(Value1, Value2, Value3, _measurement.Tags.Values);
    }

    public class MeasurementPointData<T1, T2, T3, T4> : IPointData
    {
        private IMeasurement _measurement;
        private LineProtocolMeasure<T1, T2, T3, T4> _lpMeasure;

        public MeasurementPointData(LineProtocolMeasure<T1, T2, T3, T4> lpMeasure, T1 value1, T2 value2, T3 value3, T4 value4)
        {
            _lpMeasure = lpMeasure;
            Value1 = value1;
            Value2 = value2;
            Value3 = value3;
            value4 = value4;
        }

        public T1 Value1 { get; }

        public T2 Value2 { get; }

        public T3 Value3 { get; }

        public ILineProtocolPoint AsLineProtocolPoint() => _lpMeasure.AddPoint(Value1, Value2, Value3, _measurement.Tags.Values);
    }
}
