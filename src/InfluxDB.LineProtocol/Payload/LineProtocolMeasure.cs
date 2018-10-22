using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace InfluxDB.LineProtocol.Payload
{
    public abstract class LineProtocolMeasureBase
    {
        protected LineProtocolMeasureBase(string measurement, IEnumerable<string> fieldNames, IEnumerable<string> tagNames = null)
        {
            if (string.IsNullOrEmpty(measurement)) throw new ArgumentException("A measurement name must be specified");
            if (fieldNames == null) throw new ArgumentException("At least one field must be specified");

            FieldNames = fieldNames
                .Select(t => LineProtocolSyntax.EscapeName(t))
                .ToArray();

            if (FieldNames.Length == 0) throw new ArgumentException("At least one field must be specified");

            TagNames = tagNames
                ?.OrderBy(t => t)
                ?.Select(t => LineProtocolSyntax.EscapeName(t))
                ?.ToArray();

            if (tagNames != null)
                foreach (var t in tagNames)
                    if (string.IsNullOrEmpty(t)) throw new ArgumentException("Tags must have non-empty names");

            Measurement = measurement;
        }

        public string[] FieldNames { get; }

        public string[] TagNames { get; }

        public string Measurement { get; }

        public static LineProtocolMeasure<T> Create<T>(string measurement, string fieldName, IEnumerable<string> tagNames = null)
        {
            return new LineProtocolMeasure<T>(measurement, fieldName, tagNames);
        }

        public static LineProtocolMeasure<T1, T2> Create<T1, T2>(string measurement, string field1Name, string field2Name, IEnumerable<string> tagNames = null)
        {
            return new LineProtocolMeasure<T1, T2>(measurement, field1Name, field2Name, tagNames);
        }

        public static LineProtocolMeasure<T1, T2, T3> Create<T1, T2, T3>(string measurement, string field1Name, string field2Name, string field3Name, IEnumerable<string> tagNames = null)
        {
            return new LineProtocolMeasure<T1, T2, T3>(measurement, field1Name, field2Name, field3Name, tagNames);
        }

        public static LineProtocolMeasure<T1, T2, T3, T4> Create<T1, T2, T3, T4>(string measurement, string field1Name, string field2Name, string field3Name, string field4Name, IEnumerable<string> tagNames = null)
        {
            return new LineProtocolMeasure<T1, T2, T3, T4>(measurement, field1Name, field2Name, field3Name, field4Name, tagNames);
        }
    }
    public class LineProtocolMeasure : LineProtocolMeasureBase
    {
        public LineProtocolMeasure(string measurement, IEnumerable<string> fieldNames, IEnumerable<string> tagNames = null)
            : base(measurement, fieldNames, tagNames)
        {
        }

        public ILineProtocolPoint AddPoint(IReadOnlyList<object> fieldValues, IReadOnlyList<string> tagValues = null, DateTime? utcTimestamp = null)
        {
            return new LineProtocolMeasurePoint(this, fieldValues, tagValues, utcTimestamp);
        }
    }

    public class LineProtocolMeasure<T> : LineProtocolMeasureBase
    {
        public LineProtocolMeasure(string measurement, string fieldName, IEnumerable<string> tagNames = null)
            : base(measurement, new[] { fieldName }, tagNames)
        {
        }

        public Action<T, TextWriter> FieldValueWriter { get; } = LineProtocolSyntax.GetWriter<T>();

        public ILineProtocolPoint AddPoint(T fieldValue, IReadOnlyList<string> tagValues = null, DateTime? utcTimestamp = null)
        {
            return new LineProtocolMeasurePoint<T>(this, fieldValue, tagValues, utcTimestamp);
        }
    }

    public class LineProtocolMeasure<T1, T2> : LineProtocolMeasureBase
    {
        public LineProtocolMeasure(string measurement, string field1Name, string field2Name, IEnumerable<string> tagNames = null)
            : base(measurement, new[] { field1Name, field2Name }, tagNames)
        {
        }

        public Action<T1, TextWriter> Field1ValueWriter { get; } = LineProtocolSyntax.GetWriter<T1>();

        public Action<T2, TextWriter> Field2ValueWriter { get; } = LineProtocolSyntax.GetWriter<T2>();

        public ILineProtocolPoint AddPoint(T1 field1Value, T2 field2Value, IReadOnlyList<string> tagValues = null, DateTime? utcTimestamp = null)
        {
            return new LineProtocolMeasurePoint<T1, T2>(this, field1Value, field2Value, tagValues, utcTimestamp);
        }
    }

    public class LineProtocolMeasure<T1, T2, T3> : LineProtocolMeasureBase
    {
        public LineProtocolMeasure(string measurement, string field1Name, string field2Name, string field3Name, IEnumerable<string> tagNames = null)
            : base(measurement, new[] { field1Name, field2Name, field3Name }, tagNames)
        {
        }

        public Action<T1, TextWriter> Field1ValueWriter { get; } = LineProtocolSyntax.GetWriter<T1>();

        public Action<T2, TextWriter> Field2ValueWriter { get; } = LineProtocolSyntax.GetWriter<T2>();

        public Action<T3, TextWriter> Field3ValueWriter { get; } = LineProtocolSyntax.GetWriter<T3>();

        public ILineProtocolPoint AddPoint(T1 field1Value, T2 field2Value, T3 field3Value, IReadOnlyList<string> tagValues = null, DateTime? utcTimestamp = null)
        {
            return new LineProtocolMeasurePoint<T1, T2, T3>(this, field1Value, field2Value, field3Value, tagValues, utcTimestamp);
        }
    }

    public class LineProtocolMeasure<T1, T2, T3, T4> : LineProtocolMeasureBase
    {
        public LineProtocolMeasure(string measurement, string field1Name, string field2Name, string field3Name, string field4Name, IEnumerable<string> tagNames = null)
            : base(measurement, new[] { field1Name, field2Name, field3Name }, tagNames)
        {
        }

        public Action<T1, TextWriter> Field1ValueWriter { get; } = LineProtocolSyntax.GetWriter<T1>();

        public Action<T2, TextWriter> Field2ValueWriter { get; } = LineProtocolSyntax.GetWriter<T2>();

        public Action<T3, TextWriter> Field3ValueWriter { get; } = LineProtocolSyntax.GetWriter<T3>();

        public Action<T4, TextWriter> Field4ValueWriter { get; } = LineProtocolSyntax.GetWriter<T4>();

        public ILineProtocolPoint AddPoint(T1 field1Value, T2 field2Value, T3 field3Value, T4 field4Value, IReadOnlyList<string> tagValues = null, DateTime? utcTimestamp = null)
        {
            return new LineProtocolMeasurePoint<T1, T2, T3, T4>(this, field1Value, field2Value, field3Value, field4Value, tagValues, utcTimestamp);
        }
    }
}