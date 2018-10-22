using System;
using System.Collections.Generic;
using System.IO;

namespace InfluxDB.LineProtocol.Payload
{
    /// <summary>
    /// Basic implementation of <see cref="ILineProtocolPoint"/>
    /// where all data (fields and tags) are provided.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This implementation is the most versatile, but also the most resource-consuming, as all informations (measurement, fields name, tags names) are stored in this instance.
    /// </para>
    /// <para>
    /// If you plan to share some information between multiple points, you could take advantage of the <see cref="LineProtocolMeasure"/> and <see cref="LineProtocolMeasurePoint"/>
    /// which allows to share fixed informations (like field names and tag names) between multiple points.
    /// </para>
    /// <para>If you don't want to use a single measure to generate multiple points, you can still use <see cref="LineProtocolPoint{T1}"/> or <see cref="LineProtocolPoint{T1, T2}"/>
    /// which optimize payload when you have only one or two fixed values.
    /// </para>
    /// </remarks>
    public class LineProtocolPoint : ILineProtocolPoint
    {
        public LineProtocolPoint(
            string measurement,
            IEnumerable<KeyValuePair<string, object>> fields,
            IEnumerable<KeyValuePair<string, string>> tags = null,
            DateTime? utcTimestamp = null)
        {
            if (string.IsNullOrEmpty(measurement)) throw new ArgumentException("A measurement name must be specified");
            if (fields == null) throw new ArgumentException("At least one field must be specified");

            var hasFields = false;
            foreach (var f in fields)
            {
                hasFields = true;
                if (string.IsNullOrEmpty(f.Key)) throw new ArgumentException("Fields must have non-empty names");
            }

            if (!hasFields) throw new ArgumentException("At least one field must be specified");

            if (tags != null)
                foreach (var t in tags)
                    if (string.IsNullOrEmpty(t.Key)) throw new ArgumentException("Tags must have non-empty names");

            if (utcTimestamp != null && utcTimestamp.Value.Kind != DateTimeKind.Utc)
                throw new ArgumentException("Timestamps must be specified as UTC");

            Measurement = measurement;
            Fields = fields;
            Tags = tags;
            UtcTimestamp = utcTimestamp;
        }

        public string Measurement { get; }

        public IEnumerable<KeyValuePair<string, object>> Fields { get; }

        public IEnumerable<KeyValuePair<string, string>> Tags { get; }

        public DateTime? UtcTimestamp { get; }

        public void Format(TextWriter textWriter)
        {
            if (textWriter == null) throw new ArgumentNullException(nameof(textWriter));

            textWriter.WriteLPNameEscaped(Measurement);
            textWriter.WriteLPTags(Tags);

            var fieldDelim = ' ';
            foreach (var f in Fields)
            {
                textWriter.Write(fieldDelim);
                fieldDelim = ',';
                textWriter.WriteLPNameEscaped(f.Key);
                textWriter.Write('=');
                textWriter.WriteLPValue(f.Value);
            }

            textWriter.WriteLPTimestamp(UtcTimestamp);
        }
    }

    public class LineProtocolPoint<T1> : ILineProtocolPoint
    {
        private static readonly Action<TextWriter, T1> _writer = LineProtocolSyntax.GetWriter<T1>();

        public LineProtocolPoint(
            string measurement,
            string fieldName,
            T1 fieldValue,
            IEnumerable<KeyValuePair<string, string>> tags = null,
            DateTime? utcTimestamp = null)
        {
            if (string.IsNullOrEmpty(measurement)) throw new ArgumentException("A measurement name must be specified", nameof(measurement));
            if (string.IsNullOrEmpty(fieldName)) throw new ArgumentException("Field must have a non-empty name", nameof(fieldName));

            if (tags != null)
                foreach (var t in tags)
                    if (string.IsNullOrEmpty(t.Key)) throw new ArgumentException("Tags must have non-empty names", nameof(tags));

            if (utcTimestamp != null && utcTimestamp.Value.Kind != DateTimeKind.Utc)
                throw new ArgumentException("Timestamps must be specified as UTC", nameof(utcTimestamp));

            Measurement = measurement;
            FieldName = fieldName;
            FieldValue = fieldValue;
            Tags = tags;
            UtcTimestamp = utcTimestamp;
        }

        public string Measurement { get; }

        public string FieldName { get; }

        public T1 FieldValue { get; }

        public IEnumerable<KeyValuePair<string, string>> Tags { get; }

        public DateTime? UtcTimestamp { get; }

        public void Format(TextWriter textWriter)
        {
            if (textWriter == null) throw new ArgumentNullException(nameof(textWriter));

            textWriter.WriteLPNameEscaped(Measurement);
            textWriter.WriteLPTags(Tags);

            textWriter.Write(' ');
            textWriter.WriteLPNameEscaped(FieldName);
            textWriter.Write('=');
            _writer(textWriter, FieldValue);

            textWriter.WriteLPTimestamp(UtcTimestamp);
        }
    }

    public class LineProtocolPoint<T1, T2> : ILineProtocolPoint
    {
        private static readonly Action<TextWriter, T1> _writer1 = LineProtocolSyntax.GetWriter<T1>();
        private static readonly Action<TextWriter, T2> _writer2 = LineProtocolSyntax.GetWriter<T2>();

        public LineProtocolPoint(
            string measurement,
            string field1Name,
            T1 field1Value,
            string field2Name,
            T2 field2Value,
            IEnumerable<KeyValuePair<string, string>> tags = null,
            DateTime? utcTimestamp = null)
        {
            if (string.IsNullOrEmpty(measurement)) throw new ArgumentException("A measurement name must be specified");
            if (string.IsNullOrEmpty(field1Name)) throw new ArgumentException("Field1 must have a non-empty name");
            if (string.IsNullOrEmpty(field2Name)) throw new ArgumentException("Field2 must have a non-empty name");

            if (tags != null)
                foreach (var t in tags)
                    if (string.IsNullOrEmpty(t.Key)) throw new ArgumentException("Tags must have non-empty names");

            if (utcTimestamp != null && utcTimestamp.Value.Kind != DateTimeKind.Utc)
                throw new ArgumentException("Timestamps must be specified as UTC");

            Measurement = measurement;
            Field1Name = field1Name;
            Field1Value = field1Value;
            Field2Name = field2Name;
            Field2Value = field2Value;
            Tags = tags;
            UtcTimestamp = utcTimestamp;
        }

        public string Measurement { get; }
        public string Field1Name { get; }
        public T1 Field1Value { get; }
        public string Field2Name { get; }
        public T2 Field2Value { get; }
        public IEnumerable<KeyValuePair<string, string>> Tags { get; }
        public DateTime? UtcTimestamp { get; }

        public void Format(TextWriter textWriter)
        {
            if (textWriter == null) throw new ArgumentNullException(nameof(textWriter));

            textWriter.WriteLPNameEscaped(Measurement);

            textWriter.WriteLPNameEscaped(Measurement);
            textWriter.WriteLPTags(Tags);

            textWriter.Write(' ');
            textWriter.WriteLPNameEscaped(Field1Name);
            textWriter.Write('=');
            _writer1(textWriter, Field1Value);

            textWriter.Write(',');
            textWriter.WriteLPNameEscaped(Field2Name);
            textWriter.Write('=');
            _writer2(textWriter, Field2Value);

            textWriter.WriteLPTimestamp(UtcTimestamp);
        }
    }
}

