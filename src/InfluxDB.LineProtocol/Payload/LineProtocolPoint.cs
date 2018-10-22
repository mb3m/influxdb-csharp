using System;
using System.Collections.Generic;
using System.IO;

namespace InfluxDB.LineProtocol.Payload
{
    public interface ILineProtocolPoint
    {
        void Format(TextWriter textWriter);
    }

    public class LineProtocolPoint<T, K> : ILineProtocolPoint
    {
        private static readonly Action<TextWriter, T> Writer1 = LineProtocolSyntax.GetWriter<T>();
        private static readonly Action<TextWriter, K> Writer2 = LineProtocolSyntax.GetWriter<K>();

        public string Measurement { get; }
        public string Field1Name { get; }
        public T Field1Value { get; }
        public string Field2Name { get; }
        public K Field2Value { get; }
        public IEnumerable<KeyValuePair<string, string>> Tags { get; }
        public DateTime? UtcTimestamp { get; }

        public LineProtocolPoint(
            string measurement,
            string field1Name,
            T field1Value,
            string field2Name,
            K field2Value,
            IEnumerable<KeyValuePair<string, string>> tags = null,
            DateTime? utcTimestamp = null)
        {
            if (string.IsNullOrEmpty(measurement)) throw new ArgumentException("A measurement name must be specified");
            if (string.IsNullOrEmpty(field1Name)) throw new ArgumentException("Field1 must have non-empty name");
            if (string.IsNullOrEmpty(field2Name)) throw new ArgumentException("Field2 must have non-empty name");

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

        public void Format(TextWriter textWriter)
        {
            if (textWriter == null) throw new ArgumentNullException(nameof(textWriter));

            textWriter.WriteLPNameEscaped(Measurement);

            textWriter.WriteLPNameEscaped(Measurement);
            textWriter.WriteLPTags(Tags);

            textWriter.Write(' ');
            textWriter.WriteLPNameEscaped(Field1Name);
            textWriter.Write('=');
            Writer1(textWriter, Field1Value);
            textWriter.Write(',');
            textWriter.WriteLPNameEscaped(Field2Name);
            textWriter.Write('=');
            Writer2(textWriter, Field2Value);

            textWriter.WriteLPTimestamp(UtcTimestamp);
        }
    }

    public class LineProtocolPoint<T> : ILineProtocolPoint
    {
        private static readonly Action<TextWriter, T> Writer = LineProtocolSyntax.GetWriter<T>();

        public string Measurement { get; }
        public string FieldName { get; }
        public T FieldValue { get; }
        public IEnumerable<KeyValuePair<string, string>> Tags { get; }
        public DateTime? UtcTimestamp { get; }

        public LineProtocolPoint(
            string measurement,
            string fieldName,
            T fieldValue,
            IEnumerable<KeyValuePair<string, string>> tags = null,
            DateTime? utcTimestamp = null)
        {
            if (string.IsNullOrEmpty(measurement)) throw new ArgumentException("A measurement name must be specified");
            if (string.IsNullOrEmpty(fieldName)) throw new ArgumentException("Field must have non-empty name");

            if (tags != null)
                foreach (var t in tags)
                    if (string.IsNullOrEmpty(t.Key)) throw new ArgumentException("Tags must have non-empty names");

            if (utcTimestamp != null && utcTimestamp.Value.Kind != DateTimeKind.Utc)
                throw new ArgumentException("Timestamps must be specified as UTC");

            Measurement = measurement;
            FieldName = fieldName;
            FieldValue = fieldValue;
            Tags = tags;
            UtcTimestamp = utcTimestamp;
        }

        public void Format(TextWriter textWriter)
        {
            if (textWriter == null) throw new ArgumentNullException(nameof(textWriter));

            textWriter.WriteLPNameEscaped(Measurement);
            textWriter.WriteLPTags(Tags);

            textWriter.Write(' ');
            textWriter.Write(LineProtocolSyntax.EscapeName(FieldName));
            textWriter.Write('=');
            Writer(textWriter, FieldValue);

            textWriter.WriteLPTimestamp(UtcTimestamp);
        }
    }

    public class LineProtocolPoint : ILineProtocolPoint
    {
        public string Measurement { get; }
        public IEnumerable<KeyValuePair<string, object>> Fields { get; }
        public IEnumerable<KeyValuePair<string, string>> Tags { get; }
        public DateTime? UtcTimestamp { get; }

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
}

