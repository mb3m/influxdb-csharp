using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace InfluxDB.LineProtocol.Payload
{
    public interface ILineProtocolPoint
    {
        void Format(TextWriter textWriter);
    }

    public class LineProtocolPoint<T, K> : LineProtocolPointBase, ILineProtocolPoint
    {
        private static readonly Func<object, string> Formatter1 = LineProtocolSyntax.GetFormatter(typeof(T));
        private static readonly Func<object, string> Formatter2 = LineProtocolSyntax.GetFormatter(typeof(K));

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

            textWriter.Write(LineProtocolSyntax.EscapeName(Measurement));

            if (Tags != null)
            {
                FormatTags(Tags, textWriter);
            }

            textWriter.Write(' ');
            textWriter.Write(LineProtocolSyntax.EscapeName(Field1Name));
            textWriter.Write('=');
            textWriter.Write(Formatter1(Field1Value));
            textWriter.Write(',');
            textWriter.Write(LineProtocolSyntax.EscapeName(Field2Name));
            textWriter.Write('=');
            textWriter.Write(LineProtocolSyntax.FormatValue(Field2Value));

            if (UtcTimestamp != null)
            {
                WriteUtcTimestamp(UtcTimestamp.Value, textWriter);
            }
        }
    }

    public class LineProtocolPoint<T> : LineProtocolPointBase, ILineProtocolPoint
    {
        private static readonly Func<object, string> Formatter = LineProtocolSyntax.GetFormatter(typeof(T));

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

            textWriter.Write(LineProtocolSyntax.EscapeName(Measurement));

            if (Tags != null)
            {
                FormatTags(Tags, textWriter);
            }

            textWriter.Write(' ');
            textWriter.Write(LineProtocolSyntax.EscapeName(FieldName));
            textWriter.Write('=');
            textWriter.Write(Formatter(FieldValue));

            if (UtcTimestamp != null)
            {
                WriteUtcTimestamp(UtcTimestamp.Value, textWriter);
            }
        }
    }

    public class LineProtocolPoint : LineProtocolPointBase, ILineProtocolPoint
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

            textWriter.Write(LineProtocolSyntax.EscapeName(Measurement));

            if (Tags != null)
            {
                FormatTags(Tags, textWriter);
            }

            var fieldDelim = ' ';
            foreach (var f in Fields)
            {
                textWriter.Write(fieldDelim);
                fieldDelim = ',';
                textWriter.Write(LineProtocolSyntax.EscapeName(f.Key));
                textWriter.Write('=');
                textWriter.Write(LineProtocolSyntax.FormatValue(f.Value));
            }

            if (UtcTimestamp != null)
            {
                WriteUtcTimestamp(UtcTimestamp.Value, textWriter);
            }
        }
    }

    public class LineProtocolPointBase
    {
        protected void FormatTags(IEnumerable<KeyValuePair<string, string>> tags, TextWriter textWriter)
        {
            foreach (var t in tags.OrderBy(t => t.Key))
            {
                if (t.Value == null || t.Value == string.Empty)
                    continue;

                textWriter.Write(',');
                textWriter.Write(LineProtocolSyntax.EscapeName(t.Key));
                textWriter.Write('=');
                textWriter.Write(LineProtocolSyntax.EscapeName(t.Value));
            }
        }

        protected void WriteUtcTimestamp(DateTime utcTimestamp, TextWriter textWriter)
        {
            textWriter.Write(' ');
            textWriter.Write(LineProtocolSyntax.FormatTimestamp(utcTimestamp));
        }
    }
}

