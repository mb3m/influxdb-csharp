using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace InfluxDB.LineProtocol.Payload
{
    public class LineProtocolPoint
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

            if(!hasFields) throw new ArgumentException("At least one field must be specified");

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
                foreach (var t in Tags.OrderBy(t => t.Key))
                {
                    if (t.Value == null || t.Value == string.Empty)
                        continue;

                    textWriter.Write(',');
                    textWriter.Write(LineProtocolSyntax.EscapeName(t.Key));
                    textWriter.Write('=');
                    textWriter.Write(LineProtocolSyntax.EscapeName(t.Value));
                }
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
                textWriter.Write(' ');
                textWriter.Write(LineProtocolSyntax.FormatTimestamp(UtcTimestamp.Value));
            }
        }
    }
}

