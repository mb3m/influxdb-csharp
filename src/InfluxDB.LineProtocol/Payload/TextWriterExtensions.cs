using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace InfluxDB.LineProtocol.Payload
{
    internal static class TextWriterExtensions
    {
        public static void WriteLPNameEscaped(this TextWriter @this, string nameOrKey)
        {
            if (nameOrKey == null) throw new ArgumentNullException(nameof(nameOrKey));

            int l = nameOrKey.Length;
            for (int i = 0; i < l; i++)
            {
                var c = nameOrKey[i];
                if (c == '=' || c == ',' || c == ' ')
                {
                    @this.Write('\\');
                }

                @this.Write(c);
            }
        }

        /// <summary>
        /// Write a collection of tags using the line protocol format.
        /// This methods takes care of ordering the tags by names, and escaping names and values.
        /// </summary>
        public static void WriteLPTags(this TextWriter @this, IEnumerable<KeyValuePair<string, string>> tags)
        {
            if (tags == null) return;

            foreach (var t in tags.OrderBy(t => t.Key))
            {
                if (!string.IsNullOrEmpty(t.Value))
                {
                    @this.Write(',');
                    @this.WriteLPNameEscaped(t.Key);
                    @this.Write('=');
                    @this.WriteLPNameEscaped(t.Value);
                }
            }
        }

        /// <summary>
        /// Write a collection of tags using the line protocol format.
        /// This methods expects that the names are already escaped and ordered.
        /// It takes care of escaping tag values.
        /// </summary>
        public static void WriteLPTags(this TextWriter @this, string[] tagNames, IEnumerable<string> tagValues)
        {
            if (tagNames == null) return;

            var i = 0;

            foreach (var tagValue in tagValues)
            {
                if (!string.IsNullOrEmpty(tagValue))
                {
                    // we stop as soon as we do not have any name left, ignoring all remaining tag values
                    if (i >= tagNames.Length) return;

                    @this.Write(',');
                    @this.Write(tagNames[i]); // no need to escape tag names, they are already escaped in LineProtocolMeasureBase ctor
                    @this.Write('=');
                    @this.WriteLPNameEscaped(tagValue);
                }

                i++;
            }
        }

        /// <summary>
        /// Write a collection of fields using the line protocol format.
        /// This method expects that the names are already escaped.
        /// </summary>
        public static void WriteLPFields(this TextWriter @this, string[] names, IEnumerable<object> values)
        {
            var fieldDelim = ' ';
            var i = 0;

            foreach (var value in values)
            {
                // we stop as soon as we do not have any name left, ignoring all remaining field values
                if (i >= names.Length) return;

                @this.Write(fieldDelim);
                fieldDelim = ',';
                @this.Write(names[i]);// no need to escape field names, they are already escaped in LineProtocolMeasureBase ctor
                @this.Write('=');
                @this.WriteLPValue(value);

                i++;
            }
        }

        public static void WriteLPTimestamp(this TextWriter @this, DateTime? utcTimestamp)
        {
            if (utcTimestamp == null)
            {
                return;
            }

            @this.Write(' ');
            @this.Write(LineProtocolSyntax.AsTimestamp(utcTimestamp.Value).ToString(CultureInfo.InvariantCulture));
        }

        public static void WriteLPValue(this TextWriter @this, object value) => LineProtocolSyntax.WriteObject(@this, value);
    }
}
