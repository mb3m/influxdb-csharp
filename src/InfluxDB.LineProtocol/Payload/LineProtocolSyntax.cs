using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;

namespace InfluxDB.LineProtocol.Payload
{
    internal class LineProtocolSyntax
    {
        static readonly DateTime Origin = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        static readonly Dictionary<Type, Func<object, string>> Formatters = new Dictionary<Type, Func<object, string>>
        {
            { typeof(sbyte), FormatInteger },
            { typeof(byte), FormatInteger },
            { typeof(short), FormatInteger },
            { typeof(ushort), FormatInteger },
            { typeof(int), FormatInteger },
            { typeof(uint), FormatInteger },
            { typeof(long), FormatInteger },
            { typeof(ulong), FormatInteger },
            { typeof(float), FormatFloat },
            { typeof(double), FormatFloat },
            { typeof(decimal), FormatFloat },
            { typeof(bool), FormatBoolean },
            { typeof(TimeSpan), FormatTimespan }
        };

        internal static readonly Dictionary<Type, Delegate> Writers = new Dictionary<Type, Delegate>
        {
            { typeof(sbyte), (Action<TextWriter, sbyte>)WriteSByte },
            { typeof(byte), (Action<TextWriter, byte>)WriteByte },
            { typeof(short), (Action<TextWriter, short>)WriteInt16 },
            { typeof(ushort), (Action<TextWriter, ushort>)WriteUInt16 },
            { typeof(int), (Action<TextWriter, int>)WriteInt32 },
            { typeof(uint), (Action<TextWriter, uint>)WriteUInt32 },
            { typeof(long), (Action<TextWriter, long>)WriteInt64 },
            { typeof(ulong), (Action<TextWriter, ulong>)WriteUInt64 },
            { typeof(float), (Action<TextWriter, float>)WriteSingle },
            { typeof(double), (Action<TextWriter, double>)WriteDouble },
            { typeof(decimal), (Action<TextWriter, decimal>)WriteDecimal},
            { typeof(bool), (Action<TextWriter, bool>)WriteBoolean},
            { typeof(TimeSpan), (Action<TextWriter, TimeSpan>)WriteTimeSpan},
            { typeof(string), (Action<TextWriter, string>)WriteString },
            { typeof(object), (Action<TextWriter, object>)WriteObject }
        };

        public static string EscapeName(string nameOrKey)
        {
            if (nameOrKey == null) throw new ArgumentNullException(nameof(nameOrKey));
            return nameOrKey
                .Replace("=", "\\=")
                .Replace(" ", "\\ ")
                .Replace(",", "\\,");
        }

        public static Action<TextWriter, T> GetWriter<T>()
        {
            if (!Writers.TryGetValue(typeof(T), out var action))
            {
                // generate object method call
                // Action<T, TextWriter> a = (p1, p2) => WriteObject((object)p1, p2);
                // Action<TextWriter, T> a = (p1, p2) => WriteObject(p1, (object)p2);
                var param2 = Expression.Parameter(typeof(T));
                var param1 = Expression.Parameter(typeof(TextWriter));
                var convert = Expression.Convert(param1, typeof(object));
                var call = Expression.Call(typeof(LineProtocolSyntax).GetTypeInfo().GetMethod(nameof(WriteObject)), convert, param2);
                action = Expression.Lambda<Action<TextWriter, T>>(call, param1, param2).Compile();
                Writers[typeof(T)] = action;
            }

            return (Action<TextWriter, T>)action;
        }

        public static void WriteObject(TextWriter writer, object value)
        {
            value = value ?? string.Empty;
            WriteString(writer, value?.ToString() ?? string.Empty);
        }

        public static string FormatValue(object value)
        {
            var v = value ?? "";
            Func<object, string> format;
            if (Formatters.TryGetValue(v.GetType(), out format))
                return format(v);
            return FormatString(v.ToString());
        }

        public static void WriteSByte(TextWriter writer, sbyte value)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
            writer.Write('i');
        }

        public static void WriteByte(TextWriter writer, byte value)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
            writer.Write('i');
        }

        public static void WriteInt16(TextWriter writer, short value)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
            writer.Write('i');
        }

        public static void WriteUInt16(TextWriter writer, ushort value)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
            writer.Write('i');
        }

        public static void WriteInt32(TextWriter writer, int value)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
            writer.Write('i');
        }

        public static void WriteUInt32(TextWriter writer, uint value)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
            writer.Write('i');
        }

        public static void WriteInt64(TextWriter writer, long value)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
            writer.Write('i');
        }

        public static void WriteUInt64(TextWriter writer, ulong value)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
            writer.Write('i');
        }

        public static void WriteSingle(TextWriter writer, float value)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
        }

        public static void WriteDouble(TextWriter writer, double value)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
        }

        public static void WriteDecimal(TextWriter writer, decimal value)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
        }

        public static void WriteBoolean(TextWriter writer, bool value)
        {
            writer.Write(value ? 't' : 'f');
        }

        public static void WriteTimeSpan(TextWriter writer, TimeSpan value)
        {
            writer.Write(value.TotalMilliseconds.ToString(CultureInfo.InvariantCulture));
        }

        public static void WriteString(TextWriter writer, string value)
        {
            writer.Write('"');
            if (value.IndexOf('"') == -1)
                writer.Write(value);
            else
                writer.Write(value.Replace("\"", "\\\""));
            writer.Write('"');
        }

        static string FormatInteger(object i)
        {
            return ((IFormattable)i).ToString(null, CultureInfo.InvariantCulture) + "i";
        }

        static string FormatFloat(object f)
        {
            return ((IFormattable)f).ToString(null, CultureInfo.InvariantCulture);
        }

        static string FormatTimespan(object ts)
        {
            return ((TimeSpan)ts).TotalMilliseconds.ToString(CultureInfo.InvariantCulture);
        }

        static string FormatBoolean(object b)
        {
            return ((bool)b) ? "t" : "f";
        }

        static string FormatString(object o) => FormatString(o?.ToString() ?? string.Empty);

        static string FormatString(string s)
        {
            return "\"" + s.Replace("\"", "\\\"") + "\"";
        }

        public static long AsTimestamp(DateTime utcTimestamp)
        {
            return (utcTimestamp - Origin).Ticks * 100L;
        }
    }
}
