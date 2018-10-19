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
            { typeof(sbyte), (Action<sbyte, TextWriter>)WriteSByte },
            { typeof(byte), (Action<byte, TextWriter>)WriteByte },
            { typeof(short), (Action<short, TextWriter>)WriteInt16 },
            { typeof(ushort), (Action<ushort, TextWriter>)WriteUInt16 },
            { typeof(int), (Action<int, TextWriter>)WriteInt32 },
            { typeof(uint), (Action<uint, TextWriter>)WriteUInt32 },
            { typeof(long), (Action<long, TextWriter>)WriteInt64 },
            { typeof(ulong), (Action<ulong, TextWriter>)WriteUInt64 },
            { typeof(float), (Action<float, TextWriter>)WriteSingle },
            { typeof(double), (Action<double, TextWriter>)WriteDouble },
            { typeof(decimal), (Action<decimal, TextWriter>)WriteDecimal},
            { typeof(bool), (Action<bool, TextWriter>)WriteBoolean},
            { typeof(TimeSpan), (Action<TimeSpan, TextWriter>)WriteTimeSpan},
            { typeof(string), (Action<string, TextWriter>)WriteString },
            { typeof(object), (Action<object, TextWriter>)WriteObject }

        };

        public static string EscapeName(string nameOrKey)
        {
            if (nameOrKey == null) throw new ArgumentNullException(nameof(nameOrKey));
            return nameOrKey
                .Replace("=", "\\=")
                .Replace(" ", "\\ ")
                .Replace(",", "\\,");
        }

        public static Func<object, string> GetFormatter(Type type)
        {
            return Formatters.TryGetValue(type, out var formatter) ? formatter : FormatString;
        }

        public static Action<T, TextWriter> GetWriter<T>()
        {
            if (!Writers.TryGetValue(typeof(T), out var action))
            {
                // generate object method call
                // Action<T, TextWriter> a = (p1, p2) => WriteObject((object)p1, p2);
                var param1 = Expression.Parameter(typeof(T));
                var param2 = Expression.Parameter(typeof(TextWriter));
                var convert = Expression.Convert(param1, typeof(object));
                var call = Expression.Call(typeof(LineProtocolSyntax).GetTypeInfo().GetMethod("WriteObject"), convert, param2);
                action = Expression.Lambda<Action<T, TextWriter>>(call, param1, param2).Compile();
                Writers[typeof(T)] = action;
            }

            return (Action<T, TextWriter>)action;
        }

        public static void WriteObject(object value, TextWriter writer) => WriteString(value?.ToString() ?? string.Empty, writer);

        public static void WriteSByte(sbyte value, TextWriter writer)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
            writer.Write('i');
        }

        public static void WriteByte(byte value, TextWriter writer)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
            writer.Write('i');
        }

        public static void WriteInt16(short value, TextWriter writer)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
            writer.Write('i');
        }

        public static void WriteUInt16(ushort value, TextWriter writer)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
            writer.Write('i');
        }

        public static void WriteInt32(int value, TextWriter writer)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
            writer.Write('i');
        }

        public static void WriteUInt32(uint value, TextWriter writer)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
            writer.Write('i');
        }

        public static void WriteInt64(long value, TextWriter writer)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
            writer.Write('i');
        }

        public static void WriteUInt64(ulong value, TextWriter writer)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
            writer.Write('i');
        }

        public static void WriteSingle(float value, TextWriter writer)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
        }

        public static void WriteDouble(double value, TextWriter writer)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
        }

        public static void WriteDecimal(decimal value, TextWriter writer)
        {
            writer.Write(value.ToString(CultureInfo.InvariantCulture));
        }

        public static void WriteBoolean(bool value, TextWriter writer)
        {
            writer.Write(value ? 't' : 'f');
        }

        public static void WriteTimeSpan(TimeSpan value, TextWriter writer)
        {
            writer.Write(value.TotalMilliseconds.ToString(CultureInfo.InvariantCulture));
        }

        public static void WriteString(string value, TextWriter writer)
        {
            writer.Write('"');
            if (value.IndexOf('"') == -1)
                writer.Write(value);
            else
                writer.Write(value.Replace("\"", "\\\""));
            writer.Write('"');
        }

        public static string FormatValue(object value)
        {
            var v = value ?? "";
            Func<object, string> format;
            if (Formatters.TryGetValue(v.GetType(), out format))
                return format(v);
            return FormatString(v.ToString());
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

        public static string FormatTimestamp(DateTime utcTimestamp)
        {
            var t = utcTimestamp - Origin;
            return (t.Ticks * 100L).ToString(CultureInfo.InvariantCulture);
        }
    }
}
