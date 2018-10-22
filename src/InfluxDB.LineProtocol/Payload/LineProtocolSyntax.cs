using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Reflection;

namespace InfluxDB.LineProtocol.Payload
{
    internal class LineProtocolSyntax
    {
        static readonly DateTime Origin = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static long AsTimestamp(DateTime utcTimestamp)
        {
            return (utcTimestamp - Origin).Ticks * 100L;
        }

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

        public static string EscapeName(string nameOrKey)
        {
            if (nameOrKey == null) throw new ArgumentNullException(nameof(nameOrKey));
            return nameOrKey
                .Replace("=", "\\=")
                .Replace(" ", "\\ ")
                .Replace(",", "\\,");
        }

        /// <summary>
        /// Legacy. This method should not be used anymore, replaced by <see cref="WriteObject"/>.
        /// </summary>
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

        static string FormatString(string s)
        {
            return "\"" + s.Replace("\"", "\\\"") + "\"";
        }

        internal static readonly Dictionary<Type, Delegate> CustomWriters = new Dictionary<Type, Delegate>();

        public static Action<TextWriter, T> GetWriter<T>()
        {
            var t = typeof(T);

            // First we try to obtain an already defined method using the most common types
            switch (Type.GetTypeCode(t))
            {
                case TypeCode.Boolean: return (Action<TextWriter, T>)(object)(Action<TextWriter, bool>)WriteBoolean;
                case TypeCode.Byte: return (Action<TextWriter, T>)(object)(Action<TextWriter, byte>)WriteByte;
                case TypeCode.Decimal: return (Action<TextWriter, T>)(object)(Action<TextWriter, decimal>)WriteDecimal;
                case TypeCode.Double: return (Action<TextWriter, T>)(object)(Action<TextWriter, double>)WriteDouble;
                case TypeCode.Int16: return (Action<TextWriter, T>)(object)(Action<TextWriter, short>)WriteInt16;
                case TypeCode.Int32: return (Action<TextWriter, T>)(object)(Action<TextWriter, int>)WriteInt32;
                case TypeCode.Int64: return (Action<TextWriter, T>)(object)(Action<TextWriter, long>)WriteInt64;
                case TypeCode.SByte: return (Action<TextWriter, T>)(object)(Action<TextWriter, sbyte>)WriteSByte;
                case TypeCode.Single: return (Action<TextWriter, T>)(object)(Action<TextWriter, float>)WriteSingle;
                case TypeCode.String: return (Action<TextWriter, T>)(object)(Action<TextWriter, string>)WriteString;
                case TypeCode.UInt16: return (Action<TextWriter, T>)(object)(Action<TextWriter, ushort>)WriteUInt16;
                case TypeCode.UInt32: return (Action<TextWriter, T>)(object)(Action<TextWriter, uint>)WriteUInt32;
                case TypeCode.UInt64: return (Action<TextWriter, T>)(object)(Action<TextWriter, ulong>)WriteUInt64;
            }

            // If the type does not corresponds to a well-known one, we generate a lambda function which will call the WriteString method
            if (!CustomWriters.TryGetValue(t, out var action))
            {
                // generate WriteString method call
                if (t.GetTypeInfo().IsValueType)
                {
                    // value type : can't be null
                    // compiled expression : Action<TextWriter, T> a = (p1, p2) => WriteString(p1, p2.ToString());          
                    action = (Action<TextWriter, T>)(Delegate)new Action<TextWriter, T>((w, t2) => WriteString(w, t2.ToString()));
                    //var param1 = Expression.Parameter(typeof(TextWriter));
                    //var param2 = Expression.Parameter(t);
                    //var toString = Expression.Call(param2, typeof(object).GetTypeInfo().GetMethod(nameof(ToString)));
                    //var call = Expression.Call(typeof(LineProtocolSyntax).GetTypeInfo().GetMethod(nameof(WriteString)), param1, toString);
                    //action = Expression.Lambda<Action<TextWriter, T>>(call, param1, param2).Compile();
                }
                else
                {
                    // class : can be null
                    // compiled expression : Action<TextWriter, T> a = (p1, p2) => WriteString(p1, p2 != null ? p2.ToString() : null);
                    action = (Action<TextWriter, T>)(Delegate)new Action<TextWriter, T>((w, t2) => WriteString(w, t2?.ToString()));
                    //var param1 = Expression.Parameter(typeof(TextWriter));
                    //var param2 = Expression.Parameter(t);

                    //var condition = Expression.Condition(
                    //    Expression.NotEqual(param2, Expression.Constant(null)),
                    //    Expression.Call(param2, typeof(object).GetTypeInfo().GetMethod(nameof(ToString))),
                    //    Expression.Constant(null)
                    //    );

                    //var call = Expression.Call(typeof(LineProtocolSyntax).GetTypeInfo().GetMethod(nameof(WriteString)), param1, condition);
                    //action = Expression.Lambda<Action<TextWriter, T>>(call, param1, param2).Compile();
                }

                // save it for reuse (note we have a potentially always increasing dictionary here)
                CustomWriters[t] = action;
            }

            return (Action<TextWriter, T>)action;
        }

        public static void WriteObject(TextWriter writer, object value)
        {
            if (value == null)
            {
                WriteString(writer, null);
            }

            switch (Type.GetTypeCode(value.GetType()))
            {
                case TypeCode.Boolean:
                    WriteBoolean(writer, (bool)value);
                    break;

                case TypeCode.Byte:
                    WriteByte(writer, (byte)value);
                    break;

                case TypeCode.Decimal:
                    WriteDecimal(writer, (decimal)value);
                    break;

                case TypeCode.Double:
                    WriteDouble(writer, (double)value);
                    break;

                case TypeCode.Int16:
                    WriteInt16(writer, (short)value);
                    break;

                case TypeCode.Int32:
                    WriteInt32(writer, (int)value);
                    break;

                case TypeCode.Int64:
                    WriteInt64(writer, (long)value);
                    break;

                case TypeCode.SByte:
                    WriteSByte(writer, (sbyte)value);
                    break;

                case TypeCode.Single:
                    WriteSingle(writer, (float)value);
                    break;

                case TypeCode.String:
                    WriteString(writer, (string)value);
                    break;

                case TypeCode.UInt16:
                    WriteUInt16(writer, (ushort)value);
                    break;

                case TypeCode.UInt32:
                    WriteUInt32(writer, (uint)value);
                    break;

                case TypeCode.UInt64:
                    WriteUInt64(writer, (ulong)value);
                    break;

                default:
                    WriteObjectAsString(writer, value.ToString());
                    break;
            }

        }

        public static void WriteObjectAsString(TextWriter writer, object value)
        {
            WriteString(writer, value?.ToString());
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

        public static void WriteString(TextWriter writer, string value)
        {
            writer.Write('"');
            if (value != null)
            {
                writer.Write(value.Replace("\"", "\\\""));
            }
            writer.Write('"');
        }
    }
}
