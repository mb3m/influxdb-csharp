﻿using System;
using System.Collections.Generic;
using System.Globalization;

namespace InfluxDB.LineProtocol.Payload
{
    internal static class LineProtocolSyntaxLegacy
    {

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
    }
}
