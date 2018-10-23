using InfluxDB.LineProtocol.Payload;
using System;
using System.Globalization;
using System.IO;
using Xunit;

namespace InfluxDB.LineProtocol.Payload
{
    public class LineProtocolSyntaxTests
    {
        [Fact]
        public void EscapeName()
        {
            Assert.Equal("my\\ tag\\ name", LineProtocolSyntax.EscapeName("my tag name"));
            Assert.Equal("my\\,tag\\,name", LineProtocolSyntax.EscapeName("my,tag,name"));
            Assert.Equal("my\\=tag\\=name", LineProtocolSyntax.EscapeName("my=tag=name"));
        }

        [Fact]
        public void GetWriter_Boolean()
        {
            var writer = LineProtocolSyntax.GetWriter<bool>();
            var stringWriter = new StringWriter();

            writer(stringWriter, true);
            writer(stringWriter, false);

            Assert.Equal("tf", stringWriter.ToString());
        }

        [Fact]
        public void GetWriter_Byte()
        {
            var writer = LineProtocolSyntax.GetWriter<byte>();
            var stringWriter = new StringWriter();

            writer(stringWriter, 45);
            stringWriter.Write(' ');
            writer(stringWriter, 255);

            Assert.Equal("45i 255i", stringWriter.ToString());
        }

        [Fact]
        public void GetWriter_Decimal()
        {
            var writer = LineProtocolSyntax.GetWriter<decimal>();
            var stringWriter = new StringWriter();

            writer(stringWriter, 45m);
            stringWriter.Write(' ');
            writer(stringWriter, -4194.6m);

            Assert.Equal("45 -4194.6", stringWriter.ToString());
        }

        [Fact]
        public void GetWriter_Int16()
        {
            var writer = LineProtocolSyntax.GetWriter<short>();
            var stringWriter = new StringWriter();

            writer(stringWriter, 18013);
            stringWriter.Write(' ');
            writer(stringWriter, -2487);

            Assert.Equal("18013i -2487i", stringWriter.ToString());
        }

        [Fact]
        public void GetWriter_Int32()
        {
            var writer = LineProtocolSyntax.GetWriter<int>();
            var stringWriter1 = new StringWriter();
            var stringWriter2 = new StringWriter();

            writer(stringWriter1, 48567);
            writer(stringWriter2, -451);

            Assert.Equal("48567i", stringWriter1.ToString());
            Assert.Equal("-451i", stringWriter2.ToString());
        }

        [Fact]
        public void GetWriter_Int64()
        {
            var writer = LineProtocolSyntax.GetWriter<long>();
            var stringWriter = new StringWriter();

            writer(stringWriter, long.MaxValue);
            stringWriter.Write(' ');
            writer(stringWriter, 0);
            stringWriter.Write(' ');
            writer(stringWriter, long.MinValue);

            Assert.Equal(string.Format(CultureInfo.InvariantCulture, "{0}i 0i {1}i", long.MaxValue, long.MinValue), stringWriter.ToString());
        }

        [Fact]
        public void GetWriter_SByte()
        {
            var writer = LineProtocolSyntax.GetWriter<sbyte>();
            var stringWriter = new StringWriter();

            writer(stringWriter, sbyte.MinValue);
            stringWriter.Write(' ');
            writer(stringWriter, 0);
            stringWriter.Write(' ');
            writer(stringWriter, sbyte.MaxValue);

            Assert.Equal(string.Format(CultureInfo.InvariantCulture, "{0}i 0i {1}i", sbyte.MinValue, sbyte.MaxValue), stringWriter.ToString());
        }

        [Fact]
        public void GetWriter_Single()
        {
            var writer = LineProtocolSyntax.GetWriter<float>();
            var stringWriter = new StringWriter();

            writer(stringWriter, float.MinValue);
            stringWriter.Write(' ');
            writer(stringWriter, 0);
            stringWriter.Write(' ');
            writer(stringWriter, float.MaxValue);

            Assert.Equal(string.Format(CultureInfo.InvariantCulture, "{0} 0 {1}", float.MinValue, float.MaxValue), stringWriter.ToString());
        }

        [Fact]
        public void GetWriter_String()
        {
            var writer = LineProtocolSyntax.GetWriter<string>();
            var stringWriter = new StringWriter();

            writer(stringWriter, "Hello World!");

            Assert.Equal("\"Hello World!\"", stringWriter.ToString());
        }

        [Fact]
        public void GetWriter_String_WithQuotes()
        {
            var writer = LineProtocolSyntax.GetWriter<string>();
            var stringWriter = new StringWriter();

            writer(stringWriter, "Hello \"World\"!");

            // Hello "World"! => "Hello \"World\"!"
            Assert.Equal("\"Hello \\\"World\\\"!\"", stringWriter.ToString());
        }

        [Fact]
        public void GetWriter_String_Null_As_Empty()
        {
            var writer = LineProtocolSyntax.GetWriter<string>();
            var stringWriter = new StringWriter();

            writer(stringWriter, null);

            Assert.Equal("\"\"", stringWriter.ToString());
        }

        [Fact]
        public void GetWriter_UInt16()
        {
            var writer = LineProtocolSyntax.GetWriter<ushort>();
            var stringWriter = new StringWriter();

            writer(stringWriter, ushort.MinValue);
            stringWriter.Write(' ');
            writer(stringWriter, 0);
            stringWriter.Write(' ');
            writer(stringWriter, ushort.MaxValue);

            Assert.Equal(string.Format(CultureInfo.InvariantCulture, "{0}i 0i {1}i", ushort.MinValue, ushort.MaxValue), stringWriter.ToString());
        }

        [Fact]
        public void GetWriter_UInt32()
        {
            var writer = LineProtocolSyntax.GetWriter<uint>();
            var stringWriter = new StringWriter();

            writer(stringWriter, uint.MinValue);
            stringWriter.Write(' ');
            writer(stringWriter, 0);
            stringWriter.Write(' ');
            writer(stringWriter, uint.MaxValue);

            Assert.Equal(string.Format(CultureInfo.InvariantCulture, "{0}i 0i {1}i", uint.MinValue, uint.MaxValue), stringWriter.ToString());
        }

        [Fact]
        public void GetWriter_UInt64()
        {
            var writer = LineProtocolSyntax.GetWriter<ulong>();
            var stringWriter = new StringWriter();

            writer(stringWriter, ulong.MinValue);
            stringWriter.Write(' ');
            writer(stringWriter, 0);
            stringWriter.Write(' ');
            writer(stringWriter, ulong.MaxValue);

            Assert.Equal(string.Format(CultureInfo.InvariantCulture, "{0}i 0i {1}i", ulong.MinValue, ulong.MaxValue), stringWriter.ToString());
        }

        [Fact]
        public void GetWriter_CustomValueType_As_String()
        {
            var writer = LineProtocolSyntax.GetWriter<Guid>();
            var stringWriter = new StringWriter();

            writer(stringWriter, Guid.Empty);

            Assert.Equal($"\"{Guid.Empty}\"", stringWriter.ToString());
            Assert.True(LineProtocolSyntax.CustomWriters.ContainsKey(typeof(Guid)));
        }

        [Fact]
        public void GetWriter_CustomClassType_As_String()
        {
            var writer = LineProtocolSyntax.GetWriter<MyCustomType>();
            var stringWriter = new StringWriter();

            writer(stringWriter, new MyCustomType { Value = "Hello" });
            stringWriter.Write(' ');
            writer(stringWriter, null);

            Assert.Equal($"\"MyValue Hello\" \"\"", stringWriter.ToString());
            Assert.True(LineProtocolSyntax.CustomWriters.ContainsKey(typeof(MyCustomType)));
        }

        class MyCustomType
        {
            public string Value { get; set; }
            public override string ToString() => $"MyValue {Value}";
        }
    }
}
