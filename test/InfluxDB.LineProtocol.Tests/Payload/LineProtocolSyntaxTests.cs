using InfluxDB.LineProtocol.Payload;
using System;
using System.IO;
using Xunit;

namespace InfluxDB.LineProtocol.Tests
{
    public class LineProtocolSyntaxTests
    {
        [Fact]
        public void WriteInt32()
        {
            var writer = LineProtocolSyntax.GetWriter<int>();
            var stringWriter = new StringWriter();

            writer(stringWriter, 0);

            Assert.Equal("0i", stringWriter.ToString());
        }

        [Fact]
        public void WriteUnknownTypeAsString()
        {
            var writer = LineProtocolSyntax.GetWriter<Guid>();
            var stringWriter = new StringWriter();

            writer(stringWriter, Guid.Empty);

            Assert.Equal($"\"{Guid.Empty}\"", stringWriter.ToString());
            Assert.True(LineProtocolSyntax.CustomWriters.ContainsKey(typeof(Guid)));
        }

        [Fact]
        public void WriteBoolean()
        {
            var writer = LineProtocolSyntax.GetWriter<bool>();
            var stringWriter = new StringWriter();

            writer(stringWriter, true);
            writer(stringWriter, false);

            Assert.Equal("tf", stringWriter.ToString());
        }

        [Fact]
        public void WriteStringWithQuotes()
        {
            var writer = LineProtocolSyntax.GetWriter<string>();
            var stringWriter = new StringWriter();

            writer(stringWriter, "Hello \"World\"!");

            // Hello "World"! => "Hello \"World\"!"
            Assert.Equal("\"Hello \\\"World\\\"!\"", stringWriter.ToString());
        }
    }
}
