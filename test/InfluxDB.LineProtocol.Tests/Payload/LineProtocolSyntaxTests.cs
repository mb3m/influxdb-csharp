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

            writer(0, stringWriter);

            Assert.Equal("0i", stringWriter.ToString());
        }

        [Fact]
        public void WriteUnknownTypeAsString()
        {
            var writer = LineProtocolSyntax.GetWriter<Guid>();
            var stringWriter = new StringWriter();

            writer(Guid.Empty, stringWriter);

            Assert.Equal($"\"{Guid.Empty}\"", stringWriter.ToString());
            Assert.True(LineProtocolSyntax.Writers.ContainsKey(typeof(Guid)));
        }

        [Fact]
        public void WriteBoolean()
        {
            var writer = LineProtocolSyntax.GetWriter<bool>();
            var stringWriter = new StringWriter();

            writer(true, stringWriter);
            writer(false, stringWriter);

            Assert.Equal("tf", stringWriter.ToString());
        }

        [Fact]
        public void WriteStringWithQuotes()
        {
            var writer = LineProtocolSyntax.GetWriter<string>();
            var stringWriter = new StringWriter();

            writer("Hello \"World\"!", stringWriter);

            // Hello "World"! => "Hello \"World\"!"
            Assert.Equal("\"Hello \\\"World\\\"!\"", stringWriter.ToString());
        }

        [Fact]
        public void WriteTimeSpan()
        {
            var writer = LineProtocolSyntax.GetWriter<TimeSpan>();
            var stringWriter = new StringWriter();

            writer(TimeSpan.FromSeconds(10), stringWriter);
            
            Assert.Equal("10000", stringWriter.ToString());
        }
    }
}
