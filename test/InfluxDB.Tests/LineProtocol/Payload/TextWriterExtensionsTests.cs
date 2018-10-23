using System.Collections.Generic;
using System.IO;
using Xunit;

namespace InfluxDB.LineProtocol.Payload
{
    public class TextWriterExtensionsTests
    {
        [Fact]
        public void WriteLPNameEscaped()
        {
            var writer = new StringWriter();

            writer.WriteLPNameEscaped("with space");
            writer.Write(' ');
            writer.WriteLPNameEscaped("with,comma");
            writer.Write(' ');
            writer.WriteLPNameEscaped("with=equal");

            Assert.Equal("with\\ space with\\,comma with\\=equal", writer.ToString());
        }

        [Fact]
        public void WriteLPTags_IEnumerable_With_Values()
        {
            var writer = new StringWriter();
            var tags = new[] {
                new KeyValuePair<string, string>("colour", "red"),
                new KeyValuePair<string, string>("faces", "10"),
            };

            writer.WriteLPTags(tags);

            Assert.Equal(",colour=red,faces=10", writer.ToString());
        }

        [Fact]
        public void WriteLPTags_IEnumerable_Accept_Nulls()
        {
            var writer = new StringWriter();

            writer.WriteLPTags(null);

            Assert.Equal(string.Empty, writer.ToString());
        }

        [Fact]
        public void WriteLPTags_IEnumerable_Sort_Names()
        {
            var writer = new StringWriter();
            var tags = new[] {
                new KeyValuePair<string, string>("faces", "10"),
                new KeyValuePair<string, string>("colour", "red"),
            };

            writer.WriteLPTags(tags);

            Assert.Equal(",colour=red,faces=10", writer.ToString());
        }

        [Fact]
        public void WriteLPTags_IEnumerable_Ignore_Empty()
        {
            var writer = new StringWriter();
            var tags = new[] {
                new KeyValuePair<string, string>("faces", "10"),
                new KeyValuePair<string, string>("colour", ""),
            };

            writer.WriteLPTags(tags);

            Assert.Equal(",faces=10", writer.ToString());
        }

        [Fact]
        public void WriteLPTags_IEnumerable_Ignore_Null()
        {
            var writer = new StringWriter();
            var tags = new[] {
                new KeyValuePair<string, string>("faces", null),
                new KeyValuePair<string, string>("colour", "yellow"),
            };

            writer.WriteLPTags(tags);

            Assert.Equal(",colour=yellow", writer.ToString());
        }

        [Fact]
        public void WriteLPTags_IEnumerable_Escape_Names()
        {
            var writer = new StringWriter();
            var tags = new[] {
                new KeyValuePair<string, string>("face count", "10"),
                new KeyValuePair<string, string>("colour", "yellow"),
            };

            writer.WriteLPTags(tags);

            Assert.Equal(",colour=yellow,face\\ count=10", writer.ToString());
        }
    }
}
