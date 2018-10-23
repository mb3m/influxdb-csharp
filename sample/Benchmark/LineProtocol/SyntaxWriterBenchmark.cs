using BenchmarkDotNet.Attributes;
using InfluxDB.LineProtocol.Payload;
using System;
using System.IO;
using System.Linq;

namespace InfluxDB.LineProtocol
{
    /// <summary>
    /// This benchmark compare the different methods available to serialize field values.
    /// </summary>
    [MemoryDiagnoser]
    public class SyntaxWriterBenchmark
    {
        public const int N = 500;

        private static string[] words = new[] { "red", "green", "blue", "yellow", "black", "white" };

        private readonly Values[] values;

        public SyntaxWriterBenchmark()
        {
            var random = new Random(755);
            var now = DateTime.UtcNow;
            values = Enumerable.Range(0, N)
                .Select(i => new Values { Value1 = random.Next(), Value2 = words[random.Next(words.Length)], Value3 = (.5f * random.Next()), Value4 = 1L * random.Next() * random.Next() })
                .ToArray();
        }

        /// <summary>
        /// This method is only tested as a baseline.
        /// The <see cref="LineProtocolSyntax.FormatValue"/> is not used anymore.
        /// </summary>
        [Benchmark(Baseline = true)]
        public object ObjectFormatter()
        {
            using (var writer = TextWriter.Null)
            {
                foreach (var value in values)
                {
                    writer.Write(LineProtocolSyntaxLegacy.FormatValue(value.Value1));
                    writer.Write(LineProtocolSyntaxLegacy.FormatValue(value.Value2));
                    writer.Write(LineProtocolSyntaxLegacy.FormatValue(value.Value3));
                    writer.Write(LineProtocolSyntaxLegacy.FormatValue(value.Value4));
                }

                return writer.ToString();
            }
        }

        /// <summary>
        /// This methods tests the <see cref="LineProtocolSyntax.WriteObject"/> method,
        /// to compare the boxing loss with the allocation benefit.
        /// </summary>
        [Benchmark]
        public object ObjectWriter()
        {
            using (var writer = TextWriter.Null)
            {
                foreach (var value in values)
                {
                    LineProtocolSyntax.WriteObject(writer, value.Value1);
                    LineProtocolSyntax.WriteObject(writer, value.Value2);
                    LineProtocolSyntax.WriteObject(writer, value.Value3);
                    LineProtocolSyntax.WriteObject(writer, value.Value4);
                }

                return writer.ToString();
            }
        }

        /// <summary>
        /// This methods tests the <see cref="LineProtocolSyntax.GetWriter({T}"/> method,
        /// which makes use of typed functions (no boxing) but allocate a function.
        /// </summary>
        [Benchmark]
        public object GenericWriter()
        {
            var value1Writer = LineProtocolSyntax.GetWriter<int>();
            var value2Writer = LineProtocolSyntax.GetWriter<string>();
            var value3Writer = LineProtocolSyntax.GetWriter<float>();
            var value4Writer = LineProtocolSyntax.GetWriter<long>();

            using (var writer = TextWriter.Null)
            {
                foreach (var value in values)
                {
                    value1Writer(writer, value.Value1);
                    value2Writer(writer, value.Value2);
                    value3Writer(writer, value.Value3);
                    value4Writer(writer, value.Value4);
                }

                return writer.ToString();
            }
        }

        /// <summary>
        /// This methods tests the <see cref="LineProtocolSyntax.GetWriter({T}"/> method,
        /// when the caller does not store the writer function in a reused variable.
        /// This case is expected to be less efficient (both in terms of performance and allocations) than the WriteObject method.
        /// </summary>
        [Benchmark]
        public object GenericWriterEachInstance()
        {
            using (var writer = TextWriter.Null)
            {
                foreach (var value in values)
                {
                    LineProtocolSyntax.GetWriter<int>()(writer, value.Value1);
                    LineProtocolSyntax.GetWriter<string>()(writer, value.Value2);
                    LineProtocolSyntax.GetWriter<float>()(writer, value.Value3);
                }

                return writer.ToString();
            }
        }

        class Values
        {
            public int Value1 { get; set; }

            public string Value2 { get; set; }

            public float Value3 { get; set; }

            public long Value4 { get; set; }
        }
    }
}
