using BenchmarkDotNet.Attributes;
using InfluxDB.LineProtocol.Payload;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Benchmark
{
    /// <summary>
    /// This benchmark examines the payload construction and allocations.
    /// </summary>
    [MemoryDiagnoser]
    public class LineProtocolPointFormatBenchmark
    {
        private const int N = 500;

        private static readonly string[] Colours = { "red", "blue", "green" };

        private readonly (DateTime timestamp, string colour, double value)[] data;

        private LineProtocolPayload payload;

        public LineProtocolPointFormatBenchmark()
        {
            var random = new Random(755);
            var now = DateTime.UtcNow;
            data = Enumerable.Range(0, N).Select(i => (now.AddMilliseconds(random.Next(2000)), Colours[random.Next(Colours.Length)], random.NextDouble())).ToArray();
        }

        [GlobalSetup(Target = nameof(DictionaryPayload))]
        public void PrepareDictionaryPayload()
        {
            payload = new LineProtocolPayload();
            foreach (var (timestamp, colour, value) in data)
            {
                payload.Add(new LineProtocolPoint(
                    "example",
                    new Dictionary<string, object>
                    {
                        {"value", value}
                    },
                    new Dictionary<string, string>
                    {
                        {"colour", colour}
                    },
                    timestamp
                ));
            }
        }

        [Benchmark(Baseline = true)]
        public object DictionaryPayload()
        {
            var writer = new StringWriter();
            payload.Format(writer);
            return writer.ToString();
        }

        [GlobalSetup(Target = nameof(GenericPayload))]
        public void PrepareGenericPayload()
        {
            payload = new LineProtocolPayload();
            foreach (var (timestamp, colour, value) in data)
            {
                payload.Add(new LineProtocolPoint(
                    "example",
                    new Dictionary<string, object>
                    {
                        {"value", value}
                    },
                    new Dictionary<string, string>
                    {
                        {"colour", colour}
                    },
                    timestamp
                ));
            }
        }

        [Benchmark]
        public object GenericPayload()
        {
            var writer = new StringWriter();
            payload.Format(writer);
            return writer.ToString();
        }
    }
}
