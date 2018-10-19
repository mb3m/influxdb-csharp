using BenchmarkDotNet.Attributes;
using InfluxDB.LineProtocol.Payload;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Benchmark
{
    /// <summary>
    /// This benchmark examines the payload construction and allocations.
    /// </summary>
    [MemoryDiagnoser]
    public class PayloadBenchmark
    {
        private const int N = 500;

        private static readonly string[] Colours = { "red", "blue", "green" };

        private readonly (DateTime timestamp, string colour, double value)[] data;

        public PayloadBenchmark()
        {
            var random = new Random(755);
            var now = DateTime.UtcNow;
            data = Enumerable.Range(0, N).Select(i => (now.AddMilliseconds(random.Next(2000)), Colours[random.Next(Colours.Length)], random.NextDouble())).ToArray();
        }

        [Benchmark(Baseline = true)]
        public object DictionaryPayload()
        {
            var payload = new LineProtocolPayload();

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

            return payload;
        }

        [Benchmark]
        public object ArrayPayload()
        {
            var payload = new LineProtocolPayload();

            foreach (var (timestamp, colour, value) in data)
            {
                payload.Add(new LineProtocolPoint(
                    "example",
                    new[]
                    {
                        new KeyValuePair<string, object>("value", value)
                    },
                    new[]
                    {
                        new KeyValuePair<string, string>("colour", colour)
                    },
                    timestamp
                ));
            }

            return payload;
        }
    }
}
