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
    public class LineProtocolPointAllocBenchmark
    {
        private const int N = 500;

        private static readonly string[] Colours = { "red", "blue", "green" };

        private readonly (DateTime timestamp, string colour, double value, int value2)[] data;

        public LineProtocolPointAllocBenchmark()
        {
            var random = new Random(755);
            var now = DateTime.UtcNow;
            data = Enumerable.Range(0, N).Select(i => (now.AddMilliseconds(random.Next(2000)), Colours[random.Next(Colours.Length)], random.NextDouble(), random.Next())).ToArray();
        }

        [Benchmark(Baseline = true)]
        public object DictionaryPayload()
        {
            var payload = new LineProtocolPayload();

            foreach (var (timestamp, colour, value, value2) in data)
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

            foreach (var (timestamp, colour, value, value2) in data)
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

        [Benchmark]
        public object Generic1Payload()
        {
            var payload = new LineProtocolPayload();

            foreach (var (timestamp, colour, value, value2) in data)
            {
                payload.Add(new LineProtocolPoint<double>(
                    "example",
                    "value",
                    value,
                    new[]
                    {
                        new KeyValuePair<string, string>("colour", colour)
                    },
                    timestamp
                ));
            }

            return payload;
        }

        [Benchmark]
        public object Generic2Payload()
        {
            var payload = new LineProtocolPayload();

            foreach (var (timestamp, colour, value, value2) in data)
            {
                payload.Add(new LineProtocolPoint<double, int>(
                    "example",
                    "value",
                    value,
                    "value2",
                    value2,
                    new[]
                    {
                        new KeyValuePair<string, string>("colour", colour)
                    },
                    timestamp
                ));
            }

            return payload;
        }

        [Benchmark]
        public object GenericMeasure2Payload()
        {
            var payload = new LineProtocolPayload();
            var measure2 = new LineProtocolMeasure<double, int>("example", "value", "value2", new[] { "colour" });

            foreach (var (timestamp, colour, value, value2) in data)
            {
                payload.Add(measure2.AddPoint(value, value2, new[] { colour }, timestamp));
            }

            return payload;
        }
    }
}
