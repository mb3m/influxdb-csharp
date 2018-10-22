using BenchmarkDotNet.Attributes;
using InfluxDB.LineProtocol.Payload;
using System.Collections.Generic;

namespace Benchmark
{
    [MemoryDiagnoser]
    public class LineProtocolEscapeNameBenchmark
    {
        private readonly string[] samples = new[]
        {
            "",
            "name",
            "name spaced",
            "name \"quoted\"",
            "name,with,comma"
        };

        [Benchmark(Baseline = true)]
        public object WithReplace()
        {
            var result = new List<string>(samples.Length);

            for (int i = 0; i < samples.Length; i++)
            {
                //result.Add(LineProtocolSyntax.EscapeName(samples[i]));
            }

            return result;
        }

        [Benchmark]
        public object WithCharArray()
        {
            var result = new List<string>(samples.Length);

            for (int i = 0; i < samples.Length; i++)
            {
                //result.Add(LineProtocolSyntax.EscapeName2(samples[i]));
            }

            return result;
        }
    }
}
