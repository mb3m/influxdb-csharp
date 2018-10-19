namespace Benchmark
{
    using BenchmarkDotNet.Running;
    using System.Linq;

    public class Program
    {
        public static void Main(string[] args)
        {
            switch(args.FirstOrDefault()?.ToLowerInvariant())
            {
                case "payload":
                    BenchmarkRunner.Run<PayloadBenchmark>();
                    break;

                default:
                    BenchmarkRunner.Run<WriteLineProtocol>();
                    break;
            }
        }
    }
}
