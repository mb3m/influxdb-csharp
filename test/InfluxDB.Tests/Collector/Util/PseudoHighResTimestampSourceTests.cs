﻿using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace InfluxDB.Collector.Util
{
    public class PseudoHighResTimeStampSourceTests
    {
        private readonly ITestOutputHelper output;

        public PseudoHighResTimeStampSourceTests(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public void CanSupplyHighResTimestamps()
        {
            const int iterations = 100000;

            // Even if we call inside a very tight loop, 
            // we should get better (pseudo) resolution than just DateTime.UtcNow
            long dateTimeCollisions = 0;
            DateTime previousDateTime = DateTime.UtcNow;
            for (int i = 0; i < iterations; i++)
            {
                // Attempt with date time directly
                DateTime current = DateTime.UtcNow;

                if (previousDateTime == current)
                {
                    dateTimeCollisions++;
                }
                previousDateTime = current;
            }
            
            if (0 == dateTimeCollisions)
            {
                // Warn because we do expect that datetime.now has collisions in such a tight loop
                output.WriteLine("Warning! Expected DateTime.UtcNow to have some collisions, but seemed to be high resolution already.");
            }

            // Now attempt to use our pseudo high precision provider that includes a sequence number to ensure that it 
            // never collides
            ITimestampSource target = new PseudoHighResTimestampSource();
            long highResTotalCollisions = 0;
            previousDateTime = target.GetUtcNow();
            for (int i = 0; i < iterations; i++)
            {
                // Attempt with date time directly
                DateTime current = target.GetUtcNow();

                if (previousDateTime == current)
                {
                    highResTotalCollisions++;
                }
                previousDateTime = current;
            }

            Assert.Equal(0, highResTotalCollisions);
            output.WriteLine($"No collisions detected with high resolution source, compared to {dateTimeCollisions} for DateTime.UtcNow.");
        }

        [Fact]
        public void CanSupplyHighResTimestampsInParallel()
        {
            const int iterations = 100000;

            // Even if we call inside a very tight loop, we should get better (pseudo) resolution than just DateTime.UtcNow
            int dateTimeCollisions = 0;
            DateTime previousDateTime = DateTime.UtcNow;
            Parallel.For(0, iterations, (i) => // (int i = 0; i < 100000; i++)
            {
                // Attempt with date time directly
                DateTime current = DateTime.UtcNow;

                if (previousDateTime == current)
                {
                    System.Threading.Interlocked.Increment(ref dateTimeCollisions);
                }
                previousDateTime = current;
            });

            if (0 == dateTimeCollisions)
            {
                // Warn because we do expect that datetime.now has collisions in such a tight loop
                output.WriteLine("Warning! Expected DateTime.UtcNow to have some collisions, but seemed to be high resolution already.");
            }

            // Now attempt to use our pseudo high precision provider that includes a sequence number to ensure that it 
            // never collides
            ITimestampSource target = new PseudoHighResTimestampSource();
            int highResTotalCollisions = 0;
            previousDateTime = target.GetUtcNow();
            Parallel.For(0, iterations, (i) => // (int i = 0; i < 100000; i++)
            {
                // Attempt with date time directly
                DateTime current = target.GetUtcNow();

                if (previousDateTime == current)
                {
                    System.Threading.Interlocked.Increment(ref highResTotalCollisions);
                }
                previousDateTime = current;
            });

            Assert.Equal(0, highResTotalCollisions);
            output.WriteLine($"No collisions detected with high resolution source, compared to {dateTimeCollisions} for DateTime.UtcNow.");
        }

        [Fact]
        public void WillGiveUtcDateTimeKind()
        {
            ITimestampSource target = new PseudoHighResTimestampSource();

            DateTime result = target.GetUtcNow();
            Assert.Equal(DateTimeKind.Utc, result.Kind);
        }

        [Fact]
        public void WillNotDriftTooFarFromUtcNow()
        {
            ITimestampSource target = new PseudoHighResTimestampSource();
            const int MAX_DRIFT_MS = 10;

            // Average over 10000 iterations and get the average drift
            decimal totalDrift = 0;
            const int iterations = 10000;
            for (int i = 0; i < iterations; i++)
            {
                DateTime current = DateTime.UtcNow;
                DateTime result = target.GetUtcNow();

                totalDrift += Convert.ToDecimal((result - current).TotalMilliseconds);
            }
            Decimal averageDrift = totalDrift / iterations;

            if (averageDrift > MAX_DRIFT_MS)
            {
                output.WriteLine($"Expected times were more than {MAX_DRIFT_MS}ms apart. Instead they were {averageDrift}ms apart.");
                Assert.True(false); // Force fail.
            }
            else
            {
                output.WriteLine ($"Total Drift over {iterations} iterations: {totalDrift}ms. Average {averageDrift}ms");
            }
        }
    }
}


