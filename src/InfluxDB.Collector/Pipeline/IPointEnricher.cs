namespace InfluxDB.Collector.Pipeline
{
    interface IPointEnricher
    {
        void Enrich(IMeasurement measure);
    }
}
