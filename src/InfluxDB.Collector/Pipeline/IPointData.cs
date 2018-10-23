using InfluxDB.LineProtocol.Payload;

namespace InfluxDB.Collector.Pipeline
{
    public interface IPointData
    {
        ILineProtocolPoint AsLineProtocolPoint();
    }
}
