using System;
using System.Collections.Generic;
using InfluxDB.Collector.Diagnostics;
using InfluxDB.LineProtocol.Client;
using InfluxDB.LineProtocol.Payload;

namespace InfluxDB.Collector.Pipeline.Emit
{
    class HttpLineProtocolEmitter : IDisposable, IPointEmitter
    {
        readonly ILineProtocolClient _client;

        public HttpLineProtocolEmitter(ILineProtocolClient client)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            _client = client;
        }

        public void Dispose()
        {
             // This needs to ensure outstanding operations have completed
        }

        public void Emit(IPointData point)
        {
            var payload = new LineProtocolPayload();
            payload.Add(point.AsLineProtocolPoint());

            var influxResult = _client.WriteAsync(payload).Result;
            if (!influxResult.Success)
                CollectorLog.ReportError(influxResult.ErrorMessage, null);
        }

        public void Emit(IEnumerable<IPointData> points)
        {
            var payload = new LineProtocolPayload();

            foreach (var point in points)
            {
                payload.Add(point.AsLineProtocolPoint());
            }

            var influxResult = _client.WriteAsync(payload).Result;
            if (!influxResult.Success)
                CollectorLog.ReportError(influxResult.ErrorMessage, null);
        }
    }
}
