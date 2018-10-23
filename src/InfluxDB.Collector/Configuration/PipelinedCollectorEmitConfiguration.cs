using InfluxDB.LineProtocol.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using InfluxDB.Collector.Pipeline;
using InfluxDB.Collector.Pipeline.Emit;

namespace InfluxDB.Collector.Configuration
{
    class PipelinedCollectorEmitConfiguration : CollectorEmitConfiguration
    {
        readonly CollectorConfiguration _configuration;
        readonly List<Action<IEnumerable<IPointData>>> _emitters = new List<Action<IEnumerable<IPointData>>>();
        private ILineProtocolClient _client;

        public PipelinedCollectorEmitConfiguration(
            CollectorConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            _configuration = configuration;
        }

        public override CollectorConfiguration InfluxDB(Uri serverBaseAddress, string database, string username = null, string password = null)
        {
            if (string.Compare(serverBaseAddress.Scheme, "udp", ignoreCase: true) == 0)
                _client = new LineProtocolUdpClient(serverBaseAddress, database, username, password);
            else
                _client = new LineProtocolClient(serverBaseAddress, database, username, password);
            return _configuration;
        }

        public override CollectorConfiguration Emitter(Action<IEnumerable<IPointData>> emitter)
        {
            if (emitter == null) throw new ArgumentNullException(nameof(emitter));
            _emitters.Add(emitter);
            return _configuration;
        }

        public IPointEmitter CreateEmitter(IPointEmitter parent, out Action dispose)
        {
            if (_client == null && !_emitters.Any())
            {
                dispose = null;
                return parent;
            }

            if (parent != null)
                throw new ArgumentException("Parent may not be specified here");

            var result = new List<IPointEmitter>();

            if (_client != null)
            {
                var emitter = new HttpLineProtocolEmitter(_client);
                dispose = emitter.Dispose;
                result.Add(emitter);
            }
            else
            {
                dispose = null;
            }

            foreach (var emitter in _emitters)
            {
                result.Add(new DelegateEmitter(emitter));
            }

            // perfs-short-circuit: no need for an aggregator if there is only one emitter
            if (result.Count == 1)
            {
                return result[0];
            }

            return new AggregateEmitter(result);
        }
    }
}
