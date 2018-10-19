﻿using System;
using System.Collections.Generic;
using System.IO;

namespace InfluxDB.LineProtocol.Payload
{
    public class LineProtocolPayload
    {
        readonly List<ILineProtocolPoint> _points = new List<ILineProtocolPoint>();

        public void Add(ILineProtocolPoint point)
        {
            if (point == null) throw new ArgumentNullException(nameof(point));
            _points.Add(point);
        }

        public void Format(TextWriter textWriter)
        {
            if (textWriter == null) throw new ArgumentNullException(nameof(textWriter));

            foreach (var point in _points)
            {
                point.Format(textWriter);
                textWriter.Write('\n');
            }
        }
    }
}
