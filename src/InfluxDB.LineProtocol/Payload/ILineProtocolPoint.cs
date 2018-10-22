using System.IO;

namespace InfluxDB.LineProtocol.Payload
{
    /// <summary>
    /// Defines a single point in the line protocol format.
    /// The only feature this point must implement is a way to write this point data to a textwriter.
    /// </summary>
    public interface ILineProtocolPoint
    {
        /// <summary>
        /// Format this point data and write the result to the specific <paramref name="textWriter"/>.
        /// </summary>
        void Format(TextWriter textWriter);
    }
}
