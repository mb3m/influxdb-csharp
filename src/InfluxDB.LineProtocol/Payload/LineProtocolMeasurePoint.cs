using System;
using System.Collections.Generic;
using System.IO;

namespace InfluxDB.LineProtocol.Payload
{
    public class LineProtocolMeasurePoint : ILineProtocolPoint
    {
        private readonly LineProtocolMeasureBase _measure;
        private readonly IReadOnlyCollection<object> _fieldValues;
        private readonly IReadOnlyCollection<string> _tagValues;
        private readonly DateTime? _utcTimestamp;

        public LineProtocolMeasurePoint(
            LineProtocolMeasureBase measure,
            IReadOnlyCollection<object> fieldValues,
            IReadOnlyCollection<string> tagValues = null,
            DateTime? utcTimestamp = null)
        {
            _measure = measure ?? throw new ArgumentNullException(nameof(measure));
            _fieldValues = fieldValues ?? throw new ArgumentNullException(nameof(fieldValues));
            if (_fieldValues.Count == 0)
                throw new ArgumentException("At least one field must be specified", nameof(fieldValues));

            if (_fieldValues.Count != _measure.FieldNames.Length)
                throw new ArgumentException($"The number of field values specified ({_fieldValues.Count}) is different from the number of fields declared in the measure ({_measure.FieldNames.Length})");

            if ((tagValues?.Count ?? 0) != (_measure.TagNames?.Length ?? 0))
                throw new ArgumentException($"The number of tag values specified ({tagValues?.Count ?? 0}) is different from the number of tags declared in the measure ({_measure.TagNames?.Length ?? 0})");

            _tagValues = tagValues;
            _utcTimestamp = utcTimestamp;
        }

        public void Format(TextWriter textWriter)
        {
            if (textWriter == null) throw new ArgumentNullException(nameof(textWriter));
            textWriter.WriteLPNameEscaped(_measure.Measurement);
            textWriter.WriteLPTags(_measure.TagNames, _tagValues);
            textWriter.WriteLPFields(_measure.FieldNames, _fieldValues);
            textWriter.WriteLPTimestamp(_utcTimestamp);
        }
    }

    public class LineProtocolMeasurePoint<T> : ILineProtocolPoint
    {
        private readonly LineProtocolMeasure<T> _measure;
        private readonly T _fieldValue;
        private readonly IReadOnlyCollection<string> _tagValues;
        private readonly DateTime? _utcTimestamp;

        public LineProtocolMeasurePoint(
            LineProtocolMeasure<T> measure,
            T fieldValue,
            IReadOnlyCollection<string> tagValues = null,
            DateTime? utcTimestamp = null)
        {
            _measure = measure ?? throw new ArgumentNullException(nameof(measure));
            if (_measure.FieldNames.Length != 1)
                throw new ArgumentException($"The number of field values specified (1) is different from the number of fields declared in the measure ({_measure.FieldNames.Length})");

            _fieldValue = fieldValue;

            if ((tagValues?.Count ?? 0) != (_measure.TagNames?.Length ?? 0))
                throw new ArgumentException($"The number of tag values specified ({tagValues?.Count ?? 0}) is different from the number of tags declared in the measure ({_measure.TagNames?.Length ?? 0})");

            _tagValues = tagValues;
            _utcTimestamp = utcTimestamp;
        }

        public void Format(TextWriter textWriter)
        {
            if (textWriter == null) throw new ArgumentNullException(nameof(textWriter));

            textWriter.WriteLPNameEscaped(_measure.Measurement);
            textWriter.WriteLPTags(_measure.TagNames, _tagValues);

            textWriter.Write(' ');
            textWriter.Write(_measure.FieldNames[0]);
            textWriter.Write('=');
            _measure.FieldValueWriter(textWriter, _fieldValue);

            textWriter.WriteLPTimestamp(_utcTimestamp);
        }
    }

    public class LineProtocolMeasurePoint<T1, T2> : ILineProtocolPoint
    {
        private readonly LineProtocolMeasure<T1, T2> _measure;
        private readonly T1 _field1Value;
        private readonly T2 _field2Value;
        private readonly IReadOnlyCollection<string> _tagValues;
        private readonly DateTime? _utcTimestamp;

        public LineProtocolMeasurePoint(
            LineProtocolMeasure<T1, T2> measure,
            T1 field1Value,
            T2 field2Value,
            IReadOnlyCollection<string> tagValues = null,
            DateTime? utcTimestamp = null)
        {
            _measure = measure ?? throw new ArgumentNullException(nameof(measure));
            if (_measure.FieldNames.Length != 2)
                throw new ArgumentException($"The number of field values specified (2) is different from the number of fields declared in the measure ({_measure.FieldNames.Length})");

            _field1Value = field1Value;
            _field2Value = field2Value;

            if ((tagValues?.Count ?? 0) != (_measure.TagNames?.Length ?? 0))
                throw new ArgumentException($"The number of tag values specified ({tagValues?.Count ?? 0}) is different from the number of tags declared in the measure ({_measure.TagNames?.Length ?? 0})");

            this._tagValues = tagValues;
            this._utcTimestamp = utcTimestamp;
        }

        public void Format(TextWriter textWriter)
        {
            if (textWriter == null) throw new ArgumentNullException(nameof(textWriter));

            textWriter.WriteLPNameEscaped(_measure.Measurement);
            textWriter.WriteLPTags(_measure.TagNames, _tagValues);

            textWriter.Write(' ');
            textWriter.Write(_measure.FieldNames[0]);
            textWriter.Write('=');
            _measure.Field1ValueWriter(textWriter, _field1Value);

            textWriter.Write(';');
            textWriter.Write(_measure.FieldNames[1]);
            textWriter.Write('=');
            _measure.Field2ValueWriter(textWriter, _field2Value);

            textWriter.WriteLPTimestamp(_utcTimestamp);
        }
    }

    public class LineProtocolMeasurePoint<T1, T2, T3> : ILineProtocolPoint
    {
        private readonly LineProtocolMeasure<T1, T2, T3> _measure;
        private readonly T1 _field1Value;
        private readonly T2 _field2Value;
        private readonly T3 _field3Value;
        private readonly IReadOnlyCollection<string> _tagValues;
        private readonly DateTime? _utcTimestamp;

        public LineProtocolMeasurePoint(
            LineProtocolMeasure<T1, T2, T3> measure,
            T1 field1Value,
            T2 field2Value,
            T3 field3Value,
            IReadOnlyCollection<string> tagValues = null,
            DateTime? utcTimestamp = null)
        {
            _measure = measure ?? throw new ArgumentNullException(nameof(measure));
            if (_measure.FieldNames.Length != 3)
                throw new ArgumentException($"The number of field values specified (3) is different from the number of fields declared in the measure ({_measure.FieldNames.Length})");

            _field1Value = field1Value;
            _field2Value = field2Value;
            _field3Value = field3Value;

            if ((tagValues?.Count ?? 0) != (_measure.TagNames?.Length ?? 0))
                throw new ArgumentException($"The number of tag values specified ({tagValues?.Count ?? 0}) is different from the number of tags declared in the measure ({_measure.TagNames?.Length ?? 0})");

            this._tagValues = tagValues;
            this._utcTimestamp = utcTimestamp;
        }

        public void Format(TextWriter textWriter)
        {
            if (textWriter == null) throw new ArgumentNullException(nameof(textWriter));

            textWriter.WriteLPNameEscaped(_measure.Measurement);
            textWriter.WriteLPTags(_measure.TagNames, _tagValues);

            textWriter.Write(' ');
            textWriter.Write(_measure.FieldNames[0]);
            textWriter.Write('=');
            _measure.Field1ValueWriter(textWriter, _field1Value);

            textWriter.Write(';');
            textWriter.Write(_measure.FieldNames[1]);
            textWriter.Write('=');
            _measure.Field2ValueWriter(textWriter, _field2Value);

            textWriter.Write(';');
            textWriter.Write(_measure.FieldNames[2]);
            textWriter.Write('=');
            _measure.Field3ValueWriter(textWriter, _field3Value);

            textWriter.WriteLPTimestamp(_utcTimestamp);
        }
    }

    public class LineProtocolMeasurePoint<T1, T2, T3, T4> : ILineProtocolPoint
    {
        private readonly LineProtocolMeasure<T1, T2, T3, T4> _measure;
        private readonly T1 _field1Value;
        private readonly T2 _field2Value;
        private readonly T3 _field3Value;
        private readonly T4 _field4Value;
        private readonly IReadOnlyCollection<string> _tagValues;
        private readonly DateTime? _utcTimestamp;

        public LineProtocolMeasurePoint(
            LineProtocolMeasure<T1, T2, T3, T4> measure,
            T1 field1Value,
            T2 field2Value,
            T3 field3Value,
            T4 field4Value,
            IReadOnlyCollection<string> tagValues = null,
            DateTime? utcTimestamp = null)
        {
            _measure = measure ?? throw new ArgumentNullException(nameof(measure));
            if (_measure.FieldNames.Length != 4)
                throw new ArgumentException($"The number of field values specified (4) is different from the number of fields declared in the measure ({_measure.FieldNames.Length})");

            _field1Value = field1Value;
            _field2Value = field2Value;
            _field3Value = field3Value;
            _field4Value = field4Value;

            if ((tagValues?.Count ?? 0) != (_measure.TagNames?.Length ?? 0))
                throw new ArgumentException($"The number of tag values specified ({tagValues?.Count ?? 0}) is different from the number of tags declared in the measure ({_measure.TagNames?.Length ?? 0})");

            this._tagValues = tagValues;
            this._utcTimestamp = utcTimestamp;
        }

        public void Format(TextWriter textWriter)
        {
            if (textWriter == null) throw new ArgumentNullException(nameof(textWriter));

            textWriter.WriteLPNameEscaped(_measure.Measurement);
            textWriter.WriteLPTags(_measure.TagNames, _tagValues);

            textWriter.Write(' ');
            textWriter.Write(_measure.FieldNames[0]);
            textWriter.Write('=');
            _measure.Field1ValueWriter(textWriter, _field1Value);

            textWriter.Write(';');
            textWriter.Write(_measure.FieldNames[1]);
            textWriter.Write('=');
            _measure.Field2ValueWriter(textWriter, _field2Value);

            textWriter.Write(';');
            textWriter.Write(_measure.FieldNames[2]);
            textWriter.Write('=');
            _measure.Field3ValueWriter(textWriter, _field3Value);

            textWriter.Write(';');
            textWriter.Write(_measure.FieldNames[3]);
            textWriter.Write('=');
            _measure.Field4ValueWriter(textWriter, _field4Value);

            textWriter.WriteLPTimestamp(_utcTimestamp);
        }
    }
}
