using System;
using System.Collections.Generic;
using System.IO;

namespace InfluxDB.LineProtocol.Payload
{
    public class LineProtocolMeasurePointBase : LineProtocolPointBase
    {
        protected static void WriteTags(string[] tagNames, IEnumerable<string> tagValues, TextWriter textWriter)
        {
            var i = 0;
            foreach (var tagValue in tagValues)
            {
                if (!string.IsNullOrEmpty(tagValue))
                {
                    textWriter.Write(',');
                    textWriter.Write(tagNames[i]); // no need to escape tag names, they are already escaped on LineProtocolMeasureBase ctor
                    textWriter.Write('=');
                    textWriter.WriteLPNameEscaped(tagValue);
                }

                i++;
            }
        }
    }

    public class LineProtocolMeasurePoint : LineProtocolMeasurePointBase, ILineProtocolPoint
    {
        private readonly LineProtocolMeasureBase _measure;
        private readonly IReadOnlyList<object> _fieldValues;
        private readonly IReadOnlyList<string> _tagValues;
        private readonly DateTime? _utcTimestamp;

        public LineProtocolMeasurePoint(
            LineProtocolMeasureBase measure,
            IReadOnlyList<object> fieldValues,
            IReadOnlyList<string> tagValues = null,
            DateTime? utcTimestamp = null)
        {
            this._measure = measure ?? throw new ArgumentNullException(nameof(measure));
            this._fieldValues = fieldValues ?? throw new ArgumentNullException(nameof(fieldValues));
            if (_fieldValues.Count == 0)
                throw new ArgumentException("At least one field must be specified");
            if (_fieldValues.Count != _measure.FieldNames.Length)
                throw new ArgumentException($"The number of field values specified ({_fieldValues.Count}) is different from the number of fields declared in the measure ({_measure.FieldNames.Length})");
            if ((_tagValues?.Count ?? 0) != (_measure.TagNames?.Length ?? 0))
                throw new ArgumentException($"The number of tag values specified ({_tagValues?.Count ?? 0}) is different from the number of tags declared in the measure ({_measure.TagNames?.Length ?? 0})");

            this._tagValues = tagValues;
            this._utcTimestamp = utcTimestamp;
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

    public class LineProtocolMeasurePoint<T> : LineProtocolMeasurePointBase, ILineProtocolPoint
    {
        private readonly LineProtocolMeasure<T> _measure;
        private readonly T _fieldValue;
        private readonly IReadOnlyList<string> _tagValues;
        private readonly DateTime? _utcTimestamp;

        public LineProtocolMeasurePoint(
            LineProtocolMeasure<T> measure,
            T fieldValue,
            IReadOnlyList<string> tagValues = null,
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
            _measure.FieldValueWriter(_fieldValue, textWriter);

            textWriter.WriteLPTimestamp(_utcTimestamp);
        }
    }

    public class LineProtocolMeasurePoint<T1, T2> : LineProtocolMeasurePointBase, ILineProtocolPoint
    {
        private readonly LineProtocolMeasure<T1, T2> _measure;
        private readonly T1 _field1Value;
        private readonly T2 _field2Value;
        private readonly IReadOnlyList<string> _tagValues;
        private readonly DateTime? _utcTimestamp;

        public LineProtocolMeasurePoint(
            LineProtocolMeasure<T1, T2> measure,
            T1 field1Value,
            T2 field2Value,
            IReadOnlyList<string> tagValues = null,
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
            _measure.Field1ValueWriter(_field1Value, textWriter);
            textWriter.Write(';');
            textWriter.Write(_measure.FieldNames[1]);
            textWriter.Write('=');
            _measure.Field2ValueWriter(_field2Value, textWriter);

            textWriter.WriteLPTimestamp(_utcTimestamp);
        }
    }

    public class LineProtocolMeasurePoint<T1, T2, T3> : LineProtocolMeasurePointBase, ILineProtocolPoint
    {
        private readonly LineProtocolMeasure<T1, T2, T3> _measure;
        private readonly T1 _field1Value;
        private readonly T2 _field2Value;
        private readonly T3 _field3Value;
        private readonly IReadOnlyList<string> _tagValues;
        private readonly DateTime? _utcTimestamp;

        public LineProtocolMeasurePoint(
            LineProtocolMeasure<T1, T2, T3> measure,
            T1 field1Value,
            T2 field2Value,
            T3 field3Value,
            IReadOnlyList<string> tagValues = null,
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
            _measure.Field1ValueWriter(_field1Value, textWriter);
            textWriter.Write(';');
            textWriter.Write(_measure.FieldNames[1]);
            textWriter.Write('=');
            _measure.Field2ValueWriter(_field2Value, textWriter);
            textWriter.Write(';');
            textWriter.Write(_measure.FieldNames[2]);
            textWriter.Write('=');
            _measure.Field3ValueWriter(_field3Value, textWriter);

            textWriter.WriteLPTimestamp(_utcTimestamp);
        }
    }

    public class LineProtocolMeasurePoint<T1, T2, T3, T4> : LineProtocolMeasurePointBase, ILineProtocolPoint
    {
        private readonly LineProtocolMeasure<T1, T2, T3, T4> _measure;
        private readonly T1 _field1Value;
        private readonly T2 _field2Value;
        private readonly T3 _field3Value;
        private readonly T4 _field4Value;
        private readonly IReadOnlyList<string> _tagValues;
        private readonly DateTime? _utcTimestamp;

        public LineProtocolMeasurePoint(
            LineProtocolMeasure<T1, T2, T3, T4> measure,
            T1 field1Value,
            T2 field2Value,
            T3 field3Value,
            T4 field4Value,
            IReadOnlyList<string> tagValues = null,
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
            _measure.Field1ValueWriter(_field1Value, textWriter);
            textWriter.Write(';');
            textWriter.Write(_measure.FieldNames[1]);
            textWriter.Write('=');
            _measure.Field2ValueWriter(_field2Value, textWriter);
            textWriter.Write(';');
            textWriter.Write(_measure.FieldNames[2]);
            textWriter.Write('=');
            _measure.Field3ValueWriter(_field3Value, textWriter);
            textWriter.Write(';');
            textWriter.Write(_measure.FieldNames[3]);
            textWriter.Write('=');
            _measure.Field4ValueWriter(_field4Value, textWriter);

            textWriter.WriteLPTimestamp(_utcTimestamp);
        }
    }
}
