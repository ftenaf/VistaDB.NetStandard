using System;
using System.Globalization;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class DatePartFunction : BaseDateFunction
  {
    private DateTimeFormatInfo dateFormatInfo;
    private int weekDelta;

    public DatePartFunction(SQLParser parser)
      : base(parser, 1)
    {
      parameterTypes[0] = VistaDBType.DateTime;
      dataType = VistaDBType.Int;
      dateFormatInfo = null;
      weekDelta = 0;
    }

    public override SignatureType OnPrepare()
    {
      dateFormatInfo = parent.Database.Culture.DateTimeFormat;
      weekDelta = (int) (7 - dateFormatInfo.FirstDayOfWeek + 1);
      return base.OnPrepare();
    }

    protected override object ExecuteSubProgram()
    {
      DateTime dateTime = (DateTime)paramValues[0].Value;
      switch (datePart)
      {
        case DatePart.Year:
          return dateTime.Year;
        case DatePart.Quarter:
          return (dateTime.Month - 1) / 3 + 1;
        case DatePart.Month:
          return dateTime.Month;
        case DatePart.DayOfYear:
          return dateTime.DayOfYear;
        case DatePart.Day:
          return dateTime.Day;
        case DatePart.Week:
          return (dateTime.DayOfYear - 1) / 7 + 1;
        case DatePart.WeekDay:
          if (dateTime.DayOfWeek < dateFormatInfo.FirstDayOfWeek)
            return weekDelta + dateTime.DayOfWeek;
          return dateTime.DayOfWeek - dateFormatInfo.FirstDayOfWeek + 1;
        case DatePart.Hour:
          return dateTime.Hour;
        case DatePart.Minute:
          return dateTime.Minute;
        case DatePart.Second:
          return dateTime.Second;
        case DatePart.Millisecond:
          return dateTime.Millisecond;
        default:
          return null;
      }
    }
  }
}
