using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class DateDiffFunction : BaseDateFunction
  {
    public DateDiffFunction(SQLParser parser)
      : base(parser, 2)
    {
      parameterTypes[0] = VistaDBType.DateTime;
      parameterTypes[1] = VistaDBType.DateTime;
      dataType = VistaDBType.Int;
    }

    protected override object ExecuteSubProgram()
    {
      DateTime dateTime1 = (DateTime)paramValues[0].Value;
      DateTime dateTime2 = (DateTime)paramValues[1].Value;
      switch (datePart)
      {
        case DatePart.Year:
          return dateTime2.Year - dateTime1.Year;
        case DatePart.Quarter:
          return (dateTime2.Month - 1) / 3 - (dateTime1.Month - 1) / 3 + (dateTime2.Year - dateTime1.Year) * 4;
        case DatePart.Month:
          return dateTime2.Month - dateTime1.Month + 12 * (dateTime2.Year - dateTime1.Year);
        case DatePart.DayOfYear:
        case DatePart.Day:
          return dateTime2.Date.Subtract(dateTime1.Date).Days;
        case DatePart.Week:
          return dateTime2.Subtract(dateTime1).Days / 7;
        case DatePart.WeekDay:
          return dateTime2.Subtract(dateTime1).Days;
        case DatePart.Hour:
          return (int)dateTime2.Subtract(dateTime1).TotalHours;
        case DatePart.Minute:
          return (int)dateTime2.Subtract(dateTime1).TotalMinutes;
        case DatePart.Second:
          return (int)dateTime2.Subtract(dateTime1).TotalSeconds;
        case DatePart.Millisecond:
          return (int)dateTime2.Subtract(dateTime1).TotalMilliseconds;
        default:
          return null;
      }
    }
  }
}
