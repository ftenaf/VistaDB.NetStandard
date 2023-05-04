using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class DateAddFunction : BaseDateFunction
  {
    public DateAddFunction(SQLParser parser)
      : base(parser, 2)
    {
      parameterTypes[0] = VistaDBType.Int;
      parameterTypes[1] = VistaDBType.DateTime;
      dataType = VistaDBType.DateTime;
    }

    protected override object ExecuteSubProgram()
    {
      int months = (int)paramValues[0].Value;
      DateTime dateTime = (DateTime)paramValues[1].Value;
      switch (datePart)
      {
        case DatePart.Year:
          return dateTime.AddYears(months);
        case DatePart.Quarter:
          return dateTime.AddMonths(3 * months);
        case DatePart.Month:
          return dateTime.AddMonths(months);
        case DatePart.DayOfYear:
        case DatePart.Day:
          return dateTime.AddDays(months);
        case DatePart.Week:
          return dateTime.AddDays(7 * months);
        case DatePart.WeekDay:
          return dateTime.AddDays(months);
        case DatePart.Hour:
          return dateTime.AddHours(months);
        case DatePart.Minute:
          return dateTime.AddMinutes(months);
        case DatePart.Second:
          return dateTime.AddSeconds(months);
        case DatePart.Millisecond:
          return dateTime.AddMilliseconds(months);
        default:
          return null;
      }
    }
  }
}
