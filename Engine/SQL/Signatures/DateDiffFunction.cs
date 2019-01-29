using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class DateDiffFunction : BaseDateFunction
  {
    public DateDiffFunction(SQLParser parser)
      : base(parser, 2)
    {
      this.parameterTypes[0] = VistaDBType.DateTime;
      this.parameterTypes[1] = VistaDBType.DateTime;
      this.dataType = VistaDBType.Int;
    }

    protected override object ExecuteSubProgram()
    {
      DateTime dateTime1 = (DateTime) ((IValue) this.paramValues[0]).Value;
      DateTime dateTime2 = (DateTime) ((IValue) this.paramValues[1]).Value;
      switch (this.datePart)
      {
        case DatePart.Year:
          return (object) (dateTime2.Year - dateTime1.Year);
        case DatePart.Quarter:
          return (object) ((dateTime2.Month - 1) / 3 - (dateTime1.Month - 1) / 3 + (dateTime2.Year - dateTime1.Year) * 4);
        case DatePart.Month:
          return (object) (dateTime2.Month - dateTime1.Month + 12 * (dateTime2.Year - dateTime1.Year));
        case DatePart.DayOfYear:
        case DatePart.Day:
          return (object) dateTime2.Date.Subtract(dateTime1.Date).Days;
        case DatePart.Week:
          return (object) (dateTime2.Subtract(dateTime1).Days / 7);
        case DatePart.WeekDay:
          return (object) dateTime2.Subtract(dateTime1).Days;
        case DatePart.Hour:
          return (object) (int) dateTime2.Subtract(dateTime1).TotalHours;
        case DatePart.Minute:
          return (object) (int) dateTime2.Subtract(dateTime1).TotalMinutes;
        case DatePart.Second:
          return (object) (int) dateTime2.Subtract(dateTime1).TotalSeconds;
        case DatePart.Millisecond:
          return (object) (int) dateTime2.Subtract(dateTime1).TotalMilliseconds;
        default:
          return (object) null;
      }
    }
  }
}
