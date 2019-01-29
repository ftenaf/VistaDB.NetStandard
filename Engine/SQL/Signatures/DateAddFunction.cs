using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class DateAddFunction : BaseDateFunction
  {
    public DateAddFunction(SQLParser parser)
      : base(parser, 2)
    {
      this.parameterTypes[0] = VistaDBType.Int;
      this.parameterTypes[1] = VistaDBType.DateTime;
      this.dataType = VistaDBType.DateTime;
    }

    protected override object ExecuteSubProgram()
    {
      int months = (int) ((IValue) this.paramValues[0]).Value;
      DateTime dateTime = (DateTime) ((IValue) this.paramValues[1]).Value;
      switch (this.datePart)
      {
        case DatePart.Year:
          return (object) dateTime.AddYears(months);
        case DatePart.Quarter:
          return (object) dateTime.AddMonths(3 * months);
        case DatePart.Month:
          return (object) dateTime.AddMonths(months);
        case DatePart.DayOfYear:
        case DatePart.Day:
          return (object) dateTime.AddDays((double) months);
        case DatePart.Week:
          return (object) dateTime.AddDays((double) (7 * months));
        case DatePart.WeekDay:
          return (object) dateTime.AddDays((double) months);
        case DatePart.Hour:
          return (object) dateTime.AddHours((double) months);
        case DatePart.Minute:
          return (object) dateTime.AddMinutes((double) months);
        case DatePart.Second:
          return (object) dateTime.AddSeconds((double) months);
        case DatePart.Millisecond:
          return (object) dateTime.AddMilliseconds((double) months);
        default:
          return (object) null;
      }
    }
  }
}
