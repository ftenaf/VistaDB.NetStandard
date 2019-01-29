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
      this.parameterTypes[0] = VistaDBType.DateTime;
      this.dataType = VistaDBType.Int;
      this.dateFormatInfo = (DateTimeFormatInfo) null;
      this.weekDelta = 0;
    }

    public override SignatureType OnPrepare()
    {
      this.dateFormatInfo = this.parent.Database.Culture.DateTimeFormat;
      this.weekDelta = (int) (7 - this.dateFormatInfo.FirstDayOfWeek + 1);
      return base.OnPrepare();
    }

    protected override object ExecuteSubProgram()
    {
      DateTime dateTime = (DateTime) ((IValue) this.paramValues[0]).Value;
      switch (this.datePart)
      {
        case DatePart.Year:
          return (object) dateTime.Year;
        case DatePart.Quarter:
          return (object) ((dateTime.Month - 1) / 3 + 1);
        case DatePart.Month:
          return (object) dateTime.Month;
        case DatePart.DayOfYear:
          return (object) dateTime.DayOfYear;
        case DatePart.Day:
          return (object) dateTime.Day;
        case DatePart.Week:
          return (object) ((dateTime.DayOfYear - 1) / 7 + 1);
        case DatePart.WeekDay:
          if (dateTime.DayOfWeek < this.dateFormatInfo.FirstDayOfWeek)
            return (object) (this.weekDelta + dateTime.DayOfWeek);
          return (object) (dateTime.DayOfWeek - this.dateFormatInfo.FirstDayOfWeek + 1);
        case DatePart.Hour:
          return (object) dateTime.Hour;
        case DatePart.Minute:
          return (object) dateTime.Minute;
        case DatePart.Second:
          return (object) dateTime.Second;
        case DatePart.Millisecond:
          return (object) dateTime.Millisecond;
        default:
          return (object) null;
      }
    }
  }
}
