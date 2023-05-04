using System;
using System.Globalization;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class DateNameFunction : BaseDateFunction
  {
    private DateTimeFormatInfo dateFormatInfo;

    public DateNameFunction(SQLParser parser)
      : base(parser, 1)
    {
      parameterTypes[0] = VistaDBType.DateTime;
      dataType = VistaDBType.NChar;
      dateFormatInfo = (DateTimeFormatInfo) null;
    }

    public override SignatureType OnPrepare()
    {
      dateFormatInfo = parent.Database.Culture.DateTimeFormat;
      return base.OnPrepare();
    }

    protected override object ExecuteSubProgram()
    {
      DateTime dateTime = (DateTime) ((IValue) paramValues[0]).Value;
      switch (datePart)
      {
        case DatePart.Year:
          return (object) dateTime.Year.ToString();
        case DatePart.Quarter:
          return (object) ((dateTime.Month - 1) / 3 + 1).ToString();
        case DatePart.Month:
          return (object) dateFormatInfo.MonthNames[dateTime.Month - 1];
        case DatePart.DayOfYear:
          return (object) dateTime.DayOfYear.ToString();
        case DatePart.Day:
          return (object) dateTime.Day.ToString();
        case DatePart.Week:
          return (object) ((dateTime.DayOfYear - 1) / 7 + 1).ToString();
        case DatePart.WeekDay:
          return (object) dateFormatInfo.DayNames[(int) dateTime.DayOfWeek];
        case DatePart.Hour:
          return (object) dateTime.Hour.ToString();
        case DatePart.Minute:
          return (object) dateTime.Minute.ToString();
        case DatePart.Second:
          return (object) dateTime.Second.ToString();
        case DatePart.Millisecond:
          return (object) dateTime.Millisecond.ToString();
        default:
          return (object) null;
      }
    }

    public override int GetWidth()
    {
      return 30;
    }
  }
}
