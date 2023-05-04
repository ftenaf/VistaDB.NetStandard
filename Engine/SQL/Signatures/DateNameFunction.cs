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
      dateFormatInfo = null;
    }

    public override SignatureType OnPrepare()
    {
      dateFormatInfo = parent.Database.Culture.DateTimeFormat;
      return base.OnPrepare();
    }

    protected override object ExecuteSubProgram()
    {
      DateTime dateTime = (DateTime)paramValues[0].Value;
      switch (datePart)
      {
        case DatePart.Year:
          return dateTime.Year.ToString();
        case DatePart.Quarter:
          return ((dateTime.Month - 1) / 3 + 1).ToString();
        case DatePart.Month:
          return dateFormatInfo.MonthNames[dateTime.Month - 1];
        case DatePart.DayOfYear:
          return dateTime.DayOfYear.ToString();
        case DatePart.Day:
          return dateTime.Day.ToString();
        case DatePart.Week:
          return ((dateTime.DayOfYear - 1) / 7 + 1).ToString();
        case DatePart.WeekDay:
          return dateFormatInfo.DayNames[(int)dateTime.DayOfWeek];
        case DatePart.Hour:
          return dateTime.Hour.ToString();
        case DatePart.Minute:
          return dateTime.Minute.ToString();
        case DatePart.Second:
          return dateTime.Second.ToString();
        case DatePart.Millisecond:
          return dateTime.Millisecond.ToString();
        default:
          return null;
      }
    }

    public override int GetWidth()
    {
      return 30;
    }
  }
}
