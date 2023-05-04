using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal abstract class BaseDateFunction : Function
  {
    protected DatePart datePart;

    public BaseDateFunction(SQLParser parser, int paramCount)
      : base(parser)
    {
      parser.SkipToken(true);
      if (!parser.IsToken("("))
        throw new VistaDBSQLException(500, "\"(\"", lineNo, symbolNo);
      parser.SkipToken(true);
      if (parser.TokenValue.TokenType != TokenType.Unknown)
        throw new VistaDBSQLException(550, "DATEADD", lineNo, symbolNo);
      datePart = GetDatePart(parser.TokenValue.Token);
      parser.SkipToken(true);
      for (int index = 0; index < paramCount; ++index)
      {
        parser.ExpectedExpression(",");
        parameters.Add(parser.NextSignature(true, true, 6));
      }
      parser.ExpectedExpression(")");
      paramValues = new IColumn[paramCount];
      parameterTypes = new VistaDBType[paramCount];
      signatureType = SignatureType.Expression;
      skipNull = true;
    }

    private DatePart GetDatePart(string name)
    {
      if (string.Compare(name, "YEAR", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "YYYY", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "YY", StringComparison.OrdinalIgnoreCase) == 0)
        return DatePart.Year;
      if (string.Compare(name, "QUARTER", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "QQ", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "Q", StringComparison.OrdinalIgnoreCase) == 0)
        return DatePart.Quarter;
      if (string.Compare(name, "MONTH", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "MM", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "M", StringComparison.OrdinalIgnoreCase) == 0)
        return DatePart.Month;
      if (string.Compare(name, "DAYOFYEAR", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "DY", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "Y", StringComparison.OrdinalIgnoreCase) == 0)
        return DatePart.DayOfYear;
      if (string.Compare(name, "DAY", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "DD", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "D", StringComparison.OrdinalIgnoreCase) == 0)
        return DatePart.Day;
      if (string.Compare(name, "WEEK", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "WK", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "WW", StringComparison.OrdinalIgnoreCase) == 0)
        return DatePart.Week;
      if (string.Compare(name, "WEEKDAY", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "DW", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "W", StringComparison.OrdinalIgnoreCase) == 0)
        return DatePart.WeekDay;
      if (string.Compare(name, "HOUR", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "HH", StringComparison.OrdinalIgnoreCase) == 0)
        return DatePart.Hour;
      if (string.Compare(name, "MINUTE", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "MI", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "N", StringComparison.OrdinalIgnoreCase) == 0)
        return DatePart.Minute;
      if (string.Compare(name, "SECOND", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "SS", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "S", StringComparison.OrdinalIgnoreCase) == 0)
        return DatePart.Second;
      if (string.Compare(name, "MILLISECOND", StringComparison.OrdinalIgnoreCase) == 0 || string.Compare(name, "MS", StringComparison.OrdinalIgnoreCase) == 0)
        return DatePart.Millisecond;
      throw new VistaDBSQLException(550, text, lineNo, symbolNo);
    }

    protected override bool IsEquals(Signature signature)
    {
      if (!base.IsEquals(signature))
        return false;
      return datePart == ((BaseDateFunction) signature).datePart;
    }
  }
}
