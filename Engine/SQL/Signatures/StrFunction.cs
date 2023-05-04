using System;
using System.Globalization;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class StrFunction : Function
  {
    public StrFunction(SQLParser parser)
      : base(parser, -1, true)
    {
      if (ParamCount < 1 || ParamCount > 3)
        throw new VistaDBSQLException(501, "STR", lineNo, symbolNo);
      dataType = VistaDBType.NChar;
      parameterTypes[0] = VistaDBType.Decimal;
      if (ParamCount <= 1)
        return;
      parameterTypes[1] = VistaDBType.Int;
      if (ParamCount <= 2)
        return;
      parameterTypes[2] = VistaDBType.Int;
    }

    protected override object ExecuteSubProgram()
    {
      string empty = string.Empty;
      Decimal num1 = (Decimal)paramValues[0].Value;
      int num2;
      if (ParamCount > 1)
      {
        num2 = (int)paramValues[1].Value;
        if (num2 <= 0)
          return null;
      }
      else
        num2 = 10;
      for (int index = 0; index < num2 - 1; ++index)
        empty += "#";
      string format = empty + '0';
      if (ParamCount > 2)
      {
        int num3 = (int)paramValues[2].Value;
        if (num3 > 16)
          num3 = 16;
        else if (num3 < 0)
          return null;
        format += ".";
        for (int index = 0; index < num3; ++index)
          format += "0";
      }
      string str = num1.ToString(format);
      if (str.Length > num2)
      {
        int num3 = str.IndexOf(CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);
        if (num3 > num2)
          return null;
        if (num3 == num2 - 1)
          return str.Substring(0, num2 - 1);
        return str.Substring(0, num2);
      }
      if (str.Length < num2)
        return str.PadLeft(num2, ' ');
      return str;
    }

    public override int GetWidth()
    {
      return 30;
    }
  }
}
