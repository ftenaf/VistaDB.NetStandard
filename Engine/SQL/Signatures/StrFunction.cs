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
      if (this.ParamCount < 1 || this.ParamCount > 3)
        throw new VistaDBSQLException(501, "STR", this.lineNo, this.symbolNo);
      this.dataType = VistaDBType.NChar;
      this.parameterTypes[0] = VistaDBType.Decimal;
      if (this.ParamCount <= 1)
        return;
      this.parameterTypes[1] = VistaDBType.Int;
      if (this.ParamCount <= 2)
        return;
      this.parameterTypes[2] = VistaDBType.Int;
    }

    protected override object ExecuteSubProgram()
    {
      string empty = string.Empty;
      Decimal num1 = (Decimal) ((IValue) this.paramValues[0]).Value;
      int num2;
      if (this.ParamCount > 1)
      {
        num2 = (int) ((IValue) this.paramValues[1]).Value;
        if (num2 <= 0)
          return (object) null;
      }
      else
        num2 = 10;
      for (int index = 0; index < num2 - 1; ++index)
        empty += "#";
      string format = empty + (object) '0';
      if (this.ParamCount > 2)
      {
        int num3 = (int) ((IValue) this.paramValues[2]).Value;
        if (num3 > 16)
          num3 = 16;
        else if (num3 < 0)
          return (object) null;
        format += ".";
        for (int index = 0; index < num3; ++index)
          format += "0";
      }
      string str = num1.ToString(format);
      if (str.Length > num2)
      {
        int num3 = str.IndexOf(CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);
        if (num3 > num2)
          return (object) null;
        if (num3 == num2 - 1)
          return (object) str.Substring(0, num2 - 1);
        return (object) str.Substring(0, num2);
      }
      if (str.Length < num2)
        return (object) str.PadLeft(num2, ' ');
      return (object) str;
    }

    public override int GetWidth()
    {
      return 30;
    }
  }
}
