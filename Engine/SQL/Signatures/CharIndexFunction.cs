using System;
using VistaDB.Diagnostic;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class CharIndexFunction : Function
  {
    public CharIndexFunction(SQLParser parser)
      : base(parser, -1, true)
    {
      if (this.ParamCount < 2 || this.ParamCount > 3)
        throw new VistaDBSQLException(501, "CHARINDEX", this.lineNo, this.symbolNo);
      this.dataType = VistaDBType.Int;
      this.parameterTypes[0] = VistaDBType.NChar;
      this.parameterTypes[1] = VistaDBType.NChar;
      if (this.ParamCount != 3)
        return;
      this.parameterTypes[2] = VistaDBType.Int;
    }

    protected override object ExecuteSubProgram()
    {
      string str1 = (string) ((IValue) this.paramValues[0]).Value;
      if (str1.Length == 0)
        return (object) 0;
      string str2 = (string) ((IValue) this.paramValues[1]).Value;
      if (str2.Length == 0)
        return (object) 0;
      int num = this.ParamCount == 3 ? (int) ((IValue) this.paramValues[2]).Value - 1 : 0;
      if (num >= str2.Length)
        return (object) 0;
      StringComparison comparisonType = this.parent.Database.CaseSensitive ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase;
      return (object) (str2.IndexOf(str1, num < 0 ? 0 : num, comparisonType) + 1);
    }
  }
}
