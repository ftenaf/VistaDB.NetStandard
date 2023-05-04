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
      if (ParamCount < 2 || ParamCount > 3)
        throw new VistaDBSQLException(501, "CHARINDEX", lineNo, symbolNo);
      dataType = VistaDBType.Int;
      parameterTypes[0] = VistaDBType.NChar;
      parameterTypes[1] = VistaDBType.NChar;
      if (ParamCount != 3)
        return;
      parameterTypes[2] = VistaDBType.Int;
    }

    protected override object ExecuteSubProgram()
    {
      string str1 = (string) ((IValue) paramValues[0]).Value;
      if (str1.Length == 0)
        return (object) 0;
      string str2 = (string) ((IValue) paramValues[1]).Value;
      if (str2.Length == 0)
        return (object) 0;
      int num = ParamCount == 3 ? (int) ((IValue) paramValues[2]).Value - 1 : 0;
      if (num >= str2.Length)
        return (object) 0;
      StringComparison comparisonType = parent.Database.CaseSensitive ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase;
      return (object) (str2.IndexOf(str1, num < 0 ? 0 : num, comparisonType) + 1);
    }
  }
}
