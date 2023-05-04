using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class UnicodeFunction : Function
  {
    public UnicodeFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      dataType = VistaDBType.Int;
      parameterTypes[0] = VistaDBType.NChar;
    }

    protected override object ExecuteSubProgram()
    {
      string str = (string) ((IValue) paramValues[0]).Value;
      if (str.Length == 0)
        return (object) null;
      return (object) (int) str[0];
    }
  }
}
