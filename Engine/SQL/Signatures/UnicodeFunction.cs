using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class UnicodeFunction : Function
  {
    public UnicodeFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      this.dataType = VistaDBType.Int;
      this.parameterTypes[0] = VistaDBType.NChar;
    }

    protected override object ExecuteSubProgram()
    {
      string str = (string) ((IValue) this.paramValues[0]).Value;
      if (str.Length == 0)
        return (object) null;
      return (object) (int) str[0];
    }
  }
}
