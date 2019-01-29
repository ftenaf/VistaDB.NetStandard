using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class RightFunction : Function
  {
    public RightFunction(SQLParser parser)
      : base(parser, 2, true)
    {
      this.dataType = VistaDBType.NChar;
      this.parameterTypes[0] = VistaDBType.NChar;
      this.parameterTypes[1] = VistaDBType.Int;
    }

    protected override object ExecuteSubProgram()
    {
      string str = (string) ((IValue) this.paramValues[0]).Value;
      int length = (int) ((IValue) this.paramValues[1]).Value;
      if (length > str.Length)
        length = str.Length;
      return (object) str.Substring(str.Length - length, length);
    }

    public override int GetWidth()
    {
      return this[0].GetWidth();
    }
  }
}
