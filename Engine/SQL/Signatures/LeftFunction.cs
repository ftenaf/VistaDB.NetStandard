using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class LeftFunction : Function
  {
    public LeftFunction(SQLParser parser)
      : base(parser, 2, true)
    {
      this.dataType = VistaDBType.NChar;
      this.parameterTypes[0] = VistaDBType.NChar;
      this.parameterTypes[1] = VistaDBType.Int;
    }

    protected override object ExecuteSubProgram()
    {
      int length = (int) ((IValue) this.paramValues[1]).Value;
      string str = ((IValue) this.paramValues[0]).Value as string;
      if (length <= str.Length)
        return (object) str.Substring(0, length);
      return (object) str;
    }

    public override int GetWidth()
    {
      return this[0].GetWidth();
    }
  }
}
