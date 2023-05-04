using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class NCharFunction : Function
  {
    public NCharFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      dataType = VistaDBType.NChar;
      parameterTypes[0] = VistaDBType.Int;
    }

    protected override object ExecuteSubProgram()
    {
      int num = (int) ((IValue) paramValues[0]).Value;
      if (num < 0 || num > (int) ushort.MaxValue)
        return (object) null;
      return (object) ((char) num).ToString();
    }

    public override int GetWidth()
    {
      return 1;
    }
  }
}
