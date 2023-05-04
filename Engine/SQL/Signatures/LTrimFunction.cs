using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class LTrimFunction : Function
  {
    public LTrimFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      dataType = VistaDBType.NChar;
      parameterTypes[0] = VistaDBType.NChar;
    }

    protected override object ExecuteSubProgram()
    {
      return ((string)paramValues[0].Value).TrimStart();
    }

    public override int GetWidth()
    {
      return this[0].GetWidth();
    }
  }
}
