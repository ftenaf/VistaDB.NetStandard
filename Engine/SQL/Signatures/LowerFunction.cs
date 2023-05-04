using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class LowerFunction : Function
  {
    public LowerFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      dataType = VistaDBType.NChar;
      parameterTypes[0] = VistaDBType.NChar;
    }

    protected override object ExecuteSubProgram()
    {
      return parent.Connection.StringLower((string)paramValues[0].Value);
    }

    public override int GetWidth()
    {
      return this[0].GetWidth();
    }
  }
}
