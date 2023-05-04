using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class LenFunction : Function
  {
    public LenFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      dataType = VistaDBType.Int;
      parameterTypes[0] = VistaDBType.NChar;
    }

    protected override object ExecuteSubProgram()
    {
      return ((string)paramValues[0].Value).TrimEnd().Length;
    }
  }
}
