using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class CharFunction : Function
  {
    public CharFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      dataType = VistaDBType.Char;
      parameterTypes[0] = VistaDBType.Int;
    }

    protected override object ExecuteSubProgram()
    {
      int num = (int)paramValues[0].Value;
      if (num < 0 || num > byte.MaxValue)
        return null;
      return ((char)num).ToString();
    }

    public override int GetWidth()
    {
      return 1;
    }
  }
}
