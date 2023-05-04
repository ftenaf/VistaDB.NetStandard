using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class RightFunction : Function
  {
    public RightFunction(SQLParser parser)
      : base(parser, 2, true)
    {
      dataType = VistaDBType.NChar;
      parameterTypes[0] = VistaDBType.NChar;
      parameterTypes[1] = VistaDBType.Int;
    }

    protected override object ExecuteSubProgram()
    {
      string str = (string)paramValues[0].Value;
      int length = (int)paramValues[1].Value;
      if (length > str.Length)
        length = str.Length;
      return str.Substring(str.Length - length, length);
    }

    public override int GetWidth()
    {
      return this[0].GetWidth();
    }
  }
}
