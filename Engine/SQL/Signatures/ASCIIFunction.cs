using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ASCIIFunction : Function
  {
    public ASCIIFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      dataType = VistaDBType.TinyInt;
      parameterTypes[0] = VistaDBType.NChar;
    }

    protected override object ExecuteSubProgram()
    {
      string str = (string)paramValues[0].Value;
      if (str.Length == 0 || str[0] > 'ÿ')
        return null;
      return (byte)str[0];
    }
  }
}
