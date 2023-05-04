using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class SubStringFunction : Function
  {
    public SubStringFunction(SQLParser parser)
      : base(parser, 3, true)
    {
      dataType = VistaDBType.NChar;
      parameterTypes[0] = VistaDBType.NChar;
      parameterTypes[1] = VistaDBType.Int;
      parameterTypes[2] = VistaDBType.Int;
    }

    protected override object ExecuteSubProgram()
    {
      string str = (string) ((IValue) paramValues[0]).Value;
      int startIndex = (int) ((IValue) paramValues[1]).Value - 1;
      int length = (int) ((IValue) paramValues[2]).Value;
      if (startIndex < 0 || startIndex >= str.Length || length < 0)
        return (object) null;
      if (startIndex == 0 && length >= str.Length)
        return (object) str;
      if (startIndex + length > str.Length)
        length = str.Length - startIndex;
      return (object) str.Substring(startIndex, length);
    }

    public override int GetWidth()
    {
      return this[0].GetWidth();
    }
  }
}
