using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class StuffFunction : Function
  {
    public StuffFunction(SQLParser parser)
      : base(parser, 4, true)
    {
      dataType = VistaDBType.NChar;
      parameterTypes[0] = VistaDBType.NChar;
      parameterTypes[1] = VistaDBType.Int;
      parameterTypes[2] = VistaDBType.Int;
      parameterTypes[3] = VistaDBType.NChar;
    }

    protected override object ExecuteSubProgram()
    {
      string str1 = (string)paramValues[0].Value;
      int length = (int)paramValues[1].Value - 1;
      int startIndex = (int)paramValues[2].Value;
      string str2 = (string)paramValues[3].Value;
      if (length < 0 || length >= str1.Length || startIndex < 0)
        return null;
      if (length == 0)
      {
        if (startIndex >= str1.Length)
          return str2;
        return str2 + str1.Substring(startIndex, str1.Length - startIndex);
      }
      if (length + startIndex > str1.Length)
        return str1.Substring(0, length) + str2;
      return str1.Substring(0, length) + str2 + str1.Substring(length + startIndex, str1.Length - (length + startIndex));
    }

    public override int GetWidth()
    {
      return (Utils.IsCharacterDataType(this[0].DataType) ? this[0].GetWidth() : 30) + (Utils.IsCharacterDataType(this[3].DataType) ? this[3].GetWidth() : 30);
    }
  }
}
