using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class StuffFunction : Function
  {
    public StuffFunction(SQLParser parser)
      : base(parser, 4, true)
    {
      this.dataType = VistaDBType.NChar;
      this.parameterTypes[0] = VistaDBType.NChar;
      this.parameterTypes[1] = VistaDBType.Int;
      this.parameterTypes[2] = VistaDBType.Int;
      this.parameterTypes[3] = VistaDBType.NChar;
    }

    protected override object ExecuteSubProgram()
    {
      string str1 = (string) ((IValue) this.paramValues[0]).Value;
      int length = (int) ((IValue) this.paramValues[1]).Value - 1;
      int startIndex = (int) ((IValue) this.paramValues[2]).Value;
      string str2 = (string) ((IValue) this.paramValues[3]).Value;
      if (length < 0 || length >= str1.Length || startIndex < 0)
        return (object) null;
      if (length == 0)
      {
        if (startIndex >= str1.Length)
          return (object) str2;
        return (object) (str2 + str1.Substring(startIndex, str1.Length - startIndex));
      }
      if (length + startIndex > str1.Length)
        return (object) (str1.Substring(0, length) + str2);
      return (object) (str1.Substring(0, length) + str2 + str1.Substring(length + startIndex, str1.Length - (length + startIndex)));
    }

    public override int GetWidth()
    {
      return (Utils.IsCharacterDataType(this[0].DataType) ? this[0].GetWidth() : 30) + (Utils.IsCharacterDataType(this[3].DataType) ? this[3].GetWidth() : 30);
    }
  }
}
