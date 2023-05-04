using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ReplaceFunction : Function
  {
    public ReplaceFunction(SQLParser parser)
      : base(parser, 3, true)
    {
      dataType = VistaDBType.NChar;
      parameterTypes[0] = VistaDBType.NChar;
      parameterTypes[1] = VistaDBType.NChar;
      parameterTypes[2] = VistaDBType.NChar;
    }

    protected override object ExecuteSubProgram()
    {
      string str = (string) ((IValue) paramValues[0]).Value;
      string oldValue = (string) ((IValue) paramValues[1]).Value;
      string newValue = (string) ((IValue) paramValues[2]).Value;
      if (oldValue.Length > str.Length)
        return (object) null;
      return (object) str.Replace(oldValue, newValue);
    }

    public override int GetWidth()
    {
      return (Utils.IsCharacterDataType(this[0].DataType) ? this[0].GetWidth() : 30) + (Utils.IsCharacterDataType(this[2].DataType) ? this[2].GetWidth() : 30);
    }
  }
}
