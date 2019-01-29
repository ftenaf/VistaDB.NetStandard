using System.Text;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ReverseFunction : Function
  {
    public ReverseFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      this.dataType = VistaDBType.NChar;
      this.parameterTypes[0] = VistaDBType.NChar;
    }

    protected override object ExecuteSubProgram()
    {
      string str = (string) ((IValue) this.paramValues[0]).Value;
      int length = str.Length;
      StringBuilder stringBuilder = new StringBuilder(length);
      stringBuilder.Length = length;
      for (int index = 0; index < length; ++index)
        stringBuilder[length - 1 - index] = str[index];
      return (object) stringBuilder.ToString();
    }

    public override int GetWidth()
    {
      return this[0].GetWidth();
    }
  }
}
