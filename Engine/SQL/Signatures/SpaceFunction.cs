using System.Text;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class SpaceFunction : Function
  {
    public SpaceFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      this.dataType = VistaDBType.NVarChar;
      this.parameterTypes[0] = VistaDBType.Int;
    }

    protected override object ExecuteSubProgram()
    {
      int capacity = (int) ((IValue) this.paramValues[0]).Value;
      StringBuilder stringBuilder = new StringBuilder(capacity);
      stringBuilder.Length = capacity;
      for (int index = 0; index < capacity; ++index)
        stringBuilder[index] = ' ';
      return (object) stringBuilder.ToString();
    }

    public override int GetWidth()
    {
      return 8192;
    }
  }
}
