using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class UpperFunction : Function
  {
    public UpperFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      this.dataType = VistaDBType.NChar;
      this.parameterTypes[0] = VistaDBType.NChar;
    }

    protected override object ExecuteSubProgram()
    {
      return (object) this.parent.Connection.StringUpper((string) ((IValue) this.paramValues[0]).Value);
    }

    public override int GetWidth()
    {
      return this[0].GetWidth();
    }
  }
}
