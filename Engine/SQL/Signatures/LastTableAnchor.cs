using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class LastTableAnchor : Function
  {
    public LastTableAnchor(SQLParser parser)
      : base(parser, 1, true)
    {
      this.dataType = VistaDBType.Timestamp;
      this.parameterTypes[0] = VistaDBType.NChar;
    }

    protected override object ExecuteSubProgram()
    {
      return ((IValue) this.parent.Database.GetTableAnchor((string) ((IValue) this.paramValues[0]).Value))?.Value;
    }
  }
}
