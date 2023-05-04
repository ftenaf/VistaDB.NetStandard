using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class LastTableAnchor : Function
  {
    public LastTableAnchor(SQLParser parser)
      : base(parser, 1, true)
    {
      dataType = VistaDBType.Timestamp;
      parameterTypes[0] = VistaDBType.NChar;
    }

    protected override object ExecuteSubProgram()
    {
      return ((IValue) parent.Database.GetTableAnchor((string)paramValues[0].Value))?.Value;
    }
  }
}
