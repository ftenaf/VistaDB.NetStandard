using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class LastTimestampFunction : Function
  {
    public LastTimestampFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      dataType = VistaDBType.Timestamp;
      parameterTypes[0] = VistaDBType.NChar;
    }

    protected override object ExecuteSubProgram()
    {
      return parent.Database.GetLastTimestamp((string) ((IValue) paramValues[0]).Value)?.Value;
    }

    protected override bool InternalGetIsChanged()
    {
      return true;
    }
  }
}
