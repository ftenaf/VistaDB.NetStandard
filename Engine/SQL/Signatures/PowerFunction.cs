using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class PowerFunction : Function
  {
    public PowerFunction(SQLParser parser)
      : base(parser, 2, true)
    {
      dataType = VistaDBType.Float;
      parameterTypes[0] = VistaDBType.Float;
      parameterTypes[1] = VistaDBType.Float;
    }

    protected override object ExecuteSubProgram()
    {
      return (object) Math.Pow((double) ((IValue) paramValues[0]).Value, (double) ((IValue) paramValues[1]).Value);
    }
  }
}
