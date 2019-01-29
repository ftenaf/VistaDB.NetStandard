using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class LogFunction : Function
  {
    public LogFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      this.dataType = VistaDBType.Float;
      this.parameterTypes[0] = VistaDBType.Float;
    }

    protected override object ExecuteSubProgram()
    {
      return (object) Math.Log((double) ((IValue) this.paramValues[0]).Value);
    }
  }
}
