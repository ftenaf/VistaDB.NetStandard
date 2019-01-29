using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ATanFunction : Function
  {
    public ATanFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      this.dataType = VistaDBType.Float;
      this.parameterTypes[0] = VistaDBType.Float;
    }

    protected override object ExecuteSubProgram()
    {
      return (object) Math.Atan((double) ((IValue) this.paramValues[0]).Value);
    }
  }
}
