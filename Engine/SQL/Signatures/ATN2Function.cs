using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ATN2Function : Function
  {
    public ATN2Function(SQLParser parser)
      : base(parser, 2, true)
    {
      this.dataType = VistaDBType.Float;
      this.parameterTypes[0] = VistaDBType.Float;
      this.parameterTypes[1] = VistaDBType.Float;
    }

    protected override object ExecuteSubProgram()
    {
      return (object) Math.Atan2((double) ((IValue) this.paramValues[0]).Value, (double) ((IValue) this.paramValues[1]).Value);
    }
  }
}
