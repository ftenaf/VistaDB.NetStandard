using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class CotFunction : Function
  {
    public CotFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      this.dataType = VistaDBType.Float;
      this.parameterTypes[0] = VistaDBType.Float;
    }

    protected override object ExecuteSubProgram()
    {
      return (object) (1.0 / Math.Tan((double) ((IValue) this.paramValues[0]).Value));
    }
  }
}
