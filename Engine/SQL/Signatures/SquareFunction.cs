using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class SquareFunction : Function
  {
    public SquareFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      this.dataType = VistaDBType.Float;
      this.parameterTypes[0] = VistaDBType.Float;
    }

    protected override object ExecuteSubProgram()
    {
      return (object) Math.Pow((double) ((IValue) this.paramValues[0]).Value, 2.0);
    }
  }
}
