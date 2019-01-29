using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class Log10Function : Function
  {
    public Log10Function(SQLParser parser)
      : base(parser, 1, true)
    {
      this.dataType = VistaDBType.Float;
      this.parameterTypes[0] = VistaDBType.Float;
    }

    protected override object ExecuteSubProgram()
    {
      return (object) Math.Log10((double) ((IValue) this.paramValues[0]).Value);
    }
  }
}
