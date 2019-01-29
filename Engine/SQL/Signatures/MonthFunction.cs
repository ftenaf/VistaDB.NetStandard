using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class MonthFunction : Function
  {
    public MonthFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      this.dataType = VistaDBType.Int;
      this.parameterTypes[0] = VistaDBType.DateTime;
    }

    protected override object ExecuteSubProgram()
    {
      return (object) ((DateTime) ((IValue) this.paramValues[0]).Value).Month;
    }
  }
}
