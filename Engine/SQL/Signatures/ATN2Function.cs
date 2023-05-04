using System;
using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class ATN2Function : Function
  {
    public ATN2Function(SQLParser parser)
      : base(parser, 2, true)
    {
      dataType = VistaDBType.Float;
      parameterTypes[0] = VistaDBType.Float;
      parameterTypes[1] = VistaDBType.Float;
    }

    protected override object ExecuteSubProgram()
    {
      return Math.Atan2((double)paramValues[0].Value, (double)paramValues[1].Value);
    }
  }
}
