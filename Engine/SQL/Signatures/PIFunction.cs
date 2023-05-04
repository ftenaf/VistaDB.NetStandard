using System;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class PIFunction : Function
  {
    public PIFunction(SQLParser parser)
      : base(parser, 0, true)
    {
      dataType = VistaDBType.Float;
    }

    protected override object ExecuteSubProgram()
    {
      return Math.PI;
    }

    protected override bool InternalGetIsChanged()
    {
      return true;
    }
  }
}
