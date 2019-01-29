﻿using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class LTrimFunction : Function
  {
    public LTrimFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      this.dataType = VistaDBType.NChar;
      this.parameterTypes[0] = VistaDBType.NChar;
    }

    protected override object ExecuteSubProgram()
    {
      return (object) ((string) ((IValue) this.paramValues[0]).Value).TrimStart();
    }

    public override int GetWidth()
    {
      return this[0].GetWidth();
    }
  }
}
