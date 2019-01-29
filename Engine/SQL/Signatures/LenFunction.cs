﻿using VistaDB.Engine.Internal;

namespace VistaDB.Engine.SQL.Signatures
{
  internal class LenFunction : Function
  {
    public LenFunction(SQLParser parser)
      : base(parser, 1, true)
    {
      this.dataType = VistaDBType.Int;
      this.parameterTypes[0] = VistaDBType.NChar;
    }

    protected override object ExecuteSubProgram()
    {
      return (object) ((string) ((IValue) this.paramValues[0]).Value).TrimEnd().Length;
    }
  }
}
